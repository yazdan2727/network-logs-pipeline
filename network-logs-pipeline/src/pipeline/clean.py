import logging

from sqlalchemy import text

from src.pipeline.database import get_engine

logger = logging.getLogger(__name__)


def clean_batch(batch_id):
    engine = get_engine()

    median_sql = text("""
        SELECT
            ROUND(
                PERCENTILE_CONT(0.5)
                WITHIN GROUP (
                    ORDER BY NULLIF(BTRIM(raw.bytes), '')::BIGINT
                )::NUMERIC
            )::BIGINT AS bytes_median
        FROM raw_network_logs raw
        WHERE raw.batch_id = :batch_id
          AND NULLIF(BTRIM(raw.bytes), '') IS NOT NULL
          AND NOT EXISTS (
              SELECT 1
              FROM rejected_network_logs rejected
              WHERE rejected.batch_id = raw.batch_id
                AND rejected.source_record_id = raw.source_record_id
          )
    """)

    insert_staging_sql = text("""
        INSERT INTO stg_network_logs (
            batch_id,
            source_record_id,
            timestamp,
            src_ip,
            dst_ip,
            port,
            bytes,
            protocol,
            status
        )
        SELECT
            raw.batch_id,
            raw.source_record_id,

            BTRIM(raw.timestamp)::TIMESTAMP
                AT TIME ZONE 'UTC',

            BTRIM(raw.src_ip)::INET,
            BTRIM(raw.dst_ip)::INET,
            BTRIM(raw.port)::INTEGER,

            COALESCE(
                NULLIF(BTRIM(raw.bytes), '')::BIGINT,
                :bytes_median
            ),

            raw.protocol,

            COALESCE(
                NULLIF(BTRIM(raw.status), ''),
                'UNKNOWN'
            )

        FROM raw_network_logs raw

        WHERE raw.batch_id = :batch_id

          AND NOT EXISTS (
              SELECT 1
              FROM rejected_network_logs rejected
              WHERE rejected.batch_id = raw.batch_id
                AND rejected.source_record_id = raw.source_record_id
          )

        ORDER BY raw.source_record_id
    """)

    with engine.begin() as connection:

        # 1. Calculate the median from valid records.
        bytes_median = connection.execute(
            median_sql,
            {"batch_id": batch_id},
        ).scalar_one()

        if bytes_median is None:
            raise ValueError(
                "Cannot calculate bytes median: "
                "there are no valid, non-null bytes values."
            )

        # 2. Make this operation idempotent.
        # Remove any previous staging output for this batch.
        connection.execute(
            text("""
                DELETE FROM stg_network_logs
                WHERE batch_id = :batch_id
            """),
            {"batch_id": batch_id},
        )

        # 3. Insert cleaned records.
        connection.execute(
            insert_staging_sql,
            {
                "batch_id": batch_id,
                "bytes_median": bytes_median,
            },
        )

        # 4. Update batch metadata.
        staging_count = connection.execute(
            text("""
                SELECT COUNT(*)
                FROM stg_network_logs
                WHERE batch_id = :batch_id
            """),
            {"batch_id": batch_id},
        ).scalar_one()

        connection.execute(
            text("""
                UPDATE pipeline_batches
                SET bytes_median = :bytes_median
                WHERE batch_id = :batch_id
            """),
            {
                "batch_id": batch_id,
                "bytes_median": bytes_median,
            },
        )

    result = {
        "batch_id": batch_id,
        "bytes_median": bytes_median,
        "staging_records": staging_count,
    }

    logger.info("Cleaning completed: %s", result)

    return result