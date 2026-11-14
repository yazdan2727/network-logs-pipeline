import logging

from sqlalchemy import text

from src.pipeline.database import get_engine

logger = logging.getLogger(__name__)


def aggregate_batch(batch_id):
    engine = get_engine()

    delete_sql = text("""
        WITH affected_dates AS (
            SELECT DISTINCT
                (timestamp AT TIME ZONE 'UTC')::DATE AS event_date
            FROM stg_network_logs
            WHERE batch_id = :batch_id
        )
        DELETE FROM mart_daily_network_stats mart
        USING affected_dates affected
        WHERE mart.event_date = affected.event_date
    """)

    insert_sql = text("""
        INSERT INTO mart_daily_network_stats (
            event_date,
            src_ip,
            total_events,
            total_bytes,
            avg_bytes,
            error_events,
            updated_at
        )
        SELECT
            (stg.timestamp AT TIME ZONE 'UTC')::DATE,
            stg.src_ip,
            COUNT(*)::BIGINT,
            COALESCE(SUM(stg.bytes), 0)::BIGINT,
            AVG(stg.bytes)::DOUBLE PRECISION,
            COUNT(*) FILTER (
                WHERE UPPER(BTRIM(stg.status)) IN ('TIMEOUT', 'RESET')
            )::BIGINT,
            NOW()
        FROM stg_network_logs stg
        WHERE (stg.timestamp AT TIME ZONE 'UTC')::DATE IN (
            SELECT DISTINCT
                (timestamp AT TIME ZONE 'UTC')::DATE
            FROM stg_network_logs
            WHERE batch_id = :batch_id
        )
        GROUP BY
            (stg.timestamp AT TIME ZONE 'UTC')::DATE,
            stg.src_ip
        ON CONFLICT (event_date, src_ip)
        DO UPDATE SET
            total_events = EXCLUDED.total_events,
            total_bytes = EXCLUDED.total_bytes,
            avg_bytes = EXCLUDED.avg_bytes,
            error_events = EXCLUDED.error_events,
            updated_at = NOW()
    """)

    with engine.begin() as connection:
        staging_count = connection.execute(
            text("""
                SELECT COUNT(*)
                FROM stg_network_logs
                WHERE batch_id = :batch_id
            """),
            {"batch_id": batch_id},
        ).scalar_one()

        if staging_count == 0:
            raise ValueError(
                f"No staging records found for batch {batch_id}"
            )

        # Both operations run in the same transaction.
        connection.execute(delete_sql, {"batch_id": batch_id})

        result = connection.execute(
            insert_sql,
            {"batch_id": batch_id},
        )

        mart_rows_written = result.rowcount

        affected_dates = connection.execute(
            text("""
                SELECT COUNT(DISTINCT
                    (timestamp AT TIME ZONE 'UTC')::DATE
                )
                FROM stg_network_logs
                WHERE batch_id = :batch_id
            """),
            {"batch_id": batch_id},
        ).scalar_one()

    summary = {
        "batch_id": batch_id,
        "staging_records": staging_count,
        "affected_dates": affected_dates,
        "mart_rows_written": mart_rows_written,
    }

    logger.info("Aggregation completed: %s", summary)

    return summary