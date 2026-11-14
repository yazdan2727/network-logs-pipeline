import ipaddress
import logging
from datetime import datetime, timezone
import json

from sqlalchemy import text

from src.pipeline.database import get_engine

logger = logging.getLogger(__name__)

START_DATE = datetime(2024, 1, 1, tzinfo=timezone.utc)
END_DATE = datetime(2024, 12, 31, 23, 59, 59, tzinfo=timezone.utc)
MAX_INVALID_TIMESTAMP_RATE = 0.05


def parse_timestamp(value):
    """Parse the assignment's UTC timestamp format."""
    if value is None or not value.strip():
        return None

    try:
        parsed = datetime.strptime(value.strip(), "%Y-%m-%d %H:%M:%S")
        return parsed.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def get_initial_rejection_reason(row):
    """Validate fields other than duplicate detection."""

    timestamp = parse_timestamp(row["timestamp"])

    if timestamp is None:
        return "INVALID_TIMESTAMP", None

    if not START_DATE <= timestamp <= END_DATE:
        return "TIMESTAMP_OUT_OF_RANGE", timestamp

    if not row["src_ip"] or not row["src_ip"].strip():
        return "MISSING_REQUIRED_FIELD", timestamp

    if not row["dst_ip"] or not row["dst_ip"].strip():
        return "MISSING_REQUIRED_FIELD", timestamp

    if not row["port"] or not row["port"].strip():
        return "MISSING_REQUIRED_FIELD", timestamp

    try:
        src_ip = ipaddress.ip_address(row["src_ip"].strip())
        dst_ip = ipaddress.ip_address(row["dst_ip"].strip())
    except ValueError:
        return "INVALID_IP", timestamp

    try:
        port = int(row["port"].strip())
        if not 1 <= port <= 65535:
            return "INVALID_PORT", timestamp
    except ValueError:
        return "INVALID_PORT", timestamp

    bytes_value = row["bytes"]

    if bytes_value is not None and bytes_value.strip():
        try:
            if int(bytes_value.strip()) < 0:
                return "NEGATIVE_BYTES", timestamp
        except ValueError:
            raise ValueError(
                f"Non-numeric bytes value at source_record_id="
                f"{row['source_record_id']}: {bytes_value!r}"
            )

    return None, (timestamp, src_ip, dst_ip, port)


def validate_batch(batch_id):
    engine = get_engine()

    with engine.connect() as connection:
        rows = connection.execute(
            text("""
                SELECT *
                FROM raw_network_logs
                WHERE batch_id = :batch_id
                ORDER BY source_record_id
            """),
            {"batch_id": batch_id},
        ).mappings().all()

    if not rows:
        raise ValueError(f"No raw records found for batch {batch_id}")

    total_records = len(rows)
    rejected = []
    valid_candidates = []
    invalid_timestamp_count = 0

    # First pass: validate individual fields.
    for row in rows:
        reason, parsed_values = get_initial_rejection_reason(row)

        if reason:
            rejected.append((row, reason))

            if reason in {"INVALID_TIMESTAMP", "TIMESTAMP_OUT_OF_RANGE"}:
                invalid_timestamp_count += 1
        else:
            timestamp, src_ip, dst_ip, port = parsed_values
            valid_candidates.append(
                (row, timestamp, src_ip, dst_ip, port)
            )

    # Stop if timestamp quality is below the assignment's threshold.
    invalid_timestamp_rate = invalid_timestamp_count / total_records

    logger.info(
        "Timestamp validation | invalid=%d | total=%d | rate=%.2f%%",
        invalid_timestamp_count,
        total_records,
        invalid_timestamp_rate * 100,
    )

    if invalid_timestamp_rate > MAX_INVALID_TIMESTAMP_RATE:
        raise RuntimeError(
            "Pipeline stopped: invalid/missing/out-of-range timestamps "
            f"account for {invalid_timestamp_rate:.2%} of the batch "
            f"(maximum allowed: {MAX_INVALID_TIMESTAMP_RATE:.0%})."
        )

    # Second pass: detect duplicates among otherwise valid records.
    seen_keys = set()
    duplicate_count = 0

    for row, timestamp, src_ip, dst_ip, port in valid_candidates:
        duplicate_key = (timestamp, src_ip, dst_ip, port)

        if duplicate_key in seen_keys:
            rejected.append((row, "DUPLICATE_RECORD"))
            duplicate_count += 1
        else:
            seen_keys.add(duplicate_key)

    rejected_count = len(rejected)
    valid_count = total_records - rejected_count

    insert_rejected_sql = text("""
        INSERT INTO rejected_network_logs (
            batch_id,
            source_record_id,
            rejection_reason,
            original_record
        )
        VALUES (
            :batch_id,
            :source_record_id,
            :rejection_reason,
            CAST(:original_record AS JSONB)
        )
    """)

    with engine.begin() as connection:
        # Makes validation repeatable if the same batch is validated again.
        connection.execute(
            text("""
                DELETE FROM rejected_network_logs
                WHERE batch_id = :batch_id
            """),
            {"batch_id": batch_id},
        )

        for row, reason in rejected:
            original_record = {
                column: row[column]
                for column in (
                    "timestamp", "src_ip", "dst_ip", "port",
                    "bytes", "protocol", "status", "label",
                    "anomaly_type",
                )
            }

            connection.execute(
                insert_rejected_sql,
                {
                    "batch_id": batch_id,
                    "source_record_id": row["source_record_id"],
                    "rejection_reason": reason,
                    "original_record": json.dumps(original_record),
                },
            )

        connection.execute(
            text("""
                UPDATE pipeline_batches
                SET valid_records = :valid_records,
                    rejected_records = :rejected_records,
                    duplicate_records = :duplicate_records
                WHERE batch_id = :batch_id
            """),
            {
                "batch_id": batch_id,
                "valid_records": valid_count,
                "rejected_records": rejected_count,
                "duplicate_records": duplicate_count,
            },
        )

    result = {
        "batch_id": batch_id,
        "total_records": total_records,
        "valid_records": valid_count,
        "rejected_records": rejected_count,
        "duplicate_records": duplicate_count,
        "invalid_timestamp_rate": round(invalid_timestamp_rate, 4),
    }

    logger.info("Validation completed: %s", result)
    return result