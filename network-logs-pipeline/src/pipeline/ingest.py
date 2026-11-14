
import csv
import hashlib
import logging
from pathlib import Path

from sqlalchemy import text

from src.pipeline.database import get_engine

logger = logging.getLogger(__name__)

EXPECTED_COLUMNS = {
    "timestamp",
    "src_ip",
    "dst_ip",
    "port",
    "bytes",
    "protocol",
    "status",
    "label",
    "anomaly_type",
}

BATCH_SIZE = 1000


def calculate_file_hash(file_path: Path) -> str:
    """Calculate SHA-256 hash without loading the whole file."""

    sha256 = hashlib.sha256()

    with file_path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            sha256.update(chunk)

    return sha256.hexdigest()


def validate_csv_header(file_path: Path) -> None:
    """Verify that the CSV contains all expected columns."""

    with file_path.open(
        "r", encoding="utf-8-sig", newline=""
    ) as file:
        reader = csv.DictReader(file)

        if not reader.fieldnames:
            raise ValueError("CSV file has no header")

        if len(reader.fieldnames) != len(set(reader.fieldnames)):
            raise ValueError("CSV contains duplicate column names")

        missing = EXPECTED_COLUMNS - set(reader.fieldnames)

        if missing:
            raise ValueError(
                f"Missing required CSV columns: {sorted(missing)}"
            )


def ingest_csv(file_path: Path) -> dict:
    """Load a CSV file into the raw layer exactly once."""

    file_path = Path(file_path)

    if not file_path.is_file():
        raise FileNotFoundError(file_path)

    validate_csv_header(file_path)

    batch_id = calculate_file_hash(file_path)

    engine = get_engine()

    total_records = 0

    insert_batch_sql = text("""
        INSERT INTO pipeline_batches (
            batch_id,
            file_name,
            batch_status
        )
        VALUES (
            :batch_id,
            :file_name,
            'PROCESSING'
        )
        ON CONFLICT (batch_id) DO NOTHING
        RETURNING batch_id
    """)

    insert_raw_sql = text("""
        INSERT INTO raw_network_logs (
            batch_id,
            source_record_id,
            timestamp,
            src_ip,
            dst_ip,
            port,
            bytes,
            protocol,
            status,
            label,
            anomaly_type
        )
        VALUES (
            :batch_id,
            :source_record_id,
            :timestamp,
            :src_ip,
            :dst_ip,
            :port,
            :bytes,
            :protocol,
            :status,
            :label,
            :anomaly_type
        )
    """)

    update_batch_sql = text("""
        UPDATE pipeline_batches
        SET
            total_records = :total_records,
            batch_status = 'COMPLETED',
            completed_at = NOW()
        WHERE batch_id = :batch_id
    """)

    # One transaction for batch metadata and all raw records.
    # Any failure rolls back the entire ingestion.
    with engine.begin() as connection:

        result = connection.execute(
            insert_batch_sql,
            {
                "batch_id": batch_id,
                "file_name": file_path.name,
            },
        )

        if result.scalar_one_or_none() is None:
            logger.info(
                "Batch already exists. Skipping ingestion: %s",
                batch_id,
            )

            return {
                "batch_id": batch_id,
                "status": "SKIPPED",
                "total_records": 0,
            }

        with file_path.open(
            "r", encoding="utf-8-sig", newline=""
        ) as file:

            reader = csv.DictReader(file)

            chunk = []

            for record_id, row in enumerate(reader, start=1):

                # Reject structurally malformed CSV rows.
                if None in row:
                    raise ValueError(
                        f"Unexpected extra CSV fields at record {record_id}"
                    )

                raw_record = {
                    column: row.get(column)
                    for column in EXPECTED_COLUMNS
                }

                chunk.append({
                    "batch_id": batch_id,
                    "source_record_id": record_id,
                    **raw_record,
                })

                if len(chunk) >= BATCH_SIZE:
                    connection.execute(insert_raw_sql, chunk)

                    total_records += len(chunk)
                    chunk.clear()

            # Insert remaining records.
            if chunk:
                connection.execute(insert_raw_sql, chunk)
                total_records += len(chunk)

        connection.execute(
            update_batch_sql,
            {
                "batch_id": batch_id,
                "total_records": total_records,
            },
        )

    logger.info(
        "Ingestion completed | batch_id=%s | records=%d",
        batch_id,
        total_records,
    )

    return {
        "batch_id": batch_id,
        "status": "COMPLETED",
        "total_records": total_records,
    }
