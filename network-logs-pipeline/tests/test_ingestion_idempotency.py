from sqlalchemy import text

from src.pipeline.config import DATA_DIR
from src.pipeline.ingest import ingest_csv


def test_ingestion_is_idempotent(engine, batch_id):
    csv_path = DATA_DIR / "network_logs_dirty.csv"

    # Count existing records for this batch.
    with engine.connect() as connection:
        count_before = connection.execute(
            text("""
                SELECT COUNT(*)
                FROM raw_network_logs
                WHERE batch_id = :batch_id
            """),
            {"batch_id": batch_id},
        ).scalar_one()

    # Run ingestion again using the same file.
    result = ingest_csv(csv_path)

    # Count records again.
    with engine.connect() as connection:
        count_after = connection.execute(
            text("""
                SELECT COUNT(*)
                FROM raw_network_logs
                WHERE batch_id = :batch_id
            """),
            {"batch_id": batch_id},
        ).scalar_one()

    assert result["status"] == "SKIPPED"
    assert count_before == count_after