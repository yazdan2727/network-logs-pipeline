import argparse
import logging
import subprocess
import sys

from sqlalchemy import text

from src.pipeline.config import BASE_DIR, DATA_DIR
from src.pipeline.database import get_engine
from src.pipeline.ingest import ingest_csv
from src.pipeline.validate import validate_batch
from src.pipeline.clean import clean_batch
from src.pipeline.aggregate import aggregate_batch
from src.pipeline.logging_config import setup_logging


logger = logging.getLogger(__name__)


def get_latest_batch_id():
    query = text("""
        SELECT batch_id
        FROM pipeline_batches
        WHERE batch_status = 'COMPLETED'
        ORDER BY completed_at DESC
        LIMIT 1
    """)

    with get_engine().connect() as connection:
        batch_id = connection.execute(query).scalar_one_or_none()

    if batch_id is None:
        raise RuntimeError("No completed batch found. Run ingestion first.")

    return batch_id


def run_ingestion(file_path):
    logger.info("Starting ingestion: %s", file_path)

    result = ingest_csv(file_path)

    logger.info("Ingestion result: %s", result)

    if result.get("batch_id"):
        return result["batch_id"]

    # Supports an ingestion result that reports SKIPPED without returning
    # its batch ID.
    return get_latest_batch_id()


def run_validation(batch_id):
    logger.info("Starting validation for batch %s", batch_id)
    result = validate_batch(batch_id)
    logger.info("Validation result: %s", result)
    return result


def run_cleaning(batch_id):
    logger.info("Starting cleaning for batch %s", batch_id)
    result = clean_batch(batch_id)
    logger.info("Cleaning result: %s", result)

    # Assignment requirement: warn if clean/raw is below 0.5.
    query = text("""
        SELECT
            (SELECT COUNT(*)
             FROM stg_network_logs
             WHERE batch_id = :batch_id) AS clean_count,
            (SELECT COUNT(*)
             FROM raw_network_logs
             WHERE batch_id = :batch_id) AS raw_count
    """)

    with get_engine().connect() as connection:
        counts = connection.execute(
            query, {"batch_id": batch_id}
        ).one()

    clean_count, raw_count = counts

    if raw_count and clean_count / raw_count < 0.5:
        logger.warning(
            "Clean/raw ratio is %.2f%%, below the 50%% threshold.",
            (clean_count / raw_count) * 100,
        )

    return result


def run_aggregation(batch_id):
    logger.info("Starting mart aggregation for batch %s", batch_id)
    result = aggregate_batch(batch_id)
    logger.info("Aggregation result: %s", result)
    return result


def run_all(file_path):
    logger.info("Starting full pipeline")

    batch_id = run_ingestion(file_path)

    run_validation(batch_id)
    run_cleaning(batch_id)
    run_aggregation(batch_id)

    logger.info("Full pipeline completed successfully")


def run_tests():
    logger.info("Running pytest")

    result = subprocess.run(
        [sys.executable, "-m", "pytest", "-v"],
        cwd=BASE_DIR,
        check=False,
    )

    if result.returncode != 0:
        raise RuntimeError("Tests failed.")

    logger.info("All tests passed")


def main():
    setup_logging()

    parser = argparse.ArgumentParser(
        description="Network Logs Data Pipeline"
    )

    subparsers = parser.add_subparsers(
        dest="command",
        required=True,
    )

    ingest_parser = subparsers.add_parser("ingest")
    ingest_parser.add_argument(
        "--file",
        default=str(DATA_DIR / "network_logs_dirty.csv"),
        help="Path to the input CSV file",
    )

    subparsers.add_parser("validate")
    subparsers.add_parser("clean")
    subparsers.add_parser("aggregate", aliases=["load"])
    subparsers.add_parser("test")

    all_parser = subparsers.add_parser("all")
    all_parser.add_argument(
        "--file",
        default=str(DATA_DIR / "network_logs_dirty.csv"),
        help="Path to the input CSV file",
    )

    args = parser.parse_args()

    try:
        if args.command == "ingest":
            run_ingestion(args.file)

        elif args.command == "validate":
            run_validation(get_latest_batch_id())

        elif args.command == "clean":
            run_cleaning(get_latest_batch_id())

        elif args.command in ("aggregate", "load"):
            run_aggregation(get_latest_batch_id())

        elif args.command == "test":
            run_tests()

        elif args.command == "all":
            run_all(args.file)

    except Exception:
        logger.exception("Pipeline command failed")
        raise


if __name__ == "__main__":
    main()