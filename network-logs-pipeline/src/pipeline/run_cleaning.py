import logging

from sqlalchemy import text

from src.pipeline.config import LOG_DIR
from src.pipeline.database import get_engine
from src.pipeline.clean import clean_batch


LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "pipeline.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)


if __name__ == "__main__":
    engine = get_engine()

    with engine.connect() as connection:
        batch_id = connection.execute(
            text("""
                SELECT batch_id
                FROM pipeline_batches
                WHERE batch_status = 'COMPLETED'
                ORDER BY completed_at DESC
                LIMIT 1
            """)
        ).scalar_one_or_none()

    if batch_id is None:
        raise RuntimeError("No completed ingestion batch found.")

    result = clean_batch(batch_id)

    print("\nCleaning result:")
    for key, value in result.items():
        print(f"{key}: {value}")