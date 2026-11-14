import logging

from src.pipeline.config import DATA_DIR, LOG_DIR
from src.pipeline.ingest import ingest_csv

LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(
            LOG_DIR / "pipeline.log",
            encoding="utf-8"
        ),
        logging.StreamHandler(),
    ],
)

if __name__ == "__main__":
    result = ingest_csv(
        DATA_DIR / "network_logs_dirty.csv"
    )

    print("\nIngestion result:")
    print(result)