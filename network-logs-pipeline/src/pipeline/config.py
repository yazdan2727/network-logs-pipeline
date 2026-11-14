import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import URL

# Project root directory
BASE_DIR = Path(__file__).resolve().parents[2]

# Load environment variables
load_dotenv(BASE_DIR / ".env")


def get_database_url():
    return URL.create(
        drivername="postgresql+psycopg",
        username=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
        database=os.getenv("DB_NAME", "network_logs"),
    )


DATA_DIR = BASE_DIR / "data"
LOG_DIR = BASE_DIR / "logs"