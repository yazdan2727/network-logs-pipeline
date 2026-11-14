from sqlalchemy import create_engine

from src.pipeline.config import get_database_url

engine = create_engine(
    get_database_url(),
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
)


def get_engine():
    return engine