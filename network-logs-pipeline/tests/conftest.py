import pytest
from sqlalchemy import text
from src.pipeline.database import get_engine


@pytest.fixture(scope="session")
def engine():
    return get_engine()


@pytest.fixture(scope="session")
def batch_id(engine):
    with engine.connect() as connection:
        result = connection.execute(
            text("""
                SELECT batch_id
                FROM pipeline_batches
                WHERE batch_status = 'COMPLETED'
                ORDER BY completed_at DESC
                LIMIT 1
            """)
        ).scalar_one_or_none()

    if result is None:
        pytest.fail("No completed ingestion batch found.")

    return result