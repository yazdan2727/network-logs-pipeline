from sqlalchemy import text


def test_no_invalid_timestamps(engine):
    query = text("""
        SELECT COUNT(*)
        FROM stg_network_logs
        WHERE timestamp IS NULL
           OR timestamp < TIMESTAMPTZ '2024-01-01 00:00:00+00'
           OR timestamp > TIMESTAMPTZ '2024-12-31 23:59:59+00'
    """)

    with engine.connect() as connection:
        invalid_count = connection.execute(query).scalar_one()

    assert invalid_count == 0, (
        f"Found {invalid_count} invalid timestamps in Staging"
    )


def test_no_negative_or_missing_bytes(engine):
    query = text("""
        SELECT COUNT(*)
        FROM stg_network_logs
        WHERE bytes IS NULL OR bytes < 0
    """)

    with engine.connect() as connection:
        invalid_count = connection.execute(query).scalar_one()

    assert invalid_count == 0, (
        f"Found {invalid_count} negative or missing byte values"
    )