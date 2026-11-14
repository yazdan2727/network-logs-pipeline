from sqlalchemy import text


def test_no_duplicate_mart_records(engine):
    query = text("""
        SELECT COUNT(*)
        FROM (
            SELECT event_date, src_ip
            FROM mart_daily_network_stats
            GROUP BY event_date, src_ip
            HAVING COUNT(*) > 1
        ) duplicates
    """)

    with engine.connect() as connection:
        duplicate_count = connection.execute(query).scalar_one()

    assert duplicate_count == 0, (
        f"Found {duplicate_count} duplicate date/source-IP combinations"
    )