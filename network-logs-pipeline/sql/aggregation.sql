-- Rebuild the daily mart for dates affected by the selected batch.
-- We aggregate all staging batches for those dates so reruns do not
-- add the same batch's metrics twice.

WITH affected_dates AS (
    SELECT DISTINCT
        (timestamp AT TIME ZONE 'UTC')::DATE AS event_date
    FROM stg_network_logs
    WHERE batch_id = :batch_id
)
DELETE FROM mart_daily_network_stats mart
USING affected_dates affected
WHERE mart.event_date = affected.event_date;


-- Recalculate the complete daily aggregates for those dates.

INSERT INTO mart_daily_network_stats (
    event_date,
    src_ip,
    total_events,
    total_bytes,
    avg_bytes,
    error_events,
    updated_at
)
SELECT
    (stg.timestamp AT TIME ZONE 'UTC')::DATE AS event_date,
    stg.src_ip,
    COUNT(*)::BIGINT AS total_events,
    COALESCE(SUM(stg.bytes), 0)::BIGINT AS total_bytes,
    AVG(stg.bytes)::DOUBLE PRECISION AS avg_bytes,
    COUNT(*) FILTER (
        WHERE UPPER(BTRIM(stg.status)) IN ('TIMEOUT', 'RESET')
    )::BIGINT AS error_events,
    NOW() AS updated_at
FROM stg_network_logs stg
WHERE (stg.timestamp AT TIME ZONE 'UTC')::DATE IN (
    SELECT DISTINCT
        (timestamp AT TIME ZONE 'UTC')::DATE
    FROM stg_network_logs
    WHERE batch_id = :batch_id
)
GROUP BY
    (stg.timestamp AT TIME ZONE 'UTC')::DATE,
    stg.src_ip
ON CONFLICT (event_date, src_ip)
DO UPDATE SET
    total_events = EXCLUDED.total_events,
    total_bytes = EXCLUDED.total_bytes,
    avg_bytes = EXCLUDED.avg_bytes,
    error_events = EXCLUDED.error_events,
    updated_at = NOW();