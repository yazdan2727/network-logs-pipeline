-- Top 5 source IPs by total error events across all dates.

SELECT
    src_ip,
    SUM(error_events)::BIGINT AS total_error_events
FROM mart_daily_network_stats
GROUP BY src_ip
ORDER BY total_error_events DESC, src_ip
LIMIT 5;

-- Rank source IPs by total bytes for each individual day.

SELECT
    event_date,
    src_ip,
    total_bytes,
    RANK() OVER (
        PARTITION BY event_date
        ORDER BY total_bytes DESC
    ) AS daily_rank
FROM mart_daily_network_stats
ORDER BY event_date, daily_rank, src_ip;

-- Seven-calendar-day moving average of total daily events.

WITH daily_events AS (
    SELECT
        event_date,
        SUM(total_events)::BIGINT AS total_events
    FROM mart_daily_network_stats
    GROUP BY event_date
),

date_range AS (
    SELECT
        MIN(event_date) AS min_date,
        MAX(event_date) AS max_date
    FROM daily_events
),

calendar AS (
    SELECT
        generated_date::DATE AS event_date
    FROM date_range,
         GENERATE_SERIES(
             min_date,
             max_date,
             INTERVAL '1 day'
         ) AS generated_date
),

complete_daily_events AS (
    SELECT
        calendar.event_date,
        COALESCE(daily_events.total_events, 0) AS total_events
    FROM calendar
    LEFT JOIN daily_events
        ON calendar.event_date = daily_events.event_date
),

moving_average AS (
    SELECT
        event_date,
        total_events,

        AVG(total_events) OVER (
            ORDER BY event_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS moving_avg_7_days,

        COUNT(*) OVER (
            ORDER BY event_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS days_in_window

    FROM complete_daily_events
)

SELECT
    event_date,
    total_events,
    ROUND(moving_avg_7_days, 2) AS moving_avg_7_days
FROM moving_average
WHERE days_in_window = 7
ORDER BY event_date;