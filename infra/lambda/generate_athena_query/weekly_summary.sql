CREATE TABLE IF NOT EXISTS gold.weekly_summary
WITH (
    format = 'PARQUET',
    parquet_compression = 'SNAPPY',
    external_location = 's3://shyftoff-pipeline-gold-dev/weekly_summary/'
) AS
WITH base_events AS (
    SELECT
        Extension,
        CAST("Done On" AS timestamp) AS done_on,
        CASE
            WHEN "Action" IN ('ONLINE', 'REMVCEON') THEN 1
            WHEN "Action" = 'OFFLINE' AND "Details" = 'Backoffice' THEN 1
            ELSE 0
        END AS productive_flag
    FROM silver.parquet_data
    WHERE "Done On" IS NOT NULL
),
ordered_events AS (
    SELECT
        Extension,
        done_on,
        productive_flag,
        LEAD(done_on) OVER (
            PARTITION BY Extension
            ORDER BY done_on, productive_flag, Extension
        ) AS next_done_on
    FROM base_events
),
intervaled_events AS (
    SELECT
        Extension,
        date_trunc('hour', done_on)
          + floor(minute(done_on) / 30) * interval '30' minute AS interval_start,
        date_trunc('hour', done_on)
          + (floor(minute(done_on) / 30) + 1) * interval '30' minute AS interval_end,
        date_diff(
            'second',
            date_trunc('hour', done_on)
              + floor(minute(done_on) / 30) * interval '30' minute,
            date_trunc('hour', done_on)
              + (floor(minute(done_on) / 30) + 1) * interval '30' minute
        ) AS interval_length_seconds,
        done_on,
        COALESCE(
            next_done_on,
            date_trunc('hour', done_on)
              + (floor(minute(done_on) / 30) + 1) * interval '30' minute
        ) AS next_done_on,
        productive_flag
    FROM ordered_events
),
calc_durations AS (
    SELECT
        Extension,
        interval_start,
        interval_end,
        interval_length_seconds,
        productive_flag,
        GREATEST(
            0,
            date_diff(
                'second',
                GREATEST(done_on, interval_start),
                LEAST(next_done_on, interval_end)
            )
        ) AS productive_seconds
    FROM intervaled_events
), grouped_events AS (
    SELECT
        Extension,
        interval_start as Interval,
        CAST(
            LEAST(
                SUM(CASE WHEN productive_flag = 1 THEN productive_seconds ELSE 0 END) / 3600.0,
                MAX(interval_length_seconds) / 3600.0
            ) AS double
        ) AS productive_hours
    FROM calc_durations
    GROUP BY Extension, interval_start
)
SELECT
    Extension,
    interval_start as "Interval",
    productive_hours as "Productive Hours"
FROM grouped_events
WHERE productive_hours > 0
ORDER BY Extension, interval_start
