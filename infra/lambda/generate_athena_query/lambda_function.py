import json

def handler(event, context):
    ctas_query = """
    CREATE TABLE IF NOT EXISTS gold.weekly_summary
    WITH (
        format = 'PARQUET',
        parquet_compression = 'SNAPPY',
        external_location = 's3://shyftoff-pipeline-gold-dev/weekly_summary/'
    )
    AS
    WITH productive_events AS (
        SELECT
            "Extension",
            "Done On" AS done_on,
            CASE
                WHEN "Action" IN ('ONLINE', 'REMVCEON') THEN 1
                WHEN "Action" = 'OFFLINE' AND "Details" = 'Backoffice' THEN 1
                ELSE 0
            END AS productive_flag
        FROM silver.parquet_data
    ),
    intervalized AS (
        SELECT
            "Extension",
            date_trunc('hour', done_on)
              + floor(minute(done_on) / 30) * interval '30' minute AS interval_start,
            productive_flag
        FROM productive_events
    ),
    aggregated AS (
        SELECT
            "Extension",
            interval_start,
            CAST(LEAST(SUM(productive_flag) * 0.5, 0.5) AS DOUBLE) AS productive_hours
        FROM intervalized
        GROUP BY "Extension", interval_start
    )
    SELECT *
    FROM aggregated
    ORDER BY "Extension", interval_start;
    """
    
    return {"athena_query": ctas_query.strip()}

