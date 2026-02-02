import json

def handler(event, context):
    """
    Generates an Athena CTAS query using the Glue table silver.parquet_data,
    producing Parquet output in S3 for the gold.weekly_summary table.
    """

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
            LEAST(SUM(productive_flag) * 0.5, 0.5) AS productive_hours
        FROM intervalized
        GROUP BY "Extension", interval_start
    )
    SELECT *
    FROM aggregated
    ORDER BY "Extension", interval_start;
    """

    # Return as JSON payload for Step Function
    return {
        "athena_query": ctas_query.strip()
    }

