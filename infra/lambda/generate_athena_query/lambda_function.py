import json

def handler(event, context):
    parquet_paths = event.get("silver_parquet_paths", [])
    if not parquet_paths:
        raise ValueError("No Silver parquet files provided.")

    # Build UNION ALL input for multiple parquet files
    select_statements = [f"SELECT * FROM parquet.`{path}`" for path in parquet_paths]
    union_query = " UNION ALL ".join(select_statements)

    ctas_query = f"""
    CREATE TABLE gold.weekly_summary
    WITH (format='PARQUET') AS

    WITH productive_events AS (
        SELECT
            "Extension",
            "Done On" AS done_on,
            CASE
                WHEN "Action" IN ('ONLINE', 'REMVCEON') THEN 1
                WHEN "Action" = 'OFFLINE' AND "Details" = 'Backoffice' THEN 1
                ELSE 0
            END AS productive_flag
        FROM (
            {union_query}
        )
    ),

    intervalized AS (
        SELECT
            "Extension",
            date_trunc('hour', done_on) +
            floor(minute(done_on)/30) * interval '30' minute AS interval_start,
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
    ORDER BY "Extension", interval_start
    """

    return {
        "athena_query": ctas_query
    }

