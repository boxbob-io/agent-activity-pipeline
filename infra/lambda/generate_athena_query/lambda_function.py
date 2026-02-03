from pathlib import Path

SQL_PATH = Path(__file__).with_name("weekly_summary.sql")
CTAS_QUERY = SQL_PATH.read_text(encoding="utf-8").strip()

def handler(event, context):
    """Return the Athena CTAS query used by Step Functions."""
    return {"athena_query": CTAS_QUERY}
