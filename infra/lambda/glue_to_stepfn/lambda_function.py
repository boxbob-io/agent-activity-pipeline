import os
import boto3
import json
from datetime import datetime, timezone

s3 = boto3.client("s3")
stepfunctions = boto3.client("stepfunctions")

SILVER_BUCKET = os.environ["SILVER_BUCKET"]
STEP_FUNCTION_ARN = os.environ["STEP_FUNCTION_ARN"]

def lambda_handler(event, context):
    """
    Triggered by Glue Job Completion.
    Finds Parquet files in SILVER_BUCKET created after Glue job start time,
    then triggers Step Function with those paths.
    """
    print("Received event:", json.dumps(event, indent=2))

    detail = event.get("detail", {})
    glue_job_run_id = detail.get("jobRunId")
    job_name = detail.get("jobName")
    start_time_str = detail.get("startedOn")  # ISO timestamp
    start_time = datetime.fromisoformat(start_time_str.replace("Z", "+00:00"))

    print(f"Glue Job Run ID: {glue_job_run_id}, Job Name: {job_name}, Started On: {start_time}")

    # List all parquet files in Silver bucket
    response = s3.list_objects_v2(Bucket=SILVER_BUCKET)
    parquet_files = []

    for obj in response.get("Contents", []):
        key = obj["Key"]
        if not key.endswith(".parquet"):
            continue

        # Filter by last modified time after Glue job started
        last_modified = obj["LastModified"]
        if last_modified.replace(tzinfo=timezone.utc) >= start_time:
            parquet_files.append(f"s3://{SILVER_BUCKET}/{key}")

    if not parquet_files:
        print("No new parquet files found for this Glue job run. Exiting.")
        return {"status": "no_files"}

    print(f"New parquet files: {parquet_files}")

    # Start Step Function with parquet file list
    input_payload = {
        "glue_job_run_id": glue_job_run_id,
        "silver_parquet_paths": parquet_files
    }

    response = stepfunctions.start_execution(
        stateMachineArn=STEP_FUNCTION_ARN,
        input=json.dumps(input_payload)
    )

    print(f"Started Step Function execution: {response['executionArn']}")
    return {
        "status": "success",
        "step_function_execution_arn": response["executionArn"],
        "files_processed": parquet_files
    }

