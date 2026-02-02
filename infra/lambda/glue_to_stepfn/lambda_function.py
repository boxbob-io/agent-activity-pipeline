import os
import boto3
import json

s3 = boto3.client("s3")
stepfunctions = boto3.client("stepfunctions")

SILVER_BUCKET = os.environ["SILVER_BUCKET"]
STEP_FUNCTION_ARN = os.environ["STEP_FUNCTION_ARN"]

def handler(event, context):
    """
    Triggered by Glue Job Completion.
    Finds Parquet files in SILVER_BUCKET and triggers Step Function with those paths.
    """
    print("Received event:", json.dumps(event, indent=2))

    detail = event.get("detail", {})
    glue_job_run_id = detail.get("jobRunId")
    job_name = detail.get("jobName")

    # List Parquet files in Silver bucket
    response = s3.list_objects_v2(Bucket=SILVER_BUCKET)
    parquet_files = [
        f"s3://{SILVER_BUCKET}/{obj['Key']}"
        for obj in response.get("Contents", [])
        if obj["Key"].endswith(".parquet")
    ]

    if not parquet_files:
        print("No parquet files found in Silver bucket. Exiting.")
        return {"status": "no_files"}

    print(f"Parquet files found: {parquet_files}")

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

