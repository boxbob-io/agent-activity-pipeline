import os
import json
import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

glue_client = boto3.client("glue")

# Glue job name passed via Lambda environment variable
GLUE_JOB_NAME = os.environ["GLUE_JOB_NAME"]

def handler(event, context):
    logger.info(f"Received event: {json.dumps(event)}")

    # EventBridge S3 event
    try:
        records = event.get("detail")
        bucket = records["bucket"]["name"]
        key = records["object"]["key"]

        logger.info(f"Starting Glue job {GLUE_JOB_NAME} for s3://{bucket}/{key}")

        response = glue_client.start_job_run(
            JobName=GLUE_JOB_NAME,
            Arguments={
                "--s3_bucket": bucket,
                "--s3_key": key
            }
        )

        logger.info(f"Glue job started: {response['JobRunId']}")
        return {
            "status": "success",
            "job_run_id": response["JobRunId"]
        }

    except Exception as e:
        logger.exception("Failed to start Glue job")
        raise e

