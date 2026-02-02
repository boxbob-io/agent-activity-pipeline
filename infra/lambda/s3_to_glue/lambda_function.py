import os
import json
import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

glue_client = boto3.client("glue")

GLUE_JOB_NAME = os.environ["GLUE_JOB_NAME"]
SILVER_BUCKET = os.environ["SILVER_BUCKET"]

def handler(event, context):
    logger.info(f"Received event: {json.dumps(event)}")

    try:
        detail = event.get("detail", {})
        bucket = detail.get("bucket", {}).get("name")
        key = detail.get("object", {}).get("key")

        if not bucket or not key:
            raise ValueError("Bucket or key missing in event")

        if isinstance(key, list):
            key = key[0]

        logger.info(f"Starting Glue job {GLUE_JOB_NAME} for s3://{bucket}/{key}")

        response = glue_client.start_job_run(
            JobName=GLUE_JOB_NAME,
            Arguments={
                "--SOURCE_BUCKET": bucket,
                "--SOURCE_KEY": key,
                "--SILVER_BUCKET": SILVER_BUCKET
            }
        )

        logger.info(f"Glue job started: {response['JobRunId']}")
        return {
            "status": "success",
            "job_run_id": response["JobRunId"],
            "source_bucket": bucket,
            "source_key": key,
            "silver_bucket": SILVER_BUCKET
        }

    except Exception as e:
        logger.exception("Failed to start Glue job")
        raise e

