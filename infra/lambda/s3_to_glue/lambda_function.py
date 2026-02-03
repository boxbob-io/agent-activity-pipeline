import logging
import json
import os

import boto3

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

glue_client = boto3.client("glue")

GLUE_JOB_NAME = os.environ["GLUE_JOB_NAME"]
SILVER_BUCKET = os.environ["SILVER_BUCKET"]

def _extract_s3_target(event):
    detail = event.get("detail", {})
    bucket = detail.get("bucket", {}).get("name")
    key = detail.get("object", {}).get("key")
    if isinstance(key, list):
        key = key[0] if key else None
    if not bucket or not key:
        raise ValueError("Bucket or key missing in event")
    return bucket, key


def handler(event, context):
    logger.info("Received event: %s", json.dumps(event))

    try:
        bucket, key = _extract_s3_target(event)
        logger.info("Starting Glue job %s for s3://%s/%s", GLUE_JOB_NAME, bucket, key)

        response = glue_client.start_job_run(
            JobName=GLUE_JOB_NAME,
            Arguments={
                "--SOURCE_BUCKET": bucket,
                "--SOURCE_KEY": key,
                "--SILVER_BUCKET": SILVER_BUCKET,
            },
        )

        job_run_id = response["JobRunId"]
        logger.info("Glue job started: %s", job_run_id)
        return {
            "status": "success",
            "job_run_id": job_run_id,
            "source_bucket": bucket,
            "source_key": key,
            "silver_bucket": SILVER_BUCKET,
        }
    except Exception:
        logger.exception("Failed to start Glue job")
        raise
