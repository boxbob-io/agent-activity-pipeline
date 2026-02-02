import os
import boto3
import json

stepfunctions = boto3.client("stepfunctions")

STEP_FUNCTION_ARN = os.environ["STEP_FUNCTION_ARN"]

def handler(event, context):
    """
    Triggered by Glue Job Completion.
    Starts the Step Function that:
      1. Repairs Silver table partitions
      2. Builds Gold tables via Athena
    """

    print("Received event:", json.dumps(event, indent=2))

    detail = event.get("detail", {})
    glue_job_run_id = detail.get("jobRunId")
    job_name = detail.get("jobName")

    input_payload = {
        "glue_job_run_id": glue_job_run_id,
        "job_name": job_name
    }

    response = stepfunctions.start_execution(
        stateMachineArn=STEP_FUNCTION_ARN,
        input=json.dumps(input_payload)
    )

    print(f"Started Step Function execution: {response['executionArn']}")

    return {
        "status": "started",
        "step_function_execution_arn": response["executionArn"]
    }

