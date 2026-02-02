from aws_cdk import (
    Stack,
    aws_s3 as s3,
    aws_lambda as _lambda,
    aws_iam as iam,
    aws_glue as glue,
    aws_events as events,
    aws_events_targets as targets,
)
from constructs import Construct
import os

class ShyftoffPipelineStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # -----------------------------
        # Buckets
        # -----------------------------
        bronze_bucket = s3.Bucket.from_bucket_name(
            self, "BronzeBucket",
            bucket_name=os.environ["BRONZE_BUCKET"]
        )

        silver_bucket = s3.Bucket.from_bucket_name(
            self, "SilverBucket",
            bucket_name=os.environ["SILVER_BUCKET"]
        )

        scripts_bucket = s3.Bucket.from_bucket_name(
            self, "ScriptsBucket",
            bucket_name=os.environ["SCRIPTS_BUCKET"]
        )

        # -----------------------------
        # IAM Roles
        # -----------------------------
        glue_role = iam.Role.from_role_arn(
            self, "GlueRole",
            role_arn=os.environ["GLUE_ROLE_ARN"]
        )

        lambda_role = iam.Role.from_role_arn(
            self, "LambdaRole",
            role_arn=os.environ["LAMBDA_ROLE_ARN"]
        )

        # -----------------------------
        # Glue Job
        # -----------------------------
        glue_job = glue.CfnJob(
            self, "CsvToParquetJob",
            name="shyftoff-pipeline-csv-to-parquet-dev",
            role=glue_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=f"s3://{scripts_bucket.bucket_name}/csv_to_parquet.py"
            ),
            default_arguments={
                "--job-language": "python",
                "--TempDir": f"s3://{silver_bucket.bucket_name}/temp/",
                "--output_path": f"s3://{silver_bucket.bucket_name}/"
            },
            glue_version="4.0",
            max_capacity=2
        )

        # -----------------------------
        # Lambda to trigger Glue
        # -----------------------------
        lambda_fn = _lambda.Function(
            self, "S3ToGlueLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="lambda_function.handler",   # <file_name>.<function_name>
            code=_lambda.Code.from_asset("lambda/s3_to_glue"),  # folder path
            role=lambda_role,
            environment={
                "GLUE_JOB_NAME": glue_job.name
            }
        )

        # -----------------------------
        # EventBridge Rule for S3 CSV upload
        # -----------------------------
        rule = events.Rule(
            self, "S3CsvUploadRule",
            event_pattern=events.EventPattern(
                source=["aws.s3"],
                detail_type=["Object Created"],
                detail={
                    "bucket": {"name": [bronze_bucket.bucket_name]},
                    "object": {"key": [{"suffix": ".csv"}]}
                }
            )
        )

        rule.add_target(targets.LambdaFunction(lambda_fn))

