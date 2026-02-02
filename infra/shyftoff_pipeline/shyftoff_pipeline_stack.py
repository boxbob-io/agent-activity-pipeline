from aws_cdk import (
    Stack,
    aws_s3 as s3,
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
        # Existing buckets
        # -----------------------------
        bronze_bucket = s3.Bucket.from_bucket_name(
            self,
            "BronzeBucket",
            bucket_name=os.environ.get(
                "BRONZE_BUCKET",
                "shyftoff-pipeline-bronze-dev",
            ),
        )

        silver_bucket = s3.Bucket.from_bucket_name(
            self,
            "SilverBucket",
            bucket_name=os.environ.get(
                "SILVER_BUCKET",
                "shyftoff-pipeline-silver-dev",
            ),
        )

        scripts_bucket = s3.Bucket.from_bucket_name(
            self,
            "ScriptsBucket",
            bucket_name=os.environ.get(
                "SCRIPTS_BUCKET",
                "shyftoff-pipeline-scripts-dev",
            ),
        )

        # -----------------------------
        # Existing IAM role (Glue)
        # -----------------------------
        glue_role = iam.Role.from_role_arn(
            self,
            "GlueRole",
            role_arn=os.environ["GLUE_ROLE_ARN"],
            mutable=False,
        )

        # -----------------------------
        # Glue Job
        # -----------------------------
        glue_job = glue.CfnJob(
            self,
            "CsvToParquetJob",
            name="shyftoff-pipeline-csv-to-parquet-dev",
            role=glue_role.role_arn,
            glue_version="4.0",
            worker_type="G.1X",
            number_of_workers=2,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=(
                    f"s3://{scripts_bucket.bucket_name}/scripts/csv_to_parquet.py"
                ),
            ),
            default_arguments={
                "--job-language": "python",
                "--enable-metrics": "",
                "--enable-continuous-cloudwatch-log": "true",
                "--TempDir": f"s3://{silver_bucket.bucket_name}/temp/",
                "--SILVER_BUCKET": silver_bucket.bucket_name,
            },
        )

        # -----------------------------
        # EventBridge rule (CSV upload)
        # -----------------------------
        rule = events.Rule(
            self,
            "BronzeCsvUploadRule",
            event_pattern=events.EventPattern(
                source=["aws.s3"],
                detail_type=["Object Created"],
                detail={
                    "bucket": {"name": [bronze_bucket.bucket_name]},
                    "object": {"key": [{"suffix": ".csv"}]},
                },
            ),
        )

        # -----------------------------
        # EventBridge → Glue (direct)
        # -----------------------------
        rule.add_target(
            targets.GlueStartJobRun(
                job_name=glue_job.ref,  # IMPORTANT: ref, not name
                arguments={
                    "--SOURCE_BUCKET": events.EventField.from_path(
                        "$.detail.bucket.name"
                    ),
                    "--SOURCE_KEY": events.EventField.from_path(
                        "$.detail.object.key"
                    ),
                },
            )
        )

