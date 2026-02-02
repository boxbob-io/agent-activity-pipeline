from aws_cdk import (
    Stack,
    aws_s3 as s3,
    aws_lambda as _lambda,
    aws_iam as iam,
    aws_glue as glue,
    aws_events as events,
    aws_events_targets as targets,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
)
from constructs import Construct
import os

class ShyftoffPipelineStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # -----------------------------
        # Buckets
        # -----------------------------
        bronze_bucket = s3.Bucket.from_bucket_name(self, "BronzeBucket", bucket_name=os.environ["BRONZE_BUCKET"])
        silver_bucket = s3.Bucket.from_bucket_name(self, "SilverBucket", bucket_name=os.environ["SILVER_BUCKET"])
        scripts_bucket = s3.Bucket.from_bucket_name(self, "ScriptsBucket", bucket_name=os.environ["SCRIPTS_BUCKET"])
        gold_bucket = s3.Bucket.from_bucket_name(self, "GoldBucket", bucket_name=os.environ["GOLD_BUCKET"])

        # -----------------------------
        # IAM Roles
        # -----------------------------
        glue_role = iam.Role(
            self, "GlueJobRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole")]
        )
        scripts_bucket.grant_read(glue_role)
        bronze_bucket.grant_read(glue_role)
        silver_bucket.grant_read_write(glue_role)

        lambda_role = iam.Role(
            self, "LambdaExecutionRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")]
        )
        lambda_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "glue:StartJobRun",
                    "states:StartExecution",
                    "s3:ListBucket",
                    "s3:GetObject"
                ],
                resources=["*"]
            )
        )

        step_fn_role = iam.Role(
            self, "StepFunctionRole",
            assumed_by=iam.ServicePrincipal("states.amazonaws.com")
        )
        silver_bucket.grant_read(step_fn_role)
        gold_bucket.grant_read_write(step_fn_role)
        step_fn_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "athena:StartQueryExecution",
                    "athena:GetQueryExecution",
                    "athena:GetQueryResults"
                ],
                resources=["*"]
            )
        )

        # -----------------------------
        # Glue Database + Table (Silver)
        # -----------------------------
        glue_db = glue.CfnDatabase(
            self, "SilverDatabase",
            catalog_id=self.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(name="silver")
        )

        glue.CfnTable(
            self, "SilverParquetTable",
            catalog_id=self.account,
            database_name=glue_db.database_input.name,
            table_input=glue.CfnTable.TableInputProperty(
                name="parquet_data",
                table_type="EXTERNAL_TABLE",
                parameters={"classification": "parquet", "typeOfData": "file"},
                partition_keys=[
                    glue.CfnTable.ColumnProperty(name="year", type="int"),
                    glue.CfnTable.ColumnProperty(name="month", type="int"),
                    glue.CfnTable.ColumnProperty(name="day", type="int"),
                ],
                storage_descriptor=glue.CfnTable.StorageDescriptorProperty(
                    location=f"s3://{silver_bucket.bucket_name}/data/",
                    input_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    output_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                    serde_info=glue.CfnTable.SerdeInfoProperty(
                        serialization_library="org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
                    ),
                    columns=[
                        glue.CfnTable.ColumnProperty(name="Extension", type="string"),
                        glue.CfnTable.ColumnProperty(name="Done On", type="timestamp"),
                        glue.CfnTable.ColumnProperty(name="Action", type="string"),
                        glue.CfnTable.ColumnProperty(name="Details", type="string"),
                    ]
                )
            )
        )

        # -----------------------------
        # Glue Job (CSV → Parquet)
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
                "--output_path": f"s3://{silver_bucket.bucket_name}/data/"
            },
            glue_version="4.0",
            max_capacity=2
        )

        # -----------------------------
        # Lambdas (without Step Function ARN yet)
        # -----------------------------
        s3_to_glue_lambda = _lambda.Function(
            self, "S3ToGlueLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="lambda_function.handler",
            code=_lambda.Code.from_asset("lambda/s3_to_glue"),
            role=lambda_role,
            environment={"GLUE_JOB_NAME": glue_job.ref}
        )

        glue_to_stepfn_lambda = _lambda.Function(
            self, "GlueToStepFnLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="lambda_function.handler",
            code=_lambda.Code.from_asset("lambda/glue_to_stepfn"),
            role=lambda_role,
            environment={"SILVER_BUCKET": silver_bucket.bucket_name}
        )

        generate_query_lambda = _lambda.Function(
            self, "GenerateAthenaQueryLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="lambda_function.handler",
            code=_lambda.Code.from_asset("lambda/generate_athena_query"),
            role=lambda_role
        )

        # -----------------------------
        # EventBridge Rules
        # -----------------------------
        events.Rule(
            self, "CsvUploadEventRule",
            event_pattern=events.EventPattern(
                source=["aws.s3"],
                detail_type=["Object Created"],
                detail={
                    "bucket": {"name": [bronze_bucket.bucket_name]},
                    "object": {"key": [{"suffix": ".csv"}]}
                }
            )
        ).add_target(targets.LambdaFunction(s3_to_glue_lambda))

        events.Rule(
            self, "GlueJobSuccessRule",
            event_pattern=events.EventPattern(
                source=["aws.glue"],
                detail_type=["Glue Job State Change"],
                detail={
                    "jobName": [glue_job.ref],
                    "state": ["SUCCEEDED"]
                }
            )
        ).add_target(targets.LambdaFunction(glue_to_stepfn_lambda))

        # -----------------------------
        # Step Function Tasks
        # -----------------------------
        generate_query_task = tasks.LambdaInvoke(
            self, "GenerateAthenaQuery",
            lambda_function=generate_query_lambda,
            output_path="$.Payload"
        )

        msck_repair_task = tasks.CallAwsService(
            self, "MSCKRepairSilverTable",
            service="athena",
            action="startQueryExecution",
            parameters={
                "QueryString": "MSCK REPAIR TABLE silver.parquet_data",
                "ResultConfiguration": {"OutputLocation": f"s3://{gold_bucket.bucket_name}/athena/"}
            },
            iam_resources=["*"]
        )

        athena_ctas_task = tasks.CallAwsService(
            self, "RunAthenaCTAS",
            service="athena",
            action="startQueryExecution",
            parameters={
                "QueryString": sfn.JsonPath.string_at("$.athena_query"),
                "ResultConfiguration": {"OutputLocation": f"s3://{gold_bucket.bucket_name}/athena/"}
            },
            iam_resources=["*"]
        )

        # -----------------------------
        # Step Function
        # -----------------------------
        step_fn = sfn.StateMachine(
            self, "WeeklySummaryStateMachine",
            definition=generate_query_task.next(msck_repair_task).next(athena_ctas_task),
            role=step_fn_role
        )

        # Now inject Step Function ARN into Lambda
        glue_to_stepfn_lambda.add_environment("STEP_FUNCTION_ARN", step_fn.state_machine_arn)
        step_fn.grant_start_execution(glue_to_stepfn_lambda)

