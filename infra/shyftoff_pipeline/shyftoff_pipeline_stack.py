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

        gold_bucket = s3.Bucket.from_bucket_name(
            self, "GoldBucket",
            bucket_name=os.environ["GOLD_BUCKET"]
        )

        # -----------------------------
        # IAM Roles
        # -----------------------------
        # Glue Job role
        glue_role = iam.Role(
            self, "GlueJobRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole")
            ]
        )
        scripts_bucket.grant_read(glue_role)
        bronze_bucket.grant_read(glue_role)
        silver_bucket.grant_read_write(glue_role)

        # Lambda execution role
        lambda_role = iam.Role(
            self, "LambdaExecutionRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole"),
            ]
        )
        lambda_role.add_to_policy(
            iam.PolicyStatement(
                actions=["glue:StartJobRun", "states:StartExecution", "s3:ListBucket", "s3:GetObject"],
                resources=["*"]
            )
        )

        # Step Function role with Athena permissions
        step_fn_role = iam.Role(
            self, "StepFunctionRole",
            assumed_by=iam.ServicePrincipal("states.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("AWSStepFunctionsFullAccess")
            ]
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
                "--output_path": f"s3://{silver_bucket.bucket_name}/"
            },
            glue_version="4.0",
            max_capacity=2
        )

        # -----------------------------
        # Lambda: Trigger Glue job on CSV upload
        # -----------------------------
        lambda_trigger_glue = _lambda.Function(
            self, "S3ToGlueLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="lambda_function.handler",
            code=_lambda.Code.from_asset("infra/lambda/glue_to_stepfn"),
            role=lambda_role,
            environment={
                "GLUE_JOB_NAME": glue_job.ref,
                "SILVER_BUCKET": silver_bucket.bucket_name,
                "STEP_FUNCTION_ARN": "PLACEHOLDER"  # updated after Step Function is created
            }
        )

        # EventBridge: CSV upload → Lambda
        rule_csv_upload = events.Rule(
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
        rule_csv_upload.add_target(targets.LambdaFunction(lambda_trigger_glue))

        # -----------------------------
        # Step Function Lambda: Generate Athena Query
        # -----------------------------
        generate_query_lambda = _lambda.Function(
            self, "GenerateAthenaQueryLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset("infra/lambda/generate_athena_query"),
            role=lambda_role
        )

        # Step Function Task 1: Generate Athena query
        generate_query_task = tasks.LambdaInvoke(
            self, "Generate Athena Query",
            lambda_function=generate_query_lambda,
            output_path="$.Payload"
        )

        # Step Function Task 2: Execute Athena query dynamically
        athena_task = tasks.CallAwsService(
            self, "RunAthenaQuery",
            service="athena",
            action="startQueryExecution",
            parameters={
                "QueryString": sfn.JsonPath.string_at("$.athena_query"),  # dynamic SQL
                "ResultConfiguration": {
                    "OutputLocation": f"s3://{gold_bucket.bucket_name}/"
                }
            },
            integration_pattern=sfn.IntegrationPattern.REQUEST_RESPONSE,
            iam_resources=["*"]
        )

        # Chain Step Function tasks
        definition = generate_query_task.next(athena_task)

        # Create Step Function
        step_fn = sfn.StateMachine(
            self, "WeeklySummaryStateMachine",
            definition=definition,
            role=step_fn_role
        )

        # Update Lambda environment now that Step Function exists
        lambda_trigger_glue.add_environment("STEP_FUNCTION_ARN", step_fn.state_machine_arn)

        # -----------------------------
        # EventBridge: Glue job completion → Lambda
        # -----------------------------
        glue_completion_rule = events.Rule(
            self, "GlueJobCompletionRule",
            event_pattern=events.EventPattern(
                source=["aws.glue"],
                detail_type=["Glue Job State Change"],
                detail={
                    "jobName": [glue_job.ref],
                    "state": ["SUCCEEDED"]
                }
            )
        )
        glue_completion_rule.add_target(targets.LambdaFunction(lambda_trigger_glue))

