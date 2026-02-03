import os

from aws_cdk import (
    Stack,
    aws_events as events,
    aws_events_targets as targets,
    aws_glue as glue,
    aws_iam as iam,
    aws_lambda as _lambda,
    aws_s3 as s3,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
)
from constructs import Construct

LAMBDA_CODE_ROOT = "lambda"
GLUE_SCRIPT_NAME = "csv_to_parquet.py"
GLUE_JOB_NAME = "shyftoff-pipeline-csv-to-parquet-dev"


class ShyftoffPipelineStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        buckets = self._load_buckets()
        roles = self._create_roles(buckets)
        self._create_glue_catalog(buckets["silver"])

        glue_job = self._create_glue_job(
            role=roles["glue"],
            scripts_bucket=buckets["scripts"],
            silver_bucket=buckets["silver"],
        )

        lambdas = self._create_lambdas(roles["lambda"], glue_job, buckets["silver"])

        state_machine = self._create_state_machine(
            role=roles["stepfn"],
            gold_bucket=buckets["gold"],
            generate_query_lambda=lambdas["generate_query"],
        )

        lambdas["glue_to_stepfn"].add_environment(
            "STEP_FUNCTION_ARN", state_machine.state_machine_arn
        )

        self._create_event_rules(
            bronze_bucket=buckets["bronze"],
            glue_job=glue_job,
            s3_to_glue_lambda=lambdas["s3_to_glue"],
            glue_to_stepfn_lambda=lambdas["glue_to_stepfn"],
        )

    def _load_buckets(self):
        return {
            "bronze": s3.Bucket.from_bucket_name(
                self, "BronzeBucket", bucket_name=os.environ["BRONZE_BUCKET"]
            ),
            "silver": s3.Bucket.from_bucket_name(
                self, "SilverBucket", bucket_name=os.environ["SILVER_BUCKET"]
            ),
            "gold": s3.Bucket.from_bucket_name(
                self, "GoldBucket", bucket_name=os.environ["GOLD_BUCKET"]
            ),
            "scripts": s3.Bucket.from_bucket_name(
                self, "ScriptsBucket", bucket_name=os.environ["SCRIPTS_BUCKET"]
            ),
        }

    def _create_roles(self, buckets):
        glue_role = iam.Role(
            self,
            "GlueJobRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                )
            ],
        )
        buckets["scripts"].grant_read(glue_role)
        buckets["bronze"].grant_read(glue_role)
        buckets["silver"].grant_read_write(glue_role)

        lambda_role = iam.Role(
            self,
            "LambdaExecutionRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                )
            ],
        )
        lambda_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "glue:StartJobRun",
                    "states:StartExecution",
                    "s3:ListBucket",
                    "s3:GetObject",
                ],
                resources=["*"],
            )
        )

        step_fn_role = iam.Role(
            self,
            "StepFunctionRole",
            assumed_by=iam.ServicePrincipal("states.amazonaws.com"),
        )
        buckets["silver"].grant_read(step_fn_role)
        buckets["gold"].grant_read_write(step_fn_role)
        step_fn_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "athena:StartQueryExecution",
                    "athena:GetQueryExecution",
                    "athena:GetQueryResults",
                ],
                resources=["*"],
            )
        )
        step_fn_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "glue:GetDatabase",
                    "glue:GetDatabases",
                    "glue:GetTable",
                    "glue:GetTables",
                    "glue:GetPartition",
                    "glue:GetPartitions",
                    "glue:CreateTable",
                    "glue:UpdateTable",
                    "glue:DeleteTable",
                ],
                resources=[
                    f"arn:aws:glue:{self.region}:{self.account}:catalog",
                    f"arn:aws:glue:{self.region}:{self.account}:database/*",
                    f"arn:aws:glue:{self.region}:{self.account}:table/*/*",
                ],
            )
        )

        return {"glue": glue_role, "lambda": lambda_role, "stepfn": step_fn_role}

    def _create_glue_catalog(self, silver_bucket):
        glue.CfnDatabase(
            self,
            "SilverDatabase",
            catalog_id=self.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(name="silver"),
        )

        glue.CfnDatabase(
            self,
            "GoldDatabase",
            catalog_id=self.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(name="gold"),
        )

        glue.CfnTable(
            self,
            "SilverParquetTable",
            catalog_id=self.account,
            database_name="silver",
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
                    ],
                ),
            ),
        )

    def _create_glue_job(self, role, scripts_bucket, silver_bucket):
        return glue.CfnJob(
            self,
            "CsvToParquetJob",
            name=GLUE_JOB_NAME,
            role=role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=f"s3://{scripts_bucket.bucket_name}/{GLUE_SCRIPT_NAME}",
            ),
            default_arguments={
                "--job-language": "python",
                "--TempDir": f"s3://{silver_bucket.bucket_name}/temp/",
                "--output_path": f"s3://{silver_bucket.bucket_name}/data/",
            },
            glue_version="4.0",
            max_capacity=2,
        )

    def _create_lambdas(self, role, glue_job, silver_bucket):
        s3_to_glue_lambda = _lambda.Function(
            self,
            "S3ToGlueLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="lambda_function.handler",
            code=_lambda.Code.from_asset(f"{LAMBDA_CODE_ROOT}/s3_to_glue"),
            role=role,
            environment={
                "GLUE_JOB_NAME": glue_job.ref,
                "SILVER_BUCKET": silver_bucket.bucket_name,
            },
        )

        glue_to_stepfn_lambda = _lambda.Function(
            self,
            "GlueToStepFnLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="lambda_function.handler",
            code=_lambda.Code.from_asset(f"{LAMBDA_CODE_ROOT}/glue_to_stepfn"),
            role=role,
        )

        generate_query_lambda = _lambda.Function(
            self,
            "GenerateAthenaQueryLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="lambda_function.handler",
            code=_lambda.Code.from_asset(f"{LAMBDA_CODE_ROOT}/generate_athena_query"),
            role=role,
        )

        return {
            "s3_to_glue": s3_to_glue_lambda,
            "glue_to_stepfn": glue_to_stepfn_lambda,
            "generate_query": generate_query_lambda,
        }

    def _create_state_machine(self, role, gold_bucket, generate_query_lambda):
        generate_query_task = tasks.LambdaInvoke(
            self,
            "GenerateAthenaQuery",
            lambda_function=generate_query_lambda,
            output_path="$.Payload",
        )

        msck_repair_task = tasks.CallAwsService(
            self,
            "MSCKRepairSilverTable",
            service="athena",
            action="startQueryExecution",
            parameters={
                "QueryString": "MSCK REPAIR TABLE silver.parquet_data",
                "ResultConfiguration": {
                    "OutputLocation": f"s3://{gold_bucket.bucket_name}/athena/"
                },
            },
            integration_pattern=sfn.IntegrationPattern.REQUEST_RESPONSE,
            iam_resources=["*"],
            result_path="$.msck_result",
        )

        drop_weekly_table_task = tasks.CallAwsService(
            self,
            "DropWeeklySummaryTable",
            service="athena",
            action="startQueryExecution",
            parameters={
                "QueryString": "DROP TABLE IF EXISTS gold.weekly_summary",
                "ResultConfiguration": {
                    "OutputLocation": f"s3://{gold_bucket.bucket_name}/athena/"
                },
            },
            integration_pattern=sfn.IntegrationPattern.REQUEST_RESPONSE,
            iam_resources=["*"],
            result_path="$.drop_result",
        )

        athena_ctas_task = tasks.CallAwsService(
            self,
            "RunAthenaCTAS",
            service="athena",
            action="startQueryExecution",
            parameters={
                "QueryString.$": "$.athena_query",
                "ResultConfiguration": {
                    "OutputLocation": f"s3://{gold_bucket.bucket_name}/weekly_summary/"
                },
            },
            integration_pattern=sfn.IntegrationPattern.REQUEST_RESPONSE,
            iam_resources=["*"],
            result_path="$.ctas_result",
        )

        definition = (
            generate_query_task.next(msck_repair_task)
            .next(drop_weekly_table_task)
            .next(athena_ctas_task)
        )

        return sfn.StateMachine(
            self, "WeeklySummaryStateMachine", definition=definition, role=role
        )

    def _create_event_rules(
        self,
        bronze_bucket,
        glue_job,
        s3_to_glue_lambda,
        glue_to_stepfn_lambda,
    ):
        csv_upload_rule = events.Rule(
            self,
            "CsvUploadEventRule",
            event_pattern=events.EventPattern(
                source=["aws.s3"],
                detail_type=["Object Created"],
                detail={
                    "bucket": {"name": [bronze_bucket.bucket_name]},
                    "object": {"key": [{"suffix": ".csv"}]},
                },
            ),
        )
        csv_upload_rule.add_target(targets.LambdaFunction(s3_to_glue_lambda))

        glue_job_success_rule = events.Rule(
            self,
            "GlueJobSuccessRule",
            event_pattern=events.EventPattern(
                source=["aws.glue"],
                detail_type=["Glue Job State Change"],
                detail={"jobName": [glue_job.ref], "state": ["SUCCEEDED"]},
            ),
        )
        glue_job_success_rule.add_target(targets.LambdaFunction(glue_to_stepfn_lambda))
