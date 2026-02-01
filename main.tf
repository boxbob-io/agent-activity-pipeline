terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = "us-east-1"
}

########################
# Locals
########################

locals {
  project = "shyftoff-pipeline"
  env     = "dev"

  # Check for Python files in the lambda folder
  lambda_files = fileset("${path.module}/lambda", "**/*.py")
}

########################
# Existing Resources (DATA ONLY)
########################

data "aws_s3_bucket" "bronze_bucket" {
  bucket = "${local.project}-bronze-${local.env}"
}

data "aws_s3_bucket" "silver_bucket" {
  bucket = "${local.project}-silver-${local.env}"
}

data "aws_s3_bucket" "scripts_bucket" {
  bucket = "${local.project}-scripts-${local.env}"
}

data "aws_iam_role" "glue_role" {
  name = "${local.project}-glue-${local.env}"
}

data "aws_iam_role" "lambda_role" {
  name = "${local.project}-lambda-${local.env}"
}

########################
# Fail-fast if Lambda folder is empty
########################

resource "null_resource" "validate_lambda" {
  count = length(local.lambda_files) == 0 ? 1 : 0

  provisioner "local-exec" {
    command = "echo 'ERROR: Lambda folder is empty or no .py files found!' && exit 1"
  }
}

########################
# Archive Lambda folder
########################

data "archive_file" "lambda_zip" {
  count       = length(local.lambda_files) > 0 ? 1 : 0
  type        = "zip"
  source_dir  = "${path.module}/lambda"
  output_path = "${path.module}/lambda/s3_to_glue.zip"
  depends_on  = [null_resource.validate_lambda]
}

########################
# Upload Glue Script
########################

resource "aws_s3_object" "glue_script" {
  bucket = data.aws_s3_bucket.scripts_bucket.bucket
  key    = "scripts/csv_to_parquet.py"
  source = "${path.module}/scripts/csv_to_parquet.py"
  etag   = filemd5("${path.module}/scripts/csv_to_parquet.py")
}

########################
# Glue Job
########################

resource "aws_glue_job" "csv_to_parquet" {
  name     = "${local.project}-csv-to-parquet-${local.env}"
  role_arn = data.aws_iam_role.glue_role.arn

  glue_version = "4.0"
  max_capacity = 2

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${data.aws_s3_bucket.scripts_bucket.bucket}/${aws_s3_object.glue_script.key}"
  }

  default_arguments = {
    "--job-language" = "python"
    "--TempDir"      = "s3://${data.aws_s3_bucket.silver_bucket.bucket}/temp/"
    "--output_path"  = "s3://${data.aws_s3_bucket.silver_bucket.bucket}/"
  }
}

########################
# Lambda Function
########################

resource "aws_lambda_function" "s3_to_glue" {
  function_name = "${local.project}-s3-to-glue-${local.env}"
  role          = data.aws_iam_role.lambda_role.arn
  handler       = "s3_to_glue.handler"
  runtime       = "python3.11"

  filename = data.archive_file.lambda_zip[0].output_path

  environment {
    variables = {
      GLUE_JOB_NAME = aws_glue_job.csv_to_parquet.name
    }
  }

  depends_on = [data.archive_file.lambda_zip]
}

########################
# EventBridge Rule (S3 CSV Upload)
########################

resource "aws_cloudwatch_event_rule" "s3_csv_upload" {
  name = "${local.project}-s3-csv-upload-${local.env}"

  event_pattern = jsonencode({
    source      = ["aws.s3"]
    detail-type = ["Object Created"]
    detail = {
      bucket = { name = [data.aws_s3_bucket.bronze_bucket.bucket] }
      object = { key = [{ suffix = ".csv" }] }
    }
  })
}

########################
# EventBridge Target → Lambda
########################

resource "aws_cloudwatch_event_target" "lambda_target" {
  rule      = aws_cloudwatch_event_rule.s3_csv_upload.name
  target_id = "S3ToGlueLambda"
  arn       = aws_lambda_function.s3_to_glue.arn
}

########################
# Lambda Permission for EventBridge
########################

resource "aws_lambda_permission" "allow_eventbridge" {
  statement_id  = "AllowEventBridgeInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.s3_to_glue.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.s3_csv_upload.arn
}

########################
# Glue role policy to access scripts bucket
########################

resource "aws_iam_role_policy" "glue_s3_access" {
  name = "GlueScriptAccess"
  role = data.aws_iam_role.glue_role.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:ListBucket"]
        Resource = [
          "arn:aws:s3:::${data.aws_s3_bucket.scripts_bucket.bucket}",
          "arn:aws:s3:::${data.aws_s3_bucket.scripts_bucket.bucket}/*"
        ]
      }
    ]
  })
}

