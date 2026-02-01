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
# Lambda IAM Role
########################

resource "aws_iam_role" "lambda_role" {
  name = "${local.project}-lambda-${local.env}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "lambda_policy" {
  role = aws_iam_role.lambda_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "glue:StartJobRun"
        ]
        Resource = aws_glue_job.csv_to_parquet.arn
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "*"
      }
    ]
  })
}

########################
# Lambda Packaging (auto-zip)
########################

data "archive_file" "lambda_zip" {
  type        = "zip"
  source_dir  = "${path.module}/lambda"
  output_path = "${path.module}/lambda/s3_to_glue.zip"
}

########################
# Lambda Function
########################

resource "aws_lambda_function" "s3_to_glue" {
  function_name = "${local.project}-s3-to-glue-${local.env}"
  role          = aws_iam_role.lambda_role.arn
  handler       = "s3_to_glue.handler"
  runtime       = "python3.11"

  filename = data.archive_file.lambda_zip.output_path

  environment {
    variables = {
      GLUE_JOB_NAME = aws_glue_job.csv_to_parquet.name
    }
  }

  depends_on = [aws_iam_role_policy.lambda_policy]
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
      bucket = {
        name = [data.aws_s3_bucket.bronze_bucket.bucket]
      }
      object = {
        key = [
          { suffix = ".csv" }
        ]
      }
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

