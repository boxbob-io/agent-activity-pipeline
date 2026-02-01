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
# Existing S3 Buckets
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
# IAM Role for Glue
########################

resource "aws_iam_role" "glue_role" {
  name = "${local.project}-glue-${local.env}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "glue_policy" {
  role = aws_iam_role.glue_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          data.aws_s3_bucket.bronze_bucket.arn,
          "${data.aws_s3_bucket.bronze_bucket.arn}/*",
          data.aws_s3_bucket.silver_bucket.arn,
          "${data.aws_s3_bucket.silver_bucket.arn}/*",
          data.aws_s3_bucket.scripts_bucket.arn,
          "${data.aws_s3_bucket.scripts_bucket.arn}/*"
        ]
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
# Glue Job
########################

resource "aws_glue_job" "csv_to_parquet" {
  name     = "${local.project}-csv-to-parquet-${local.env}"
  role_arn = aws_iam_role.glue_role.arn

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${data.aws_s3_bucket.scripts_bucket.bucket}/${aws_s3_object.glue_script.key}"
  }

  glue_version = "4.0"
  max_capacity = 2

  default_arguments = {
    "--TempDir"     = "s3://${data.aws_s3_bucket.silver_bucket.bucket}/temp/"
    "--output_path" = "s3://${data.aws_s3_bucket.silver_bucket.bucket}/"
    "--job-language" = "python"
  }
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
        key = [{
          suffix = ".csv"
        }]
      }
    }
  })
}

########################
# EventBridge → Glue Target
########################

resource "aws_cloudwatch_event_target" "glue_job_target" {
  rule      = aws_cloudwatch_event_rule.s3_csv_upload.name
  target_id = "GlueCsvToParquet"
  arn       = aws_glue_job.csv_to_parquet.arn
  role_arn = aws_iam_role.glue_role.arn

  input_transformer {
    input_paths = {
      bucket = "$.detail.bucket.name"
      key    = "$.detail.object.key"
    }

    input_template = <<EOF
{
  "Arguments": {
    "--s3_bucket": <bucket>,
    "--s3_key": <key>
  }
}
EOF
  }
}

########################
# Allow EventBridge to Start Glue
########################

resource "aws_iam_role_policy" "eventbridge_glue_start" {
  role = aws_iam_role.glue_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = "glue:StartJobRun"
      Resource = aws_glue_job.csv_to_parquet.arn
    }]

