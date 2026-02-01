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
# Existing Infrastructure (DATA ONLY)
########################

data "aws_s3_bucket" "bronze_bucket" {
  bucket = "${local.project}-bronze-${local.env}"
}

data "aws_s3_bucket" "silver_bucket" {
  bucket = "${local.project}-silver-${local.env}"
}

data "aws_s3_bucket" "scripts_bucket" {
  bucket = "${local.project}-scripts"
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
# Glue Job (Per-file processing)
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
# EventBridge Rule (CSV Upload)
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
# EventBridge → Glue Target
########################

resource "aws_cloudwatch_event_target" "glue_job_target" {
  rule      = aws_cloudwatch_event_rule.s3_csv_upload.name
  target_id = "GlueCsvToParquet"
  arn       = aws_glue_job.csv_to_parquet.arn

  # This role must ALREADY allow events.amazonaws.com
  # to call glue:StartJobRun
  role_arn = data.aws_iam_role.glue_role.arn

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

