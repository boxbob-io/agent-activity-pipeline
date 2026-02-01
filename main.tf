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

locals {
  project = "shyftoff-pipeline"
  env     = "dev"
}

# -------------------
# Bronze Bucket (raw CSVs)
# -------------------
resource "aws_s3_bucket" "bronze_bucket" {
  bucket = "${local.project}-bronze-${local.env}"

  tags = {
    Project = local.project
    Env     = local.env
    Purpose = "daily-csv-ingestion"
  }
}

resource "aws_s3_bucket_versioning" "bronze_bucket" {
  bucket = aws_s3_bucket.bronze_bucket.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "bronze_bucket" {
  bucket = aws_s3_bucket.bronze_bucket.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# -------------------
# Silver Bucket (Parquet output)
# -------------------
resource "aws_s3_bucket" "silver_bucket" {
  bucket = "${local.project}-silver-${local.env}"

  tags = {
    Project = local.project
    Env     = local.env
    Purpose = "processed-parquet"
  }
}

resource "aws_s3_bucket_versioning" "silver_bucket" {
  bucket = aws_s3_bucket.silver_bucket.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "silver_bucket" {
  bucket = aws_s3_bucket.silver_bucket.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# -------------------
# Glue IAM Role
# -------------------
resource "aws_iam_role" "glue_role" {
  name = "${local.project}-glue-${local.env}"
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Principal = {
          Service = "glue.amazonaws.com"
        },
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy" "glue_policy" {
  name = "${local.project}-glue-policy-${local.env}"
  role = aws_iam_role.glue_role.id
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "s3:GetObject",
          "s3:ListBucket"
        ],
        Resource = [
          aws_s3_bucket.bronze_bucket.arn,
          "${aws_s3_bucket.bronze_bucket.arn}/*"
        ]
      },
      {
        Effect = "Allow",
        Action = [
          "s3:PutObject",
          "s3:ListBucket"
        ],
        Resource = [
          aws_s3_bucket.silver_bucket.arn,
          "${aws_s3_bucket.silver_bucket.arn}/*"
        ]
      },
      {
        Effect = "Allow",
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ],
        Resource = "*"
      }
    ]
  })
}

# -------------------
# Glue Job
# -------------------
resource "aws_glue_job" "csv_to_parquet" {
  name     = "${local.project}-csv-to-parquet-${local.env}"
  role_arn = aws_iam_role.glue_role.arn

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://<YOUR_SCRIPT_BUCKET>/scripts/csv_to_parquet.py"
  }

  max_capacity = 2
  glue_version = "3.0"

  default_arguments = {
    "--TempDir"     = "s3://${aws_s3_bucket.silver_bucket.bucket}/temp/"
    "--input_path"  = "s3://${aws_s3_bucket.bronze_bucket.bucket}/"
    "--output_path" = "s3://${aws_s3_bucket.silver_bucket.bucket}/"
  }
}

# -------------------
# EventBridge Rule to trigger Glue on new CSV
# -------------------
resource "aws_cloudwatch_event_rule" "s3_csv_rule" {
  name        = "${local.project}-s3-csv-upload-${local.env}"
  description = "Trigger Glue job only when new CSVs are uploaded to bronze bucket"

  event_pattern = jsonencode({
    "source": ["aws.s3"],
    "detail-type": ["Object Created"],
    "detail": {
      "bucket": {
        "name": [aws_s3_bucket.bronze_bucket.bucket]
      },
      "object": {
        "key": [{
          "suffix": ".csv"
        }]
      }
    }
  })
}

# Glue Target for EventBridge
resource "aws_cloudwatch_event_target" "glue_target" {
  rule      = aws_cloudwatch_event_rule.s3_csv_rule.name
  target_id = "glueJobTarget"
  arn       = aws_glue_job.csv_to_parquet.arn
}

# Permission for EventBridge to start Glue job
resource "aws_glue_trigger" "csv_trigger" {
  name     = "${local.project}-csv-trigger-${local.env}"
  type     = "EVENT"
  description = "Trigger Glue job on new CSV files"

  actions {
    job_name = aws_glue_job.csv_to_parquet.name
  }

  predicate {
    conditions {
      logical_operator = "EQUALS"
      state            = "SUCCEEDED"
    }
  }
}

