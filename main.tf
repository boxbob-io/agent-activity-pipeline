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

resource "aws_s3_bucket" "ingest_bucket" {
  bucket = "${local.project}-agent-activity-${local.env}"

  tags = {
    Project = local.project
    Env     = local.env
    Purpose = "daily-csv-ingestion"
  }
}

resource "aws_s3_bucket_versioning" "ingest_bucket" {
  bucket = aws_s3_bucket.daily_csv.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "ingest_bucket" {
  bucket = aws_s3_bucket.daily_csv.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

