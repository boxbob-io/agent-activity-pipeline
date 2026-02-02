# Agent Activity ETL Pipeline

## Overview

This is an automated data pipeline to serve as the ETL and analytics backbone agent event data. Built on AWS CDK, it is triggered on the ingestion of raw CSV data, transforms it into Parquet format, and aggregates it into weekly summaries for reporting and analysis. The pipeline uses several AWS services such as S3, Glue, Lambda, Step Functions, Athena, and EventBridge to ensure a seamless event-driven workflow. All AWS resources are managed by the AWS CDK stack and can be re-deployed at any time via GitHub action.

*This problem statement was broken into two parts, I'll separate the architectures accordingly.*

## Architecture : Part One
The goal of this process is to generate a transformed version of the agent event data using SQL from an ingested CSV file. My secondary goal was to ensure that this is a _nearly_ production ready pipeline that will scale nicely given more dramatically sized agent event datasets.

![alt text](image-url)
