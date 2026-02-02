# Agent Activity ETL Pipeline

## Overview

This is an automated data pipeline to serve as the ETL and analytics backbone agent event data. Built on AWS CDK, it is triggered on the ingestion of raw CSV data, transforms it into Parquet format, and aggregates it into weekly summaries for reporting and analysis. The pipeline uses several AWS services such as S3, Glue, Lambda, Step Functions, Athena, and EventBridge to ensure a seamless event-driven workflow. All AWS resources are managed by the AWS CDK stack and can be re-deployed at any time via GitHub action.

*This problem statement was broken into two parts, I'll separate the architectures accordingly.*

## Architecture : Part One
The goal of this process is to generate a transformed version of the agent event data using SQL from an ingested CSV file. My secondary goal was to ensure that this is a _nearly_ production ready pipeline that will scale nicely given more dramatically sized agent event datasets.

![Pipeline One](images/shyftoff_pipeline_one.png)



## Architecture : Part Two
This is the portion of the process that has not yet been implemented, as the problem statement suggested the _description_ of a notification service that would notify of missing data in the summary table for a 24 hour window.

![Pipeline One](images/notification_service_two.png)

The proposed extension here would serve both for this implementation and any further notifications that we would like to extend to cover. As proposed, there would be a lambda we'll call "check-summary-lambda". This lambda would be tasked with validating the previous day's event data. I imagine that the simplest validation method for identifying gaps within intervals that don't exist would be with a simple count of the distinct intervals where the values are within the provided 24 hour window (either the last 24 hours from now or yesterday); this resolves two problems simultaneoously, any missing intervals would flagged, as well as failures to load altogether; anything less than 48 intervals is worth notifying. Check-summary-lambda would be triggered from two places, a cron scheduled eventbridge and a custom put event triggered at the end of the insert process described in the first part. Our cron would be scheduled to run daily just outside of the expected SLA of our first event process. This lambda. if anyhting is found out of the ordinary would send a message to an SNS queeue, which would in turn trigger a lambda to do the actual notification.
