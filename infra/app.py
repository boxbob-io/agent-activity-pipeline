#!/usr/bin/env python3
import os
import aws_cdk as cdk
from shyftoff_pipeline.shyftoff_pipeline_stack import ShyftoffPipelineStack

app = cdk.App()
ShyftoffPipelineStack(app, "ShyftoffPipelineStack",
                      env=cdk.Environment(account=os.environ["CDK_DEFAULT_ACCOUNT"],
                                          region=os.environ.get("AWS_REGION", "us-east-1")))
app.synth()

