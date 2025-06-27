"""
Main entry point for Pulumi program.
This file imports the infrastructure module to deploy the ODC AWS resources.
"""

import pulumi
from infrastructure.odc_aws_stack import OdcAwsStack

# Create the ODC AWS Stack
stack = OdcAwsStack("dxr-odc")

# Export key outputs
pulumi.export('documents_bucket_name', stack.documents_bucket.id)
pulumi.export('allowed_bucket_name', stack.allowed_bucket.id)
pulumi.export('quarantine_bucket_name', stack.quarantine_bucket.id)
pulumi.export('lambda_function_name', stack.lambda_function.name)
pulumi.export('lambda_function_arn', stack.lambda_function.arn)
pulumi.export("security_alert_topic_arn", stack.security_alert_topic.arn)
