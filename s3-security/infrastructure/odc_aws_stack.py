"""
ODC AWS stack for managing S3 buckets, Lambda functions, and notifications.
"""

import pulumi
import pulumi_aws as aws
import json
from infrastructure.config import (
    common_tags,
    environment,
    notification_prefix,
    notification_suffix,
    dxr_api_url,
    dxr_api_key,
    dxr_datasource_id,
    blocked_labels,
    remove_from_source,
    lambda_runtime,
    lambda_memory_size,
    lambda_timeout,
)

class OdcAwsStack:
    def __init__(self, name):
        self.name = name

        # Create Documents S3 Bucket (main bucket)
        self.documents_bucket = aws.s3.Bucket(
            f"{name}-documents-bucket",
            acl="private",
            versioning={"enabled": True},
            tags={**common_tags, "Name": f"{name}-documents-bucket"},
        )

        # Create Allowed S3 Bucket
        self.allowed_bucket = aws.s3.Bucket(
            f"{name}-allowed-bucket",
            acl="private",
            versioning={"enabled": True},
            tags={**common_tags, "Name": f"{name}-allowed-bucket"}
        )

        # Create Quarantine S3 Bucket
        self.quarantine_bucket = aws.s3.Bucket(
            f"{name}-quarantine-bucket",
            acl="private",
            versioning={"enabled": True},
            tags={**common_tags, "Name": f"{name}-quarantine-bucket"}
        )

        # Create SNS topic for security alerts
        self.security_alert_topic = aws.sns.Topic(
            f"{name}-security-alerts",
            display_name="Security Alert Notifications",
            tags={**common_tags, "Name": f"{name}-security-alerts"},
        )

        # Create IAM role for Lambda function
        lambda_role = aws.iam.Role(
            f"{name}-lambda-role",
            assume_role_policy="""
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Action": "sts:AssumeRole",
                        "Principal": {
                            "Service": "lambda.amazonaws.com"
                        },
                        "Effect": "Allow"
                    }
                ]
            }
            """,
            tags={**common_tags, "Name": f"{name}-lambda-role"}
        )

        # Attach policies to the Lambda role
        aws.iam.RolePolicyAttachment(
            f"{name}-lambda-basic-execution",
            role=lambda_role.name,
            policy_arn="arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
        )

        # Create policy for S3 access to all buckets
        s3_access_policy = aws.iam.Policy(
            f"{name}-s3-access-policy",
            policy=pulumi.Output.all(
                self.documents_bucket.arn, 
                self.allowed_bucket.arn, 
                self.quarantine_bucket.arn
            ).apply(
                lambda arns: json.dumps({
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Action": [
                                "s3:GetObject",
                                "s3:PutObject",
                                "s3:ListBucket",
                                "s3:DeleteObject",
                                "s3:CopyObject"
                            ],
                            "Resource": [
                                arns[0],
                                f"{arns[0]}/*",
                                arns[1],
                                f"{arns[1]}/*",
                                arns[2],
                                f"{arns[2]}/*"
                            ]
                        }
                    ]
                })
            ),
            tags={**common_tags, "Name": f"{name}-s3-access-policy"}
        )

        # Create policy for SNS publishing
        sns_publish_policy = aws.iam.Policy(
            f"{name}-sns-publish-policy",
            policy=self.security_alert_topic.arn.apply(
                lambda arn: json.dumps(
                    {
                        "Version": "2012-10-17",
                        "Statement": [
                            {
                                "Effect": "Allow",
                                "Action": ["sns:Publish"],
                                "Resource": arn,
                            }
                        ],
                    }
                )
            ),
            tags={**common_tags, "Name": f"{name}-sns-publish-policy"},
        )

        # Attach S3 policy to Lambda role
        aws.iam.RolePolicyAttachment(
            f"{name}-lambda-s3-access",
            role=lambda_role.name,
            policy_arn=s3_access_policy.arn
        )

        # Attach SNS policy to Lambda role
        aws.iam.RolePolicyAttachment(
            f"{name}-lambda-sns-access",
            role=lambda_role.name,
            policy_arn=sns_publish_policy.arn,
        )

        # Create Lambda function
        self.lambda_function = aws.lambda_.Function(
            f"{name}-lambda",
            runtime=lambda_runtime,
            code=pulumi.AssetArchive({".": pulumi.FileArchive("./lambda")}),
            handler="index.lambda_handler",
            role=lambda_role.arn,
            timeout=lambda_timeout,
            memory_size=lambda_memory_size,
            environment={
                "variables": {
                    "BUCKET_NAME": self.documents_bucket.id,
                    "ENVIRONMENT": environment,
                    "ALLOWED_BUCKET": self.allowed_bucket.id,
                    "QUARANTINE_BUCKET": self.quarantine_bucket.id,
                    "ALERT_SNS_TOPIC_ARN": self.security_alert_topic.arn,
                    "DXR_API_URL": dxr_api_url,
                    "DXR_API_KEY": dxr_api_key,
                    "DXR_DATASOURCE_ID": dxr_datasource_id,
                    "BLOCKED_LABELS": blocked_labels,
                    "REMOVE_FROM_SOURCE": str(remove_from_source).lower(),
                }
            },
            tags={**common_tags, "Name": f"{name}-lambda"},
        )

        # Create CloudWatch Log Group for Lambda
        lambda_log_group = aws.cloudwatch.LogGroup(
            f"{name}-lambda-logs",
            name=self.lambda_function.name.apply(lambda name: f"/aws/lambda/{name}"),
            retention_in_days=3,  # Adjust retention period as needed
            tags={**common_tags, "Name": f"{name}-lambda-logs"}
        )

        # Grant permission to S3 to invoke the Lambda
        self.lambda_permission = aws.lambda_.Permission(
            f"{name}-lambda-permission",
            action="lambda:InvokeFunction",
            function=self.lambda_function.name,
            principal="s3.amazonaws.com",
            source_arn=self.documents_bucket.arn
        )

        # Add bucket notification for the Lambda function
        self.bucket_notification = aws.s3.BucketNotification(
            resource_name=f"{name}-bucket-notification",
            bucket=self.documents_bucket.id,
            lambda_functions=[
                {
                    "lambdaFunctionArn": self.lambda_function.arn,
                    "events": ["s3:ObjectCreated:*"],
                    "filterPrefix": notification_prefix or "",
                    "filterSuffix": notification_suffix or "",
                }
            ],
            opts=pulumi.ResourceOptions(depends_on=[self.lambda_permission]),
        )
