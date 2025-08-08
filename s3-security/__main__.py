import pulumi
import pulumi_aws as aws
import os

# Buckets
source_bucket = aws.s3.Bucket("source-bucket", bucket="dxr-s3security-demo-documents")
allowed_bucket = aws.s3.Bucket("allowed-bucket", bucket="dxr-s3security-demo-allowed")
quarantine_bucket = aws.s3.Bucket("quarantine-bucket", bucket="dxr-s3security-demo-quarantine")

# Lambda role
lambda_role = aws.iam.Role(
    "lambda-role",
    assume_role_policy='''{
        "Version": "2012-10-17",
        "Statement": [{
            "Action": "sts:AssumeRole",
            "Principal": {"Service": "lambda.amazonaws.com"},
            "Effect": "Allow",
            "Sid": ""
        }]
    }''',
)

aws.iam.RolePolicyAttachment(
    "lambda-basic-exec",
    role=lambda_role.name,
    policy_arn="arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
)
aws.iam.RolePolicyAttachment(
    "lambda-s3-full",
    role=lambda_role.name,
    policy_arn="arn:aws:iam::aws:policy/AmazonS3FullAccess",
)

# Build output zip is created by build_lambda.sh
lambda_fn = aws.lambda_.Function(
    "odc-lambda",
    role=lambda_role.arn,
    runtime="python3.11",
    handler="lambda_function.lambda_handler",
    code=pulumi.FileArchive("lambda_package.zip"),
    timeout=300,
    environment=aws.lambda_.FunctionEnvironmentArgs(
        variables={
            "SOURCE_BUCKET": source_bucket.bucket,
            "ALLOWED_BUCKET": allowed_bucket.bucket,
            "QUARANTINE_BUCKET": quarantine_bucket.bucket,
            "DXR_API_URL": os.getenv("DXR_API_URL", ""),
            "DXR_DATASOURCE_ID": os.getenv("DXR_DATASOURCE_ID", ""),
            "DXR_API_KEY": os.getenv("DXR_API_KEY", ""),
            "ALLOWED_LABELS": os.getenv("ALLOWED_LABELS", "Allowed"),
            "BLOCKED_LABELS": os.getenv("BLOCKED_LABELS", ""),
            "REMOVE_FROM_SOURCE": os.getenv("REMOVE_FROM_SOURCE", "false"),
        }
    ),
)

# Allow S3 bucket to invoke the Lambda
invoke_permission = aws.lambda_.Permission(
    "allow-s3-invoke",
    action="lambda:InvokeFunction",
    function=lambda_fn.name,
    principal="s3.amazonaws.com",
    source_arn=source_bucket.arn,
)

# Configure S3 event notification for object created (PUT)
notification = aws.s3.BucketNotification(
    "source-bucket-notification",
    bucket=source_bucket.id,
    lambda_functions=[
        aws.s3.BucketNotificationLambdaFunctionArgs(
            lambda_function_arn=lambda_fn.arn,
            events=["s3:ObjectCreated:Put"],
        )
    ],
    opts=pulumi.ResourceOptions(depends_on=[invoke_permission]),
)

pulumi.export("source_bucket", source_bucket.bucket)
pulumi.export("allowed_bucket", allowed_bucket.bucket)
pulumi.export("quarantine_bucket", quarantine_bucket.bucket)
pulumi.export("lambda_name", lambda_fn.name)
