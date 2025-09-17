import logging
import os
import ssl
import urllib.error
import urllib.request
from datetime import datetime

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    """
    Lambda function to fetch streaming JSON data from API and store directly in S3
    """
    try:
        # Get configuration from environment variables or event
        api_endpoint = event.get("app_url") or os.environ.get("API_ENDPOINT")
        pat_token = event.get("pat_token") or os.environ.get("PAT_TOKEN")
        s3_bucket = os.environ["S3_BUCKET"]

        if not api_endpoint:
            return {
                "statusCode": 400,
                "body": "Missing 'app_url' in event payload or API_ENDPOINT environment variable",
            }

        if not pat_token:
            return {
                "statusCode": 400,
                "body": "Missing 'pat_token' in event payload or PAT_TOKEN environment variable",
            }

        # Initialize S3 client
        s3 = boto3.client("s3")

        # Use the files endpoint
        if not api_endpoint.endswith("/api/v1/files"):
            if api_endpoint.endswith("/"):
                export_url = f"{api_endpoint}api/v1/files"
            else:
                export_url = f"{api_endpoint}/api/v1/files"
        else:
            export_url = api_endpoint

        logger.info(f"Fetching data from {export_url}")

        # Generate S3 key with timestamp and partitioning
        timestamp = datetime.utcnow().strftime("%Y/%m/%d/%H%M%S")
        # s3_key = f"raw-data/year={timestamp[:4]}/month={timestamp[5:7]}/day={timestamp[8:10]}/files_{timestamp[11:]}.jsonl"
        s3_key = "data.jsonl"

        # Create request with PAT token
        request = urllib.request.Request(
            export_url, headers={"Authorization": f"Bearer {pat_token}"}
        )

        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        # Fetch data and stream to S3
        with urllib.request.urlopen(request, context=ssl_context) as response:
            logger.info(
                f"Successfully connected to API. Response status: {response.status}"
            )

            # Upload the streaming response directly to S3
            s3.upload_fileobj(
                response,
                s3_bucket,
                s3_key,
                ExtraArgs={
                    "ContentType": "application/json",
                    "Metadata": {
                        "source": "files-api",
                        "timestamp": datetime.utcnow().isoformat(),
                        "endpoint": export_url,
                    },
                },
            )

        logger.info(f"Successfully uploaded data to s3://{s3_bucket}/{s3_key}")

        return {
            "statusCode": 200,
            "body": f"Successfully exported files data to s3://{s3_bucket}/{s3_key}",
        }

    except urllib.error.HTTPError as e:
        error_msg = f"HTTP error {e.code}: {e.reason}"
        logger.error(error_msg)

        return {"statusCode": 500, "body": error_msg}
    except urllib.error.URLError as e:
        error_msg = f"URL error: {e.reason}"
        logger.error(error_msg)

        return {"statusCode": 500, "body": error_msg}
    except Exception as e:
        error_msg = f"Error in lambda_handler: {str(e)}"
        logger.error(error_msg)

        return {"statusCode": 500, "body": error_msg}
