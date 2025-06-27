import os
import logging
from typing import List
import boto3
import urllib.parse
from dxrpy import DXRClient
from dxrpy.utils import File

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dxr_api_key = os.environ["DXR_API_KEY"]
dxr_api_url = os.environ["DXR_API_URL"]
dxr_datasource_id = os.environ.get("DXR_DATASOURCE_ID")

allowed_bucket = os.environ["ALLOWED_BUCKET"]
quarantine_bucket = os.environ["QUARANTINE_BUCKET"]
alert_topic_arn = os.environ.get("ALERT_SNS_TOPIC_ARN")

# Whether to delete files from source bucket after processing
remove_from_source = os.environ.get("REMOVE_FROM_SOURCE", "false").lower() == "true"

# Configuration for labels loaded from environment variables
allowed_labels_str = os.environ.get("ALLOWED_LABELS", "Allowed")
ALLOWED_LABELS = [
    label.strip() for label in allowed_labels_str.split(",") if label.strip()
]

blocked_labels_str = os.environ.get("BLOCKED_LABELS", "")
BLOCKED_LABELS = [
    label.strip() for label in blocked_labels_str.split(",") if label.strip()
]

s3_client = boto3.client("s3")
sns_client = boto3.client("sns")


def send_security_alert(file_key, bucket_name, detected_labels, category):
    """Send a security alert when sensitive content is detected."""
    if not alert_topic_arn:
        logger.warning("Alert SNS topic not configured, skipping alert")
        return

    try:
        # Create a detailed alert message
        subject = f"🚨 Sensitive Content Detected - File Quarantined"

        label_names = [
            label.name for label in detected_labels if label.name in BLOCKED_LABELS
        ]

        message = f"""
SECURITY ALERT: Sensitive content detected and quarantined

File Details:
- File: {file_key}
- Source Bucket: {bucket_name}
- Quarantine Bucket: {quarantine_bucket}
- Category: {category}
- Detected Sensitive Labels: {', '.join(label_names)}

Timestamp: {boto3.Session().region_name}
This file has been automatically moved to quarantine for review.
        """.strip()

        # Send the alert
        response = sns_client.publish(
            TopicArn=alert_topic_arn, Subject=subject, Message=message
        )

        logger.info(
            f"Security alert sent for {file_key}. Message ID: {response['MessageId']}"
        )

    except Exception as e:
        logger.error(f"Failed to send security alert for {file_key}: {str(e)}")


def lambda_handler(event, context):
    if not dxr_api_key or not dxr_api_url or not dxr_datasource_id:
        logger.error("Data X-Ray environment variables not set.")
        return {
            "statusCode": 500,
            "body": "Data X-Ray environment variables not set.",
        }

    datasource_ids: List[int] = [int(id.strip()) for id in dxr_datasource_id.split(",")]
    local_file_paths = []
    decoded_file_keys = []
    documents_bucket = event["Records"][0]["s3"]["bucket"]["name"]

    for record in event["Records"]:
        file_key = record["s3"]["object"]["key"]
        decoded_file_key = urllib.parse.unquote_plus(file_key)
        local_file_path = f"/tmp/{decoded_file_key.split('/')[-1]}"
        decoded_file_keys.append(decoded_file_key)
        local_file_paths.append(local_file_path)

        # Log the bucket name and file key
        logger.info(
            f"Downloading file from bucket: {documents_bucket}, key: {decoded_file_key}"
        )

        # Download the file from S3
        try:
            s3_client.download_file(documents_bucket, decoded_file_key, local_file_path)
        except s3_client.exceptions.NoSuchKey:
            logger.error(
                f"File not found: {decoded_file_key} in bucket: {documents_bucket}"
            )
            raise
        except Exception as e:
            logger.error(f"Error downloading file: {e}")
            raise

    # Initialize the on-demand classifier
    dxr = DXRClient(
        api_url=dxr_api_url,
        api_key=dxr_api_key,
        ignore_ssl=True,
    )

    # Create a classification job with all files
    results = dxr.on_demand_classifier.run_job(
        [File(path) for path in local_file_paths], datasource_ids
    )

    if len(results) == 0:
        logger.warning("Metadata not found for any files. Exiting...")
        return {"statusCode": 200, "body": "No metadata found"}

    for i, result in enumerate(results):
        decoded_file_key = decoded_file_keys[i]

        # Check if file should go to allowed bucket based on labels
        is_allowed = (
            any(label.name in ALLOWED_LABELS for label in result.labels)
            if ALLOWED_LABELS
            else False
        )

        # Check if file should be quarantined based on blocked labels
        is_blocked = (
            any(label.name in BLOCKED_LABELS for label in result.labels)
            if BLOCKED_LABELS
            else False
        )

        # Update S3 object metadata
        new_metadata = {
            "category": result.category,
        }

        # Update metadata regardless of where the file will go
        s3_client.copy_object(
            Bucket=documents_bucket,
            Key=decoded_file_key,
            CopySource={"Bucket": documents_bucket, "Key": decoded_file_key},
            Metadata=new_metadata,
            MetadataDirective="REPLACE",
        )
        logger.info(f"Updated metadata for {decoded_file_key}")

        # Determine target bucket
        target_bucket = None
        if is_blocked:
            target_bucket = quarantine_bucket
            logger.info(
                f"File {decoded_file_key} has blocked labels and will be quarantined."
            )
            # Send security alert for quarantined files
            send_security_alert(
                decoded_file_key, documents_bucket, result.labels, result.category
            )
        else:
            target_bucket = allowed_bucket
            logger.info(
                f"File {decoded_file_key} does not have blocked labels and will be moved to allowed bucket."
            )

        # Move the file to the appropriate bucket
        s3_client.copy_object(
            Bucket=target_bucket,
            Key=decoded_file_key,
            CopySource={"Bucket": documents_bucket, "Key": decoded_file_key},
        )

        # Delete from source bucket if remove_from_source is enabled
        if remove_from_source:
            s3_client.delete_object(Bucket=documents_bucket, Key=decoded_file_key)
            logger.info(f"Moved {decoded_file_key} to {target_bucket}")
        else:
            logger.info(f"Copied {decoded_file_key} to {target_bucket}")

    return {"statusCode": 200, "body": "Processing complete"}
