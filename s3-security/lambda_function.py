import logging
import os
import urllib.parse
from typing import List

import boto3
import botocore.exceptions
import dotenv
from dxrpy import DXRClient
from dxrpy.utils import File

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dotenv.load_dotenv()

dxr_api_key = os.environ["DXR_API_KEY"]
dxr_api_url = os.environ["DXR_API_URL"]
dxr_datasource_id = os.environ.get("DXR_DATASOURCE_ID", "")

allowed_bucket = os.environ["ALLOWED_BUCKET"]
quarantine_bucket = os.environ["QUARANTINE_BUCKET"]

# Whether to delete files from source bucket after processing
remove_from_source = os.environ.get("REMOVE_FROM_SOURCE", "false").lower() in (
    "true",
    "1",
    "yes",
    "on",
)

# Configuration for labels loaded from environment variables
allowed_labels_str = os.environ.get("ALLOWED_LABELS", "Allowed")
ALLOWED_LABELS = {
    label.strip() for label in allowed_labels_str.split(",") if len(label.strip()) > 0
}

blocked_labels_str = os.environ.get("BLOCKED_LABELS", "")
BLOCKED_LABELS = {
    label.strip() for label in blocked_labels_str.split(",") if len(label.strip()) > 0
}

s3_client = boto3.client("s3")


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
    intake_bucket = event["Records"][0]["s3"]["bucket"]["name"]

    for record in event["Records"]:
        file_key = record["s3"]["object"]["key"]
        decoded_file_key = urllib.parse.unquote_plus(file_key)
        local_file_path = f"/tmp/{decoded_file_key.split('/')[-1]}"
        decoded_file_keys.append(decoded_file_key)
        local_file_paths.append(local_file_path)

        # Log the bucket name and file key
        logger.info(
            f"Downloading file from bucket: {intake_bucket}, key: {decoded_file_key}"
        )

        # Download the file from S3
        try:
            s3_client.download_file(intake_bucket, decoded_file_key, local_file_path)
        except botocore.exceptions.ClientError as e:
            error_code = e.response.get("Error", {}).get("Code")
            if error_code == "NoSuchKey":
                logger.error(
                    f"File not found: {decoded_file_key} in bucket: {intake_bucket}"
                )
                raise
            else:
                logger.error(f"Error downloading file: {e}")
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
            Bucket=intake_bucket,
            Key=decoded_file_key,
            CopySource={"Bucket": intake_bucket, "Key": decoded_file_key},
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
        elif is_allowed:
            target_bucket = allowed_bucket
            logger.info(
                f"File {decoded_file_key} has allowed labels and will be moved to allowed bucket."
            )
        else:
            # No matching rules - file stays in source bucket
            logger.info(f"File {decoded_file_key} will remain in the intake bucket.")
            continue

        # Move the file to the appropriate bucket
        s3_client.copy_object(
            Bucket=target_bucket,
            Key=decoded_file_key,
            CopySource={"Bucket": intake_bucket, "Key": decoded_file_key},
        )

        # Delete from source bucket if remove_from_source is enabled
        if remove_from_source:
            s3_client.delete_object(Bucket=intake_bucket, Key=decoded_file_key)
            logger.info(f"Moved {decoded_file_key} to {target_bucket}")
        else:
            logger.info(f"Copied {decoded_file_key} to {target_bucket}")

    return {"statusCode": 200, "body": "Processing complete"}
