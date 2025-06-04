import boto3
import requests

APP_URL = ""  # Replace with your app URL


def lambda_handler(event, context):
    s3 = boto3.client("s3")
    url = f"{APP_URL}/export"
    bucket = "my-export-data-bucket"
    key = "exports/data.jsonl"

    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        s3.upload_fileobj(r.raw, bucket, key)

    return {"statusCode": 200, "body": f"Exported to s3://{bucket}/{key}"}
