import boto3
import requests


def lambda_handler(event, context):
    app_url = event.get("app_url")
    if not app_url:
        return {"statusCode": 400, "body": "Missing 'app_url' in event payload"}

    pit_token = event.get("pit_token")
    if not pit_token:
        return {"statusCode": 400, "body": "Missing 'pit_token' in event payload"}

    s3 = boto3.client("s3")

    headers = {"Authorization": f"Bearer {pit_token}"}
    export_url = f"{app_url}/api/indexed-files"
    bucket = "my-export-data-bucket"
    key = "exports/data.jsonl"

    with requests.get(export_url, headers=headers, verify=False, stream=True) as r:
        r.raise_for_status()
        s3.upload_fileobj(r.raw, bucket, key)

    return {"statusCode": 200, "body": f"Exported to s3://{bucket}/{key}"}
