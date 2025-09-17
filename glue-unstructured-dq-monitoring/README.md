# Glue Unstructured Data Quality Monitoring

This playbook proves out how Data X-Ray labels and metadata can drive unstructured data quality alerts inside the AWS Glue ecosystem. It provisions a lightweight Lambda ingestion flow, stages results in S3, and runs an AWS Glue Data Quality ruleset to flag high-risk records before they feed downstream ETL jobs.

## What this deploys
- **AWS Lambda** helper that exports Data X-Ray findings into parquet/JSON blobs in S3.
- **Glue Crawler** that maintains tables describing the latest scan results.
- **Glue Data Quality job** that evaluates labels and metadata against configurable rules.
- **Terraform stack** that ties together IAM roles, buckets, and job orchestration.

## Why teams use it
- Catch sensitive or policy-violating files before they move deeper into unstructured data lakes.
- Translate Data X-Ray classifications into Glue DQ scorecards stakeholders already understand.
- Automate alerting and reprocessing loops as new files arrive, without hand stitching AWS resources.

## Getting started
1. Run `./create-lambda-package.sh` to bundle the Lambda dependencies.
2. Deploy the Terraform stack as usual (`terraform init && terraform apply`).
3. Kick off the Lambda to refresh S3 snapshots:
   ```bash
   aws lambda invoke --function-name files-dq-check-fetch-files-data --payload '{}' response.json --region us-east-1
   ```
4. Refresh the Glue tables so Data Quality has the latest partitions:
   ```bash
   aws glue start-crawler --name files-dq-check-files-crawler --region us-east-1
   ```
5. Run the Glue DQ job to score unstructured files and enforce the bundled ruleset:
   ```bash
   aws glue start-job-run --job-name $(terraform output -raw glue_job_name) --region us-east-1
   ```

## Default ruleset
The sample Glue DQ rules identify files that contain highly sensitive labels but are missing required gating labels. Use this as a starting point and adapt the logic to your own label taxonomy.

```text
Rules = [
    ColumnExists "labels",
    CustomSql "SELECT COUNT(*) FROM primary WHERE array_contains(transform(labels, x -> x.name), 'CONFIDENTIAL') AND NOT array_contains(transform(labels, x -> x.name), 'GATED')" = 0
]
```

## Next steps
- Extend the Lambda export to include custom metadata attributes or business unit tags.
- Push Glue DQ results into Amazon EventBridge for automated Slack or ticketing alerts.
- Schedule the job with Glue triggers so scorecards stay up to date without manual intervention.
