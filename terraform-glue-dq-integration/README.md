### Build Lambda .zip file
./create-lambda-package.sh

### Invoke Lambda to fetch data from server and store it in S3
aws lambda invoke --function-name files-dq-check-fetch-files-data --payload '{}' response.json --region us-east-1

### Start Glue Crawler to fetch data from S3
aws glue start-crawler --name files-dq-check-files-crawler --region us-east-1

### Run a custom job to perform data quality check
aws glue start-job-run --job-name $(terraform output -raw glue_job_name) --region us-east-1

### Ruleset
Rules = [
    ColumnExists "labels",
    CustomSql "SELECT COUNT(*) FROM primary WHERE array_contains(transform(labels, x -> x.name), 'CONFIDENTIAL') AND NOT array_contains(transform(labels, x -> x.name), 'GATED')" = 0
]