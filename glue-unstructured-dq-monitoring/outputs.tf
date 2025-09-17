output "s3_bucket_name" {
  description = "Name of the S3 bucket storing files data"
  value       = aws_s3_bucket.files_data.id
}

output "glue_database_name" {
  description = "Name of the Glue database"
  value       = aws_glue_catalog_database.files_db.name
}

output "lambda_function_name" {
  description = "Name of the Lambda function"
  value       = aws_lambda_function.fetch_files_data.function_name
}

output "glue_job_name" {
  description = "Name of the Glue Data Quality job"
  value       = aws_glue_job.data_quality_check.name
}