output "s3_bucket_name" {
  value = aws_s3_bucket.export_data.bucket
}

output "lambda_function_name" {
  value = aws_lambda_function.export_lambda.function_name
}
