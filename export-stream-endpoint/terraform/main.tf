provider "aws" {
  region = "eu-north-1"
}

# S3 bucket
resource "aws_s3_bucket" "export_data" {
  bucket = "my-export-data-bucket"
  force_destroy = true
}

# IAM role for Lambda
resource "aws_iam_role" "lambda_exec" {
  name = "LambdaExportExecutionRole"
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect = "Allow",
      Principal = { Service = "lambda.amazonaws.com" },
      Action = "sts:AssumeRole"
    }]
  })
}

# IAM policy for Lambda to write to S3
resource "aws_iam_policy" "lambda_s3_limited" {
  name        = "LambdaS3LimitedAccess"
  description = "Allow Lambda to List, Get, and Put objects only in my-export-data-bucket"
  policy      = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "s3:ListBucket"
        ],
        Resource = "arn:aws:s3:::my-export-data-bucket"
      },
      {
        Effect = "Allow",
        Action = [
          "s3:GetObject",
          "s3:PutObject"
        ],
        Resource = "arn:aws:s3:::my-export-data-bucket/*"
      }
    ]
  })
}

resource "aws_iam_policy_attachment" "lambda_s3_limited_attach" {
  name       = "lambda-s3-limited-policy-attach"
  roles      = [aws_iam_role.lambda_exec.name]
  policy_arn = aws_iam_policy.lambda_s3_limited.arn
}

resource "aws_iam_role_policy_attachment" "lambda_logs" {
  role       = aws_iam_role.lambda_exec.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# Lambda Function
resource "aws_lambda_function" "export_lambda" {
  function_name = "export-jsonl-lambda"
  role          = aws_iam_role.lambda_exec.arn
  handler       = "lambda_function.lambda_handler"
  runtime       = "python3.11"
  timeout       = 60

  filename         = "${path.module}/lambda/lambda_function.zip"
  source_code_hash = filebase64sha256("${path.module}/lambda/lambda_function.zip")
}


# Glue role
resource "aws_iam_role" "glue_exec" {
  name = "GlueCrawlerS3Role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect = "Allow",
      Principal = { Service = "glue.amazonaws.com" },
      Action = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_s3" {
  role       = aws_iam_role.glue_exec.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_exec.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# Glue Database
resource "aws_glue_catalog_database" "export_db" {
  name = "my_export_db"
}

# Glue Crawler
resource "aws_glue_crawler" "export_crawler" {
  name         = "export-jsonl-crawler"
  role         = aws_iam_role.glue_exec.arn
  database_name = aws_glue_catalog_database.export_db.name
  table_prefix = "exported_"

  s3_target {
    path = "s3://${aws_s3_bucket.export_data.bucket}/exports/"
  }

  # schedule = "cron(0 12 * * ? *)" # optional: runs every day at noon
}
