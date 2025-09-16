terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# S3 Bucket for storing files data
resource "aws_s3_bucket" "files_data" {
  bucket = "${var.project_name}-files-data-${random_string.suffix.result}"
  force_destroy = true
}

resource "random_string" "suffix" {
  length  = 8
  special = false
  upper   = false
}

resource "aws_s3_bucket_versioning" "files_data" {
  bucket = aws_s3_bucket.files_data.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "files_data" {
  bucket = aws_s3_bucket.files_data.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# IAM Role for Lambda
resource "aws_iam_role" "lambda_role" {
  name = "${var.project_name}-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy" "lambda_policy" {
  name = "${var.project_name}-lambda-policy"
  role = aws_iam_role.lambda_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:*:*:*"
      },
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.files_data.arn,
          "${aws_s3_bucket.files_data.arn}/*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_basic" {
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
  role       = aws_iam_role.lambda_role.name
}

# Lambda function to fetch data
resource "aws_lambda_function" "fetch_files_data" {
  filename      = "lambda_function.zip"
  function_name = "${var.project_name}-fetch-files-data"
  role          = aws_iam_role.lambda_role.arn
  handler       = "fetch_files_data.lambda_handler"
  runtime       = "python3.9"
  timeout       = 300

  environment {
    variables = {
      API_ENDPOINT = var.api_endpoint
      S3_BUCKET    = aws_s3_bucket.files_data.id
      PAT_TOKEN    = var.pat_token
    }
  }

  depends_on = [data.archive_file.lambda_zip]
}

data "archive_file" "lambda_zip" {
  type        = "zip"
  source_dir  = "lambda"
  output_path = "lambda_function.zip"
}

# Glue Database
resource "aws_glue_catalog_database" "files_db" {
  name = "${var.project_name}_files_db"
}

# IAM Role for Glue
resource "aws_iam_role" "glue_role" {
  name = "${var.project_name}-glue-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service_role" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_s3_policy" {
  name = "${var.project_name}-glue-s3-policy"
  role = aws_iam_role.glue_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetBucketLocation",
          "s3:GetObject",
          "s3:ListBucket",
          "s3:PutObject",
          "s3:DeleteObject"
        ]
        Resource = [
          aws_s3_bucket.files_data.arn,
          "${aws_s3_bucket.files_data.arn}/*",
          aws_s3_bucket.glue_scripts.arn,
          "${aws_s3_bucket.glue_scripts.arn}/*"
        ]
      }
    ]
  })
}

# Glue Crawler (manual only - no schedule)
resource "aws_glue_crawler" "files_crawler" {
  database_name = aws_glue_catalog_database.files_db.name
  name          = "${var.project_name}-files-crawler"
  role          = aws_iam_role.glue_role.arn

  s3_target {
    path = "s3://${aws_s3_bucket.files_data.bucket}/"
  }

  # No schedule - manual only
}

# S3 bucket for Glue scripts
resource "aws_s3_bucket" "glue_scripts" {
  bucket = "${var.project_name}-glue-scripts-${random_string.suffix.result}"
}

resource "aws_s3_object" "dq_script" {
  bucket = aws_s3_bucket.glue_scripts.id
  key    = "scripts/data_quality_check.py"
  source = "glue_scripts/data_quality_check.py"
  etag   = filemd5("glue_scripts/data_quality_check.py")
}

# Glue Data Quality Job (manual only)
resource "aws_glue_job" "data_quality_check" {
  name     = "${var.project_name}-dq-check"
  role_arn = aws_iam_role.glue_role.arn

  command {
    script_location = "s3://${aws_s3_bucket.glue_scripts.bucket}/scripts/data_quality_check.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-bookmark-option"               = "job-bookmark-enable"
    "--enable-continuous-cloudwatch-log" = "true"
    "--DATABASE_NAME"                     = aws_glue_catalog_database.files_db.name
    "--TABLE_NAME"                        = "raw_data"
    "--S3_BUCKET"                        = aws_s3_bucket.files_data.id
  }

  execution_property {
    max_concurrent_runs = 1
  }

  max_retries = 1
  timeout     = 60
}

data "aws_caller_identity" "current" {}