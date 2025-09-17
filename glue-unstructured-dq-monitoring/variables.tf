variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Project name for resource naming"
  type        = string
  default     = "files-dq-check"
}

variable "api_endpoint" {
  description = "API endpoint to fetch files data"
  type        = string
}

variable "pat_token" {
  description = "PAT token for API authentication"
  type        = string
  default     = ""
  sensitive   = true
}