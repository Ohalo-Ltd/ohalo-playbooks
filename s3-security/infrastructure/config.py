"""
Configuration settings for the Pulumi deployment.
"""

import os
import pulumi
from dotenv import load_dotenv

# Load environment variables from .env file if it exists
# This way we can use environment variables directly without setting Pulumi config
env_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
if os.path.exists(env_file):
    load_dotenv(env_file)

config = pulumi.Config()

# Project configuration
project_name = config.get("projectName") or "odc-aws"
environment = config.get("environment") or "dev"

# S3 Bucket Configuration
bucket_versioning = config.get_bool("bucketVersioning") or True
notification_prefix = config.get("notificationPrefix") or ""
notification_suffix = config.get("notificationSuffix") or ""

# Lambda Configuration
lambda_runtime = config.get("lambdaRuntime") or "python3.10"
lambda_memory_size = int(config.get("lambdaMemorySize") or 128)
lambda_timeout = int(config.get("lambdaTimeout") or 30)

# Data X-Ray Configuration - prioritize environment variables over Pulumi config
dxr_api_url = os.environ.get("DXR_API_URL") or config.get_secret("dxrApiUrl") or ""
dxr_api_key = os.environ.get("DXR_API_KEY") or config.get_secret("dxrApiKey") or ""
dxr_datasource_id = os.environ.get("DXR_DATASOURCE_ID") or config.get("dxrDatasourceId") or ""

# Label Configuration - prioritize environment variables over Pulumi config
blocked_labels = os.environ.get("BLOCKED_LABELS") or config.get("blockedLabels") or ""
remove_from_source_str = os.environ.get("REMOVE_FROM_SOURCE")
remove_from_source = (remove_from_source_str.lower() == "true" if remove_from_source_str else 
                     config.get_bool("removeFromSource") or True)

# Tags
common_tags = {
    "Project": project_name,
    "Environment": environment,
    "ManagedBy": "Pulumi"
}
