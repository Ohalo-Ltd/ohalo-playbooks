# S3 ETL Quarantine Automation (Pulumi + Data X-Ray)

This playbook operationalizes continuous monitoring for unstructured ETL zones. It pairs Data X-Ray file inspection with Pulumi-managed AWS infrastructure to quarantine risky payloads before they land in downstream analytics pipelines.

## What Does This Do?
- **Watches staging buckets feeding ETL jobs** for newly arrived objects.
- **Classifies files** using the Data X-Ray API to surface sensitivity and policy violations.
- **Automatically quarantines, reroutes, or releases files** based on configurable rules so downstream jobs only ingest clean assets.
- **Manages infrastructure as code** with Pulumi in Python for reproducible environments.
- **Packages lambdas with uv** so dependency management stays deterministic.

## How It Works
1. **Upload a file** to the source S3 bucket.
2. **Lambda is triggered** automatically.
3. **Lambda downloads the file**, calls Data X-Ray for classification, and updates S3 object metadata.
4. **File is moved** to the allowed or quarantine bucket, or left in place if no rule matches.

## Project Structure
- `lambda_function.py`: Lambda handler with all business logic.
- `main.py`: Pulumi stack definition (S3 buckets, Lambda, permissions, notifications).
- `build_lambda.sh`: Builds the Lambda deployment package with all dependencies.
- `start_demo.sh`: One-step script to set up everything (dependencies, Lambda package, Pulumi stack).
- `pyproject.toml`: Python dependencies for uv.
- `.env.example`: Example environment variables for DXR and bucket config.
- `.gitignore`: Ignores build artifacts and secrets.
- `Pulumi.yaml`, `Pulumi.dev.yaml`: Pulumi project and stack config.

## Getting Started
1. **Install prerequisites:**
   - Python 3.11+
   - [uv](https://github.com/astral-sh/uv)
   - AWS credentials (e.g., via `aws configure`)
2. **Configure environment:**
   - Copy `.env.example` to `.env` and fill in your DXR and bucket details.
   - (Optional) Set `PULUMI_CONFIG_PASSPHRASE` in `.env` to avoid prompts.
3. **Deploy the stack:**
   - Run `./start_demo.sh` to install dependencies, build the Lambda package, and deploy everything with Pulumi.

## Usage
- **Upload files** to the source bucket (see Pulumi output for bucket name).
- **Files are classified** and routed automatically.
- **Check the allowed/quarantine buckets** for results.

## Clean Up
To destroy all resources:
```zsh
pulumi destroy --yes
```

## Advanced
- All Lambda dependencies are packaged using uv and `build_lambda.sh`.
- Pulumi uses the uv-managed virtualenv for stack execution.
- S3 triggers are set up using bucket notifications and Lambda permissions.

## Support
If you have issues, check:
- Your Python and uv setup
- AWS credentials
- Environment variables in `.env`
- Pulumi documentation: https://www.pulumi.com/docs/
