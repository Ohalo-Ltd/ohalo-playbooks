# ODC AWS Infrastructure with Pulumi

This project contains the Pulumi infrastructure code for deploying the Open Data Cube (ODC) S3 and Lambda components on AWS.

## Prerequisites

1. [Pulumi CLI](https://www.pulumi.com/docs/get-started/install/)
2. Python 3.11 or later
3. AWS CLI configured with appropriate credentials
4. Virtualenv (recommended)

## Setup

1. Clone this repository
2. Create a virtual environment:
   ```
   python -m venv .venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```
3. Install dependencies:
   ```
   pip install -r requirements.txt
   pip install -r requirements-dev.txt  # If you plan to run tests
   ```

## Environment Variables

The Lambda function requires several environment variables. Create a `.env` file based on the `.env.example` file:

```
cp .env.example .env
```

Then edit the `.env` file and fill in the required values.

### How Environment Variables are Used

The configuration system will:
1. Load environment variables from your `.env` file automatically
2. Use these values directly in the Lambda function environment
3. Fall back to values in Pulumi config only if environment variables are not set

This approach keeps sensitive information out of your codebase while making local development simple.

## Lambda Code

Place your Lambda function code in the `lambda` directory at the root of this project. At minimum, you should have:

```
lambda/
  └── index.py  # Contains the handler function
```

## Deploy

Deploy the infrastructure:

```
pulumi up
```

This will show you a preview of the resources that will be created. Type `yes` to proceed with the deployment.

## Access Outputs

Once deployment is complete, you can access important outputs:

```
pulumi stack output bucket_name
pulumi stack output lambda_function_name
pulumi stack output lambda_function_arn
```

## Destroy

To destroy the infrastructure:

```
pulumi destroy
```

## Project Structure

- `infrastructure/__main__.py`: Entry point for the Pulumi program
- `infrastructure/stacks/`: Contains the infrastructure stack modules
  - `odc_aws_stack.py`: S3 bucket, Lambda, and notification configuration
- `infrastructure/config.py`: Configuration variables
- `Pulumi.yaml`: Pulumi project file
- `Pulumi.dev.yaml`: Pulumi stack configuration for the dev environment
- `lambda/`: Directory containing Lambda function code
