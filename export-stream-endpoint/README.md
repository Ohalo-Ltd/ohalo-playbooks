# FastAPI JSONL Dummy Exporter with AWS Lambda + Terraform

This repository demonstrates how to:

1. Build a **minimal dummy FastAPI app** that streams JSONL.
2. Expose the app via **ngrok**.
3. Connect it to an **AWS Lambda function** using **Terraform** to fetch and upload the data to **S3**.

---

## Requirements

- Python 3.12+
- [ngrok](https://ngrok.com/download)
- AWS CLI
- Terraform

## Step 1: Run the FastAPI App Locally

### Create virtual environment
```
python -m venv .env
source .env/bin/activate
pip install -r requirements.txt
```

### Run the dummy export application locally
```
uvicorn main:app --reload
```

## Step 2: Expose the running application with ngrok

The following command should give you a URL like `https://f156-78-83-61-108.ngrok-free.app`.
```
ngrok http http://localhost:8000
```

## Step 3: Modify the AWS Lambda function

### Modify the code to point to the new app URL

Go into `terraform/lambda/lambda_function.py` and modify the code to point to the `ngrok` url: 
```
APP_URL = "{NGROK_URL}"
```

### Build the Lambda `.zip` archive
The following command should generate a `lambda_function.zip` in the `terraform/lambda` folder:
```
cd terraform/lambda
./build.sh
```

Run the following if you get permission issues:
```
chmod +x build.sh
```

## Step 4: Deploy infrastructure with Terraform
Terraform would provision:
- S3 bucket
- Necessary IAM Roles
- Lambda function
- Glue crawler
- Glue Data Catalog

### Login into AWS
There are multiple options here. For example, you can use `awscli` which would prompt for access keys and setup info:
```
aws configure
```

### Deploy with Terraform
```
cd terraform
terraform init
terraform apply
```

### Run the Lambda function

### Run the Glue crawler

### Query the data using Amazon Athena

### Cleanup resources
```
terraform destroy
```