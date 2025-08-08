#!/bin/zsh
set -e

# Clean previous build
rm -rf lambda_build lambda_package.zip
mkdir lambda_build

# Copy lambda function
cp lambda_function.py lambda_build/

# Install dependencies to build folder using uv
uv pip install --target lambda_build dxrpy boto3 python-dotenv

# Zip up the build folder
cd lambda_build
zip -r ../lambda_package.zip .
cd ..

echo "Lambda package built: lambda_package.zip"
