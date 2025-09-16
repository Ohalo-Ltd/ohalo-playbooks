#!/bin/bash
# create-lambda-package.sh

set -e

echo "Creating Lambda deployment package with dependencies..."

# Clean up any previous builds
rm -rf lambda-build
rm -f lambda_function.zip

# Create build directory
mkdir lambda-build

# Copy Lambda source code
cp lambda/*.py lambda-build/

# Install dependencies in the build directory
pip install requests -t lambda-build/

# Create deployment package
cd lambda-build
zip -r ../lambda_function.zip .
cd ..

# Clean up build directory
rm -rf lambda-build

echo "Lambda deployment package created: lambda_function.zip"
echo "Now run: terraform apply"