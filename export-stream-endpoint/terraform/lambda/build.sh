#!/bin/bash

set -e

cd "$(dirname "$0")"

rm -rf package
mkdir -p package

pip install -r requirements.txt --target package

cp lambda_function.py package/

cd package
zip -r ../lambda_function.zip .
