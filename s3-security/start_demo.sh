#!/bin/zsh
set -e

# Ensure uv is installed
if ! command -v uv &> /dev/null; then
  echo "uv not found. Please install uv (https://github.com/astral-sh/uv) before running this script."
  exit 1
fi

# Ensure Pulumi is installed
if ! command -v pulumi &> /dev/null; then
  echo "Pulumi not found. Please install Pulumi (https://www.pulumi.com/docs/get-started/install/) before running this script."
  exit 1
fi

# Load environment variables (including optional PULUMI_CONFIG_PASSPHRASE)
if [ -f .env ]; then
  set -a
  source .env
  set +a
fi

# Create/refresh project virtualenv and install deps from pyproject.toml
uv sync

# Make Pulumi use the project venv Python
export PULUMI_PYTHON_CMD="$(pwd)/.venv/bin/python"

# Build Lambda package with dependencies
./build_lambda.sh

# Select or init stack
pulumi stack select dev || pulumi stack init dev

# Deploy the stack
pulumi up --yes

echo "Demo stack deployed!"
