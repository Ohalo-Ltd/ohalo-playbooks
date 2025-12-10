#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
TEMPLATE_PATH="${SCRIPT_DIR}/dxr-single-server-cloudformation.yaml"
ANSIBLE_ROOT="${SCRIPT_DIR}/../dxr/ohalo-ansible"
INVENTORY_PATH=""
ARTIFACT_BUCKET=""
ARTIFACT_PREFIX="dxr-artifacts"
STACK_NAME=""
KEY_PAIR_NAME=""
SSH_LOCATION="0.0.0.0/0"
AWS_REGION=""
SKIP_DEPLOY="false"
AMI_ID=""

usage() {
  cat <<'USAGE'
Package the local ohalo-ansible tree, upload it to S3, and deploy the DXR stack.

Required flags:
  --artifact-bucket <bucket>   S3 bucket for the packaged assets
  --inventory <path>           Inventory JSON to upload (will be patched for IPs)
  --key-pair <name>            Existing EC2 key pair name
  --stack-name <name>          CloudFormation stack name
  --ami-id <ami>               RHEL 8.10 AMI ID for the target region

Optional flags:
  --ansible-dir <path>         Override the default ../dxr/ohalo-ansible directory
  --artifact-prefix <prefix>   Prefix (folder) inside the S3 bucket (default: dxr-artifacts)
  --ssh-location <cidr>        CIDR block allowed to SSH (default: 0.0.0.0/0)
  --region <aws-region>        AWS region if not set in your CLI config
  --skip-deploy                Only upload artifacts; skip stack deployment
  -h, --help                   Show this help message
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --artifact-bucket)
      ARTIFACT_BUCKET="$2"
      shift 2
      ;;
    --artifact-prefix)
      ARTIFACT_PREFIX="$2"
      shift 2
      ;;
    --inventory)
      INVENTORY_PATH="$2"
      shift 2
      ;;
    --ansible-dir)
      ANSIBLE_ROOT="$2"
      shift 2
      ;;
    --stack-name)
      STACK_NAME="$2"
      shift 2
      ;;
    --key-pair)
      KEY_PAIR_NAME="$2"
      shift 2
      ;;
    --ssh-location)
      SSH_LOCATION="$2"
      shift 2
      ;;
    --ami-id)
      AMI_ID="$2"
      shift 2
      ;;
    --region)
      AWS_REGION="$2"
      shift 2
      ;;
    --skip-deploy)
      SKIP_DEPLOY="true"
      shift 1
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[ERROR] Unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ -z "$ARTIFACT_BUCKET" || -z "$INVENTORY_PATH" || -z "$STACK_NAME" || -z "$KEY_PAIR_NAME" || -z "$AMI_ID" ]]; then
  echo "[ERROR] Missing required arguments" >&2
  usage
  exit 1
fi

if [[ ! -f "$TEMPLATE_PATH" ]]; then
  echo "[ERROR] CloudFormation template not found at $TEMPLATE_PATH" >&2
  exit 1
fi

if [[ ! -d "$ANSIBLE_ROOT" ]]; then
  echo "[ERROR] Unable to locate Ansible directory: $ANSIBLE_ROOT" >&2
  exit 1
fi

if [[ ! -f "$INVENTORY_PATH" ]]; then
  echo "[ERROR] Inventory file not found: $INVENTORY_PATH" >&2
  exit 1
fi

if ! command -v aws >/dev/null 2>&1; then
  echo "[ERROR] AWS CLI is required" >&2
  exit 1
fi

TIMESTAMP=$(date +%Y%m%d%H%M%S)
ARTIFACT_NAME="dxr-ansible-${TIMESTAMP}.tar.gz"
ARTIFACT_PATH="${SCRIPT_DIR}/${ARTIFACT_NAME}"

pushd "$ANSIBLE_ROOT" >/dev/null
  tar -czf "$ARTIFACT_PATH" .
popd >/dev/null

echo "[INFO] Created artifact: $ARTIFACT_PATH"

PREFIX_CLEAN="${ARTIFACT_PREFIX#/}"
PREFIX_CLEAN="${PREFIX_CLEAN%/}"
if [[ -n "$PREFIX_CLEAN" ]]; then
  ANSIBLE_KEY="$PREFIX_CLEAN/$ARTIFACT_NAME"
  INVENTORY_KEY="$PREFIX_CLEAN/$(basename "$INVENTORY_PATH")"
else
  ANSIBLE_KEY="$ARTIFACT_NAME"
  INVENTORY_KEY="$(basename "$INVENTORY_PATH")"
fi

AWS_CMD=(aws)
if [[ -n "$AWS_REGION" ]]; then
  AWS_CMD+=("--region" "$AWS_REGION")
fi

"${AWS_CMD[@]}" s3 cp "$ARTIFACT_PATH" "s3://${ARTIFACT_BUCKET}/${ANSIBLE_KEY}"
"${AWS_CMD[@]}" s3 cp "$INVENTORY_PATH" "s3://${ARTIFACT_BUCKET}/${INVENTORY_KEY}"

echo "[INFO] Uploaded artifacts to s3://${ARTIFACT_BUCKET}/${PREFIX_CLEAN}"

if [[ "$SKIP_DEPLOY" == "true" ]]; then
  echo "[INFO] --skip-deploy set; exiting after uploads"
  exit 0
fi

PARAM_OVERRIDES=(
  "KeyPairName=${KEY_PAIR_NAME}"
  "SSHLocation=${SSH_LOCATION}"
  "AnsibleArtifactBucket=${ARTIFACT_BUCKET}"
  "AnsibleArtifactKey=${ANSIBLE_KEY}"
  "InventoryBucket=${ARTIFACT_BUCKET}"
  "InventoryKey=${INVENTORY_KEY}"
  "LatestAmiId=${AMI_ID}"
)

"${AWS_CMD[@]}" cloudformation deploy \
  --template-file "$TEMPLATE_PATH" \
  --stack-name "$STACK_NAME" \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides "${PARAM_OVERRIDES[@]}"

echo "[INFO] Stack deployment initiated: $STACK_NAME"
