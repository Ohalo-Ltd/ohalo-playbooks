#!/usr/bin/env bash
set -euo pipefail

# One-cycle DXR → Purview sync helper
# - Loads local .env
# - Supports a --fast mode to limit scope and shorten timeouts
# - Honors any env overrides you export before calling

HERE_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$HERE_DIR"

if [[ -f .env ]]; then
  echo "Loading env from $HERE_DIR/.env"
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

FAST=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --fast)
      FAST=1
      shift
      ;;
    *)
      echo "Unknown option: $1" >&2
      echo "Usage: $0 [--fast]" >&2
      exit 2
      ;;
  esac
done

if [[ $FAST -eq 1 ]]; then
  export DXR_TAGS_LIMIT=${DXR_TAGS_LIMIT:-10}
  export DXR_DATASOURCES_LIMIT=${DXR_DATASOURCES_LIMIT:-10}
  export HTTP_TIMEOUT_SECONDS=${HTTP_TIMEOUT_SECONDS:-20}
  export ENABLE_LABEL_STATS=${ENABLE_LABEL_STATS:-1}
  echo "FAST mode: DXR_TAGS_LIMIT=$DXR_TAGS_LIMIT DXR_DATASOURCES_LIMIT=$DXR_DATASOURCES_LIMIT HTTP_TIMEOUT_SECONDS=$HTTP_TIMEOUT_SECONDS"
fi

export RUN_ONCE=1
python purview_dxr_integration.py

