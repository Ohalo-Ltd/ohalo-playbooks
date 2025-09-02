#!/usr/bin/env bash
set -euo pipefail

# One-cycle DXR → Purview smoke sync helper
# - Loads local .env from this folder
# - Smoke by default: limits scope/timeouts for a quick run
# - Use --full to disable smoke limits
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

SMOKE=1
FULL=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --smoke)
      SMOKE=1
      shift
      ;;
    --full)
      SMOKE=0
      FULL=1
      shift
      ;;
    *)
      echo "Unknown option: $1" >&2
      echo "Usage: $0 [--smoke|--full]" >&2
      exit 2
      ;;
  esac
done

# Smoke defaults (can be overridden via env). Applies unless --full.
if [[ $SMOKE -eq 1 && $FULL -eq 0 ]]; then
  export DXR_TAGS_LIMIT=${DXR_TAGS_LIMIT:-5}       # limit number of classifications fetched
  export HTTP_TIMEOUT_SECONDS=${HTTP_TIMEOUT_SECONDS:-20}
  echo "Smoke mode: DXR_TAGS_LIMIT=$DXR_TAGS_LIMIT HTTP_TIMEOUT_SECONDS=$HTTP_TIMEOUT_SECONDS"
fi

export RUN_ONCE=1
python purview_dxr_integration.py
