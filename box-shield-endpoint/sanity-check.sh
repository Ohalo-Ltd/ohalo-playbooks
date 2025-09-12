#!/usr/bin/env bash
set -euo pipefail

# Sanity check for box-shield-endpoint configuration and connectivity
HERE_DIR="$(cd "$(dirname "$0")" && pwd)"
ENV_FILE="$HERE_DIR/.env"

if [[ -f "$ENV_FILE" ]]; then
  echo "Loading env from $ENV_FILE"
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
else
  echo "Note: $ENV_FILE not found. Relying on current environment."
fi

req=( DXR_APP_URL DXR_PAT_TOKEN )
missing=()
for v in "${req[@]}"; do
  if [[ -z "${!v-}" ]]; then missing+=("$v"); fi
done
if (( ${#missing[@]} > 0 )); then
  echo "Missing required env vars: ${missing[*]}"; exit 1
fi

# Determine Box auth mode
TOKEN="${BOX_DEVELOPER_TOKEN-}"
if [[ -z "${TOKEN}" && -n "${BOX_CONFIG_JSON_PATH-}" ]]; then
  echo "JWT config provided: ${BOX_CONFIG_JSON_PATH}"
  echo "Note: This sanity script does not mint a JWT; use dev token for quick checks or run Python which handles JWT."
fi

if [[ -n "${TOKEN}" ]]; then
  echo "Using Developer Token (for sanity only)."
  echo "Probing Box Security Classification template..."
  CODE=$(curl -sS -o /dev/stderr -w "%{http_code}" \
    -H "Authorization: Bearer ${TOKEN}" \
    -H 'Accept: application/json' \
    https://api.box.com/2.0/metadata_templates/enterprise/securityClassification-6VMVochwUWo/schema)
  if [[ "$CODE" == 2* ]]; then
    echo "✔ Security Classification template reachable"
  else
    echo "Security Classification probe returned HTTP ${CODE}. Check token/scopes."
  fi
else
  echo "No BOX_DEVELOPER_TOKEN provided; skip direct API probe. Use Python flow for JWT."
fi

echo "Done."
