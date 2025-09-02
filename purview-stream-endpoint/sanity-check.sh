#!/usr/bin/env bash
set -euo pipefail

# Sanity check for purview-stream-endpoint configuration and connectivity
# - Loads env from purview-stream-endpoint/.env if present
# - Validates required variables
# - Acquires OAuth tokens (data-plane and governance)
# - Probes a few Purview APIs for basic reachability/permissions

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

req=(
  DXR_APP_URL
  DXR_PAT_TOKEN
  PURVIEW_ENDPOINT
  AZURE_TENANT_ID
  AZURE_CLIENT_ID
  AZURE_CLIENT_SECRET
  PURVIEW_DOMAIN_NAME
)

missing=()
for v in "${req[@]}"; do
  if [[ -z "${!v-}" ]]; then
    missing+=("$v")
  fi
done

if (( ${#missing[@]} > 0 )); then
  echo "Missing required env vars: ${missing[*]}"
  exit 1
fi

SCOPE_DATA_PLANE=${PURVIEW_DATA_SCOPE:-"https://purview.azure.net/.default"}
SCOPE_GOV=${PURVIEW_GOV_RESOURCE:-"https://api.purview-service.microsoft.com/.default"}

echo "Acquiring OAuth tokens..."
TOKEN_DATA=$(curl -sS \
  -X POST "https://login.microsoftonline.com/${AZURE_TENANT_ID}/oauth2/v2.0/token" \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  --data-urlencode "client_id=${AZURE_CLIENT_ID}" \
  --data-urlencode "client_secret=${AZURE_CLIENT_SECRET}" \
  --data-urlencode "grant_type=client_credentials" \
  --data-urlencode "scope=${SCOPE_DATA_PLANE}" 2>&1)

if [[ "${TOKEN_DATA}" != *"access_token"* ]]; then
  echo "Failed to obtain data-plane token. Response: ${TOKEN_DATA}"
  exit 2
fi
ACCESS_TOKEN_DATA=$(printf '%s' "$TOKEN_DATA" | python -c 'import sys, json; print(json.load(sys.stdin)["access_token"])')
echo "✔ Data-plane token acquired"

# Try governance token: prefer shared-service scope, then fall back to data-plane scope if needed
TOKEN_GOV=$(curl -sS \
  -X POST "https://login.microsoftonline.com/${AZURE_TENANT_ID}/oauth2/v2.0/token" \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  --data-urlencode "client_id=${AZURE_CLIENT_ID}" \
  --data-urlencode "client_secret=${AZURE_CLIENT_SECRET}" \
  --data-urlencode "grant_type=client_credentials" \
  --data-urlencode "scope=${SCOPE_GOV}" 2>&1)

if [[ "${TOKEN_GOV}" == *"access_token"* ]]; then
  ACCESS_TOKEN_GOV=$(printf '%s' "$TOKEN_GOV" | python -c 'import sys, json; print(json.load(sys.stdin)["access_token"])')
  echo "✔ Governance token acquired (shared-service scope)"
else
  echo "Shared-service governance token failed; trying data-plane scope as fallback..."
  TOKEN_GOV2=$(curl -sS \
    -X POST "https://login.microsoftonline.com/${AZURE_TENANT_ID}/oauth2/v2.0/token" \
    -H 'Content-Type: application/x-www-form-urlencoded' \
    --data-urlencode "client_id=${AZURE_CLIENT_ID}" \
    --data-urlencode "client_secret=${AZURE_CLIENT_SECRET}" \
    --data-urlencode "grant_type=client_credentials" \
    --data-urlencode "scope=https://purview.azure.net/.default" 2>&1)
  if [[ "${TOKEN_GOV2}" != *"access_token"* ]]; then
    echo "Failed to obtain governance token with both scopes. Responses:"
    echo "- GOV scope (${SCOPE_GOV}): ${TOKEN_GOV}"
    echo "- Data-plane scope: ${TOKEN_GOV2}"
    exit 3
  fi
  ACCESS_TOKEN_GOV=$(printf '%s' "$TOKEN_GOV2" | python -c 'import sys, json; print(json.load(sys.stdin)["access_token"])')
  echo "✔ Governance token acquired (data-plane scope fallback)"
fi

# Probe: list collections via governance API
echo "Probing Collections (governance)..."
GOV_BASE=${PURVIEW_GOV_BASE:-"https://api.purview-service.microsoft.com"}
COLL_RESP=$(curl -sS -o /dev/stderr -w "%{http_code}" \
  -H "Authorization: Bearer ${ACCESS_TOKEN_GOV}" \
  "${GOV_BASE}/catalog/api/collections?api-version=2023-09-01")
if [[ "$COLL_RESP" != 2* ]]; then
  echo "Collections probe returned HTTP ${COLL_RESP}. You may need Collection Admin or correct endpoint."
else
  echo "✔ Collections reachable (governance)"
fi

# Probe: Atlas type read via data-plane (should be 200)
echo "Probing Atlas type read (data-plane)..."
TYPE_RESP=$(curl -sS -o /dev/stderr -w "%{http_code}" \
  -H "Authorization: Bearer ${ACCESS_TOKEN_DATA}" \
  -H 'Accept: application/json' \
  "${PURVIEW_ENDPOINT}/datamap/api/atlas/v2/types/typedef/name/Asset")
if [[ "$TYPE_RESP" != 2* ]]; then
  echo "Atlas type read returned HTTP ${TYPE_RESP}. Ensure Data Curator on a collection or correct endpoint."
else
  echo "✔ Atlas reachable (data-plane)"
fi

# Probe: Account-host Collections API using data-plane token (fallback path)
echo "Probing Account-host Collections (data-plane)..."
ACCT_COLL_RESP=$(curl -sS -o /dev/stderr -w "%{http_code}" \
  -H "Authorization: Bearer ${ACCESS_TOKEN_DATA}" \
  -H 'Accept: application/json' \
  "${PURVIEW_ENDPOINT}/account/collections?api-version=2019-11-01-preview")
if [[ "$ACCT_COLL_RESP" == 2* ]]; then
  echo "✔ Account-host collections reachable (data-plane)"
else
  echo "Account-host collections probe returned HTTP ${ACCT_COLL_RESP}"
fi

# Probe: Domain visibility by slug via governance
slug=$(printf '%s' "${PURVIEW_DOMAIN_NAME}" | tr '[:upper:]' '[:lower:]' | sed -E 's/[[:space:]]+/-/g; s/[^a-z0-9-]/-/g; s/-+/-/g; s/^-+//; s/-+$//')
echo "Probing Domain visibility for '${PURVIEW_DOMAIN_NAME}' (slug=${slug})..."
DOM_RESP=$(curl -sS -o /dev/stderr -w "%{http_code}" \
  -H "Authorization: Bearer ${ACCESS_TOKEN_GOV}" \
  -H 'Accept: application/json' \
  "${GOV_BASE}/catalog/api/domains/${slug}?api-version=2023-09-01")
case "$DOM_RESP" in
  200) echo "✔ Domain visible" ;;
  404) echo "Domain not found. Create it in Purview Studio or adjust name." ;;
  401|403) echo "Unauthorized to read domain. Grant Domain Administrator or appropriate role." ;;
  *) echo "Domain probe returned HTTP ${DOM_RESP}" ;;
esac

echo "Done."
