"""
Purview <-> Data X-Ray integration

Poll the Data X-Ray endpoint once per minute and sync each returned
"tag" as a custom asset in Microsoft Purview's Data Map.

Requirements (pip):
  azure-identity
  azure-purview-datamap   # Data Map (entities/types/lineage)
  requests
  python-dotenv

App registration (service principal) steps:
  - Create an app registration in Azure AD
  - Create a client secret for the app registration
  - Grant the service principal the Purview collection role (Data Curator or Collection Admin) on the target collection
  - Use the Purview endpoint in the form https://<account>.purview.azure.com
  Note: The SDK uses the https://purview.azure.net/.default scope implicitly.

Environment variables required:
  DXR_APP_URL               e.g., https://my-dxr.example.com
  DXR_PAT_TOKEN             Personal Access Token to call DXR API
  DXR_TAGS_PATH             (optional) path after base URL. default: /api/tags
  DXR_SEARCHABLE_DATASOURCES_PATH (optional) path for datasources. default: /api/datasources/searchable

  PURVIEW_ENDPOINT          e.g., https://<account-name>.purview.azure.com or https://api.purview-service.microsoft.com
  AZURE_TENANT_ID           Entra tenant ID
  AZURE_CLIENT_ID           App registration (client) ID
  AZURE_CLIENT_SECRET       App registration client secret
  PURVIEW_COLLECTION_ID     (optional) six-character collection ID to place/override entities into

  The service principal must be granted at least Data Curator on the target collection (Purview Studio > Collections > Access control).

Notes:
- We keep qualifiedName stable using the pattern: dxrtag://{tenant}/{tagId}
- We use bulk entity create/update (AtlasEntitiesWithExtInfo) for idempotent upserts
- We create a custom type `unstructured_dataset` if it doesn't exist
- When ingesting tags, we also call `/api/datasources/searchable` to map datasource IDs to names.

# See: https://learn.microsoft.com/en-us/purview/data-gov-api-create-assets and https://learn.microsoft.com/en-us/purview/data-gov-api-rest-data-plane

"""
from __future__ import annotations

import os
import time
import json
import logging
from typing import Dict, Any, List
from typing import Tuple, Set
from collections import defaultdict
import re

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from azure.identity import ClientSecretCredential
from azure.core.exceptions import HttpResponseError
from azure.purview.datamap import DataMapClient
from azure.purview.datamap.models import (
    AtlasEntity,
    AtlasEntitiesWithExtInfo,
)

from dotenv import load_dotenv

load_dotenv()

#
# PURVIEW_GOV_RESOURCE (optional) e.g., https://api.purview-service.microsoft.com/.default
# PURVIEW_GOV_BASE (optional) e.g., https://<account>.purview.azure.com
# To force a specific OAuth resource and silence warnings, you can set:
#   PURVIEW_GOV_RESOURCE=https://purview.azure.net/.default
#   (or) PURVIEW_GOV_RESOURCE=https://api.purview-service.microsoft.com/.default
# ------------ Configuration & Logging ------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("purview-dxr")

POLL_SECONDS = int(os.getenv("POLL_SECONDS", "60"))
SYNC_DELETE = os.getenv("SYNC_DELETE", "0").lower() in ("1", "true", "yes")
DISABLE_GOVERNANCE = os.getenv("DISABLE_GOVERNANCE", "0").lower() in ("1", "true", "yes")

# ------------ Governance (Catalog Admin) API helpers ------------
GOV_API_VERSION = "2023-09-01"

def _governance_base() -> str:
    """
    Base for Governance (Catalog Admin) API calls.

    Important: Governance APIs are hosted on the shared service domain
    (https://api.purview-service.microsoft.com) and NOT on the account endpoint
    (https://<account>.purview.azure.com). Allow overriding via PURVIEW_GOV_BASE.
    """
    gov_base = os.environ.get("PURVIEW_GOV_BASE")
    if gov_base:
        return gov_base.rstrip("/")
    return "https://api.purview-service.microsoft.com"

def _governance_headers() -> Dict[str, str]:
    """
    Acquire a token for the Governance (Catalog Admin) API and build headers.
    We prefer the well-known Purview data-plane resource first to maximize compatibility,
    then try the shared service resource. Users can override via PURVIEW_GOV_RESOURCE.
    """
    from azure.identity import ClientSecretCredential
    from azure.core.exceptions import ClientAuthenticationError

    cred = ClientSecretCredential(tenant_id=TENANT_ID, client_id=CLIENT_ID, client_secret=CLIENT_SECRET)

    # Build scopes to try, in order:
    # 1) explicit override (if provided)
    # 2) api.purview-service.microsoft.com (shared service governance)
    # 3) purview.azure.net (data plane) — fallback if tenant hasn't consented to shared service app
    scopes_to_try: List[str] = []

    gov_resource = os.environ.get("PURVIEW_GOV_RESOURCE")
    if gov_resource:
        scope = gov_resource.strip()
        if not scope.endswith("/.default"):
            scope = scope[:-1] + ".default" if scope.endswith("/") else scope + "/.default"
        scopes_to_try.append(scope)

    # Prefer shared-service governance resource, then fall back to data plane
    scopes_to_try.append("https://api.purview-service.microsoft.com/.default")
    scopes_to_try.append("https://purview.azure.net/.default")

    last_exc: Exception | None = None
    token: str | None = None
    for scope in scopes_to_try:
        try:
            token = cred.get_token(scope).token
            break
        except ClientAuthenticationError as exc:
            # Expected on tenants where the resource isn't present/consented; try the next scope.
            last_exc = exc
            continue

    if not token:
        # All scopes failed
        if last_exc:
            raise last_exc
        raise SystemExit("Failed to acquire token for governance API (all scopes tried)")

    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

# ------------ Collections API helpers (governance vs account-host fallback) ------------

def _gov_collections_put(name: str, body: Dict[str, Any]) -> requests.Response:
    base = _governance_base()
    headers = _governance_headers()
    url = f"{base}/catalog/api/collections/{name}?api-version={GOV_API_VERSION}"
    return requests.put(url, headers=headers, json=body)

def _gov_collections_get(name: str) -> requests.Response:
    base = _governance_base()
    headers = _governance_headers()
    url = f"{base}/catalog/api/collections/{name}?api-version={GOV_API_VERSION}"
    return requests.get(url, headers=headers)

ACCOUNT_COLLECTIONS_API_VERSION = "2019-11-01-preview"

def _acct_collections_put(name: str, body: Dict[str, Any]) -> requests.Response:
    """Account-host Collections API using data-plane token."""
    url = f"{PURVIEW_ENDPOINT}/account/collections/{name}?api-version={ACCOUNT_COLLECTIONS_API_VERSION}"
    headers = _atlas_headers()
    return requests.put(url, headers=headers, json=body)

def _acct_collections_get(name: str) -> requests.Response:
    url = f"{PURVIEW_ENDPOINT}/account/collections/{name}?api-version={ACCOUNT_COLLECTIONS_API_VERSION}"
    headers = _atlas_headers()
    return requests.get(url, headers=headers)

def _normalize_purview_endpoint(raw: str) -> str:
    """Ensure endpoint has scheme and points to an account-specific host.
    Accepts forms like 'contoso.purview.azure.com' or 'https://contoso.purview.azure.com'.
    Rejects 'purview.azure.net' (AAD resource) and bare 'purview.azure.com' without account.
    """
    if not raw:
        raise SystemExit("PURVIEW_ENDPOINT is required.")
    ep = raw.strip()
    if not ep.startswith("http"):
        ep = "https://" + ep
    # Strip trailing slashes
    ep = ep.rstrip("/")
    # Quick sanity checks
    host = ep.split("//", 1)[1]
    if host == "purview.azure.net":
        raise SystemExit(
            "PURVIEW_ENDPOINT must be your account endpoint (e.g., https://<account>.purview.azure.com), not purview.azure.net"
        )
    if host.endswith("purview.azure.com") and host.count(".") < 3:
        # expect <account>.purview.azure.com => at least 3 dots in host
        raise SystemExit(
            "PURVIEW_ENDPOINT looks incomplete. Expected format: https://<account>.purview.azure.com"
        )
    return ep

def _normalize_base_url(raw: str) -> str:
    """Ensure base URL has scheme and no trailing slash."""
    if not raw:
        raise SystemExit("DXR_APP_URL is required.")
    url = raw.strip()
    if not url.startswith("http"):
        url = "https://" + url
    url = url.rstrip("/")
    return url

DXR_APP_URL = _normalize_base_url(os.environ.get("DXR_APP_URL"))
DXR_PAT_TOKEN = os.environ.get("DXR_PAT_TOKEN")
# Remove DXR_FILES_PATH, add DXR_TAGS_PATH and DXR_SEARCHABLE_DATASOURCES_PATH
DXR_TAGS_PATH = os.environ.get("DXR_TAGS_PATH", "/api/tags")
DXR_SEARCHABLE_DATASOURCES_PATH = os.environ.get("DXR_SEARCHABLE_DATASOURCES_PATH", "/api/datasources/searchable")

PURVIEW_ENDPOINT = _normalize_purview_endpoint(os.environ.get("PURVIEW_ENDPOINT"))
TENANT_ID = os.environ.get("AZURE_TENANT_ID")
CLIENT_ID = os.environ.get("AZURE_CLIENT_ID")
CLIENT_SECRET = os.environ.get("AZURE_CLIENT_SECRET")
PURVIEW_COLLECTION_ID = os.environ.get("PURVIEW_COLLECTION_ID")  # optional (default write target for reduced-permissions mode)
PURVIEW_DOMAIN_ID = os.environ.get("PURVIEW_DOMAIN_ID")  # optional: domain referenceName (alias for parent selection)
PURVIEW_PARENT_COLLECTION_ID = os.environ.get("PURVIEW_PARENT_COLLECTION_ID")  # optional: parent refName under which to create child collections
UNSTRUCTURED_DATASETS_COLLECTION_NAME = os.environ.get("UNSTRUCTURED_DATASETS_COLLECTION_NAME", "Unstructured Datasets")
UNSTRUCTURED_DATASETS_COLLECTION_REF = os.environ.get("UNSTRUCTURED_DATASETS_COLLECTION_REF")  # optional fixed refName for UD collection
PURVIEW_DOMAIN_NAME = os.environ.get("PURVIEW_DOMAIN_NAME")

REQUIRED_ENV = [
    "DXR_APP_URL",
    "DXR_PAT_TOKEN",
    "PURVIEW_ENDPOINT",
    "AZURE_TENANT_ID",
    "AZURE_CLIENT_ID",
    "AZURE_CLIENT_SECRET",
    "PURVIEW_DOMAIN_NAME",
]
missing = [k for k in REQUIRED_ENV if not os.environ.get(k)]
if missing:
    raise SystemExit(f"Missing required environment variables: {', '.join(missing)}")
def _account_get_domain(domain_name: str) -> requests.Response:
    """
    Get a domain by its slugified name using the /account/domains/{slug} endpoint.
    (Not used by ensure_domain_exists anymore.)
    """
    slug = _slug(domain_name)
    return _account_get(f"/domains/{slug}")

def ensure_domain_exists(domain_name: str) -> None:
    """
    Ensure the given domain exists in Purview. If not, raise with guidance.
    Prefer a **direct** domain read (GET /catalog/api/domains/{slug}) which requires only
    visibility on that domain, instead of listing **all** domains (which may require
    broader admin rights in some tenants).
    """
    slug = _slug(domain_name)
    base = _governance_base()
    headers = _governance_headers()
    url = f"{base}/catalog/api/domains/{slug}?api-version={GOV_API_VERSION}"
    resp = requests.get(url, headers=headers)

    # Successful: domain exists and is visible
    if resp.status_code == 200:
        return

    # Not found: clear, actionable error
    if resp.status_code == 404:
        raise SystemExit(f"Domain {domain_name} not found in Purview. Ensure it is created manually.")

    # Authz issues: user/service principal cannot view this domain
    if resp.status_code in (401, 403):
        raise SystemExit(
            f"Not authorized to access domain '{domain_name}'. "
            "Grant the service principal Domain Administrator (or higher) on that domain."
        )

    # Anything else: bubble up the HTTP error with context
    try:
        resp.raise_for_status()
    except requests.HTTPError as e:
        # Attach a short body preview for easier debugging
        body = resp.text[:300] if hasattr(resp, "text") else ""
        raise requests.HTTPError(f"{e} Body: {body}") from e

# ------------ HTTP Client (DXR) ------------

def _build_http_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(total=5, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504])
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.mount("http://", HTTPAdapter(max_retries=retries))
    return s

HTTP = _build_http_session()

def _get_access_token() -> str:
    cred = ClientSecretCredential(tenant_id=TENANT_ID, client_id=CLIENT_ID, client_secret=CLIENT_SECRET)
    token = cred.get_token("https://purview.azure.net/.default").token
    return token

# ------------ Atlas REST helpers ------------
def _atlas_headers() -> Dict[str, str]:
    token = _get_access_token()
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

def _atlas_get(path: str, params: Dict[str, Any] | None = None) -> requests.Response:
    url = f"{PURVIEW_ENDPOINT}{path}"
    return requests.get(url, headers=_atlas_headers(), params=params)

def _atlas_post(path: str, payload: Dict[str, Any]) -> requests.Response:
    url = f"{PURVIEW_ENDPOINT}{path}"
    return requests.post(url, headers=_atlas_headers(), json=payload)

def _atlas_delete(path: str, params: Dict[str, Any] | None = None) -> requests.Response:
    url = f"{PURVIEW_ENDPOINT}{path}"
    return requests.delete(url, headers=_atlas_headers(), params=params)




def get_dxr_tags() -> List[Dict[str, Any]]:
    """Call DXR /api/tags endpoint and return a list of tag dicts."""
    url = f"{DXR_APP_URL}{DXR_TAGS_PATH}"
    headers = {
        "Authorization": f"Bearer {DXR_PAT_TOKEN}",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9,ja;q=0.8",
        "Referer": DXR_APP_URL,
    }
    try:
        resp = HTTP.get(url, headers=headers, timeout=30)
    except Exception as e:
        logger.error("Request to %s failed with exception: %s", url, e)
        raise
    if resp.status_code == 401 or resp.status_code == 403:
        raise SystemExit(
            f"Unauthorized (status {resp.status_code}) from DXR endpoint. "
            "Check DXR_PAT_TOKEN and permissions."
        )
    if resp.status_code >= 400:
        logger.error("Failed to fetch tags: HTTP %s %s", resp.status_code, resp.text[:500])
        resp.raise_for_status()
    try:
        data = resp.json()
        if isinstance(data, list):
            return data
        if isinstance(data, dict) and "items" in data and isinstance(data["items"], list):
            return data["items"]
        logger.warning("Unexpected DXR tags response shape: %s", type(data))
        return []
    except Exception as e:
        logger.warning("Failed to parse DXR tags response as JSON: %s", e)
        return []


def get_dxr_searchable_datasources() -> Tuple[Dict[str, str], List[Dict[str, Any]]]:
    """Call DXR /api/datasources/searchable and return a dict mapping id (str) to name, and the full list."""
    url = f"{DXR_APP_URL}{DXR_SEARCHABLE_DATASOURCES_PATH}"
    headers = {
        "Authorization": f"Bearer {DXR_PAT_TOKEN}",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9,ja;q=0.8",
        "Referer": DXR_APP_URL,
    }
    try:
        resp = HTTP.get(url, headers=headers, timeout=30)
    except Exception as e:
        logger.error("Request to %s failed with exception: %s", url, e)
        return {}, []
    if resp.status_code == 401 or resp.status_code == 403:
        logger.error("Unauthorized (status %s) fetching datasources", resp.status_code)
        return {}, []
    if resp.status_code >= 400:
        logger.error("Failed to fetch datasources: HTTP %s %s", resp.status_code, resp.text[:500])
        return {}, []
    try:
        data = resp.json()
        if isinstance(data, list):
            name_map = {str(item.get("id")): item.get("name", "") for item in data if "id" in item}
            return name_map, data
        if isinstance(data, dict) and "items" in data and isinstance(data["items"], list):
            name_map = {str(item.get("id")): item.get("name", "") for item in data["items"] if "id" in item}
            return name_map, data["items"]
        logger.warning("Unexpected DXR datasources response shape: %s", type(data))
        return {}, []
    except Exception as e:
        logger.warning("Failed to parse DXR datasources response as JSON: %s", e)
        return {}, []

# ------------ Purview (Data Map) ------------

def get_purview_client() -> DataMapClient:
    cred = ClientSecretCredential(tenant_id=TENANT_ID, client_id=CLIENT_ID, client_secret=CLIENT_SECRET)
    return DataMapClient(endpoint=PURVIEW_ENDPOINT, credential=cred)


CUSTOM_TYPE_NAME = "unstructured_dataset"

def ensure_types_and_relationships() -> None:
    # Check for unstructured_dataset; if missing, (re)submit typedefs
    check = _atlas_get(f"/datamap/api/atlas/v2/types/typedef/name/{CUSTOM_TYPE_NAME}")
    if check.status_code == 200:
        return
    if check.status_code not in (200, 404):
        check.raise_for_status()

    # Entity defs
    unstructured_dataset_def = {
        "name": "unstructured_dataset",
        "superTypes": ["DataSet"],
        "attributeDefs": [
            {"name": "type", "typeName": "string", "isOptional": True},
            {"name": "parametersParameters", "typeName": "array<string>", "isOptional": True},
            {"name": "parametersValues", "typeName": "array<string>", "isOptional": True},
            {"name": "parametersTypes", "typeName": "array<string>", "isOptional": True},
            {"name": "parametersMatchStrategies", "typeName": "array<string>", "isOptional": True},
            {"name": "parametersOperators", "typeName": "array<string>", "isOptional": True},
            {"name": "parametersGroupIds", "typeName": "array<int>", "isOptional": True},
            {"name": "parametersGroupOrders", "typeName": "array<int>", "isOptional": True},
            {"name": "datasourceIds", "typeName": "array<string>", "isOptional": True},
            {"name": "datasourceNames", "typeName": "array<string>", "isOptional": True},
            {"name": "dxrTenant", "typeName": "string", "isOptional": True},
            {"name": "tagId", "typeName": "string", "isOptional": True},
        ],
    }

    unstructured_datasource_def = {
        "name": "unstructured_datasource",
        "superTypes": ["DataSet"],
        "attributeDefs": [
            {"name": "datasourceId", "typeName": "string", "isOptional": False},
            {"name": "connectorTypeName", "typeName": "string", "isOptional": True},
            {"name": "dxrTenant", "typeName": "string", "isOptional": True},
        ],
    }

    rel_ds_has_dataset = {
        "name": "unstructured_datasource_has_unstructured_dataset",
        "relationshipCategory": "COMPOSITION",
        "endDef1": {
            "type": "unstructured_datasource",
            "name": "datasets",
            "isContainer": True,
            "cardinality": "SET"
        },
        "endDef2": {
            "type": "unstructured_dataset",
            "name": "datasource",
            "isContainer": False,
            "cardinality": "SINGLE"
        },
        "propagateTags": "NONE"
    }

    payload = {"entityDefs": [unstructured_datasource_def, unstructured_dataset_def],
               "relationshipDefs": [rel_ds_has_dataset]}
    resp = _atlas_post("/datamap/api/atlas/v2/types/typedefs", payload)
    if resp.status_code not in (200, 201):
        logger.error("Failed to create typedefs: %s %s", resp.status_code, resp.text)
        resp.raise_for_status()
    logger.info("Ensured custom types & relationship")


def _to_qualified_name(tag: Dict[str, Any]) -> str:
    tenant = tag.get("tenant") or os.environ.get("DXR_TENANT", "default")
    tag_id = str(tag.get("id"))
    return f"dxrtag://{tenant}/{tag_id}"

def _qn_datasource(tenant: str, dsid: str) -> str:
    return f"dxrds://{tenant}/{dsid}"
def _account_get(path: str) -> requests.Response:
    """
    Back-compat wrapper routed to the Governance (Catalog Admin) API.
    Example: _account_get("/collections") -> GET {base}/catalog/api/collections?api-version=...
    """
    base = _governance_base()
    headers = _governance_headers()
    p = path if path.startswith("/") else f"/{path}"
    url = f"{base}/catalog/api{p}?api-version={GOV_API_VERSION}"
    return requests.get(url, headers=headers)

def _account_put(path: str, payload: Dict[str, Any]) -> requests.Response:
    """
    Back-compat wrapper routed to the Governance (Catalog Admin) API.
    Example: _account_put("/collections/{name}", {...})
    """
    base = _governance_base()
    headers = _governance_headers()
    p = path if path.startswith("/") else f"/{path}"
    url = f"{base}/catalog/api{p}?api-version={GOV_API_VERSION}"
    return requests.put(url, headers=headers, json=payload)

def _account_delete(path: str) -> requests.Response:
    """
    Back-compat wrapper routed to the Governance (Catalog Admin) API.
    Example: _account_delete("/collections/{name}")
    """
    base = _governance_base()
    headers = _governance_headers()
    p = path if path.startswith("/") else f"/{path}"
    url = f"{base}/catalog/api{p}?api-version={GOV_API_VERSION}"
    return requests.delete(url, headers=headers)

_slug_re = re.compile(r"[^a-z0-9-]+")

def _slug(name: str) -> str:
    s = (name or "").strip().lower()
    # Replace spaces with hyphens first
    s = s.replace(" ", "-")
    # Replace any non [a-z0-9-] with hyphen
    s = _slug_re.sub("-", s)
    # Collapse multiple hyphens
    s = re.sub(r"-+", "-", s)
    # Trim leading/trailing hyphens and enforce length
    s = s.strip("-") or "unnamed"
    return s[:90]

def _make_collection_ref_for_datasource(dsname: str, dsid: str, max_len: int = 36) -> str:
    """Build a collection referenceName that satisfies account API constraints (3..36 chars).
    Strategy: slug(dsname) + '-' + dsid if it fits; otherwise truncate the slug to fit.
    If dsid itself is very long, shorten the suffix using a hash.
    """
    base = _slug(dsname)
    id_str = str(dsid)
    # If id is too long, shorten using a stable 8-char hash
    if len(id_str) > max_len - 3:  # leave at least 2 for base + '-'
        import hashlib
        short = hashlib.sha1(id_str.encode("utf-8")).hexdigest()[:8]
        id_str = short
    suffix = f"-{id_str}"
    # Truncate base to fit
    keep = max_len - len(suffix)
    if keep < 1:
        # fallback: minimal base
        base_part = "c"
    else:
        base_part = base[:keep]
        base_part = base_part.strip("-") or "c"
    ref = f"{base_part}{suffix}"
    # Enforce minimum length 3
    if len(ref) < 3:
        ref = (ref + "xxx")[:3]
    return ref

def ensure_collections(domain_name: str, datasource_items: List[Dict[str, Any]]) -> Dict[str, str]:
    """
    Ensure the parent collection for the domain exists (unless PURVIEW_PARENT_COLLECTION_ID is provided),
    and child collections for each datasource under the chosen parent.
    Uses the Catalog Admin API endpoints for collections (/catalog/api/collections/{name}?api-version=2023-09-01).
    """
    # Determine parent collection referenceName
    if PURVIEW_PARENT_COLLECTION_ID:
        parent_name = PURVIEW_PARENT_COLLECTION_ID
    elif PURVIEW_DOMAIN_ID:
        # Domain IDs cannot serve as parent collections for the account-host API.
        # We'll try governance path to materialize a parent collection from the domain name slug.
        parent_name = _slug(domain_name)
        parent_body = {"friendlyName": domain_name}
        resp = _gov_collections_put(parent_name, parent_body)
        if resp.status_code not in (200, 201):
            # Fallback only works with a collection as parent. Ask user to set PURVIEW_PARENT_COLLECTION_ID.
            logger.warning("Governance parent ensure failed (%s). Set PURVIEW_PARENT_COLLECTION_ID to an existing collection refName.", resp.status_code)
            raise requests.HTTPError(response=resp)
    else:
        # Try to ensure a parent collection derived from domain slug via governance
        parent_name = _slug(domain_name)
        parent_body = {"friendlyName": domain_name}
        resp = _gov_collections_put(parent_name, parent_body)
        if resp.status_code not in (200, 201):
            logger.warning("Governance parent ensure failed (%s). Set PURVIEW_PARENT_COLLECTION_ID to an existing collection refName to proceed.", resp.status_code)
            raise requests.HTTPError(response=resp)

    child_map: Dict[str, str] = {}
    def _acct_put_with_variants(name: str, dsname: str, parent: str) -> requests.Response:
        variants = [
            {"friendlyName": dsname, "parentCollection": {"referenceName": parent}},
            {"name": dsname, "parentCollection": {"referenceName": parent}},
            {"friendlyName": dsname, "parentCollection": {"type": "CollectionReference", "referenceName": parent}},
            {"name": dsname, "parentCollection": {"type": "CollectionReference", "referenceName": parent}},
        ]
        last = None
        for body in variants:
            r = _acct_collections_put(name, body)
            if r.status_code in (200, 201):
                return r
            last = r
        assert last is not None
        return last

    for item in datasource_items:
        dsid = str(item.get("id"))
        dsname = item.get("name", dsid)
        child_name = _make_collection_ref_for_datasource(dsname, dsid)
        body = {"friendlyName": dsname, "parentCollection": {"referenceName": parent_name}}
        # Try governance first
        r = _gov_collections_put(child_name, body)
        if r.status_code not in (200, 201):
            # Fallback to account-host Collections API
            r2 = _acct_put_with_variants(child_name, dsname, parent_name)
            if r2.status_code not in (200, 201):
                # include short body previews to aid troubleshooting
                b1 = getattr(r, "text", "")[:180]
                b2 = getattr(r2, "text", "")[:180]
                logger.error("Failed to ensure child collection '%s': gov=%s acct=%s govBody=%s acctBody=%s", child_name, r.status_code, r2.status_code, b1, b2)
                r2.raise_for_status()
        child_map[dsid] = child_name
    return child_map

def ensure_unstructured_datasets_collection(parent_reference: str) -> str:
    """Ensure a single collection for unstructured datasets exists under the given parent and return its referenceName."""
    ref = UNSTRUCTURED_DATASETS_COLLECTION_REF or _slug(UNSTRUCTURED_DATASETS_COLLECTION_NAME)
    body = {
        "friendlyName": UNSTRUCTURED_DATASETS_COLLECTION_NAME,
        "parentCollection": {"referenceName": parent_reference},
    }
    # Try governance first
    r = _gov_collections_put(ref, body)
    if r.status_code not in (200, 201):
        # Fallback to account-host Collections API with variants
        variants = [
            {"friendlyName": UNSTRUCTURED_DATASETS_COLLECTION_NAME, "parentCollection": {"referenceName": parent_reference}},
            {"name": UNSTRUCTURED_DATASETS_COLLECTION_NAME, "parentCollection": {"referenceName": parent_reference}},
            {"friendlyName": UNSTRUCTURED_DATASETS_COLLECTION_NAME, "parentCollection": {"type": "CollectionReference", "referenceName": parent_reference}},
            {"name": UNSTRUCTURED_DATASETS_COLLECTION_NAME, "parentCollection": {"type": "CollectionReference", "referenceName": parent_reference}},
        ]
        last = None
        for body2 in variants:
            r2 = _acct_collections_put(ref, body2)
            if r2.status_code in (200, 201):
                return ref
            last = r2
        if last is not None:
            b1 = getattr(r, "text", "")[:180]
            b2 = getattr(last, "text", "")[:180]
            logger.error("Failed to ensure UD collection '%s': gov=%s acct=%s govBody=%s acctBody=%s", ref, r.status_code, last.status_code, b1, b2)
            last.raise_for_status()
    return ref

def create_relationship(rel_type: str, end1_qn: str, end1_type: str, end2_qn: str, end2_type: str) -> None:
    payload = {
        "typeName": rel_type,
        "end1": {"typeName": end1_type, "uniqueAttributes": {"qualifiedName": end1_qn}},
        "end2": {"typeName": end2_type, "uniqueAttributes": {"qualifiedName": end2_qn}},
    }
    resp = _atlas_post("/datamap/api/atlas/v2/relationship", payload)
    if resp.status_code in (200, 201):
        return
    if resp.status_code == 409:
        # already exists
        return
    logger.warning("Relationship create failed (%s): %s", resp.status_code, resp.text[:300])


def build_entities_payload_for_tags(tags: List[Dict[str, Any]], ds_map: Dict[str, str]) -> AtlasEntitiesWithExtInfo:
    entities: List[AtlasEntity] = []
    dxr_tenant = os.environ.get("DXR_TENANT", "default")
    for tag in tags:
        qn = _to_qualified_name(tag)
        tag_id = str(tag.get("id"))
        name = tag.get("name")
        description = tag.get("description")
        type_ = tag.get("type")
        # Extract parameters arrays from first savedQueryDtoList's query.query_items
        parametersParameters = []
        parametersValues = []
        parametersTypes = []
        parametersMatchStrategies = []
        parametersOperators = []
        parametersGroupIds = []
        parametersGroupOrders = []
        # Collect datasourceIds from all savedQueryDtoList entries
        datasource_ids_set = set()
        saved_query_dtos = tag.get("savedQueryDtoList") or []
        if saved_query_dtos:
            # For parameters arrays, use first query's query_items if present
            first_query_dto = saved_query_dtos[0]
            query = first_query_dto.get("query", {})
            query_items = query.get("query_items", []) if isinstance(query, dict) else []
            for item in query_items:
                parametersParameters.append(str(item.get("parameter", "")))
                parametersValues.append(str(item.get("value", "")))
                parametersTypes.append(str(item.get("type", "")))
                parametersMatchStrategies.append(str(item.get("match_strategy", "")))
                parametersOperators.append(str(item.get("operator", "")))
                # group_id and group_order as ints if possible
                try:
                    gid = int(item.get("group_id")) if item.get("group_id") is not None else None
                except Exception:
                    gid = None
                try:
                    gorder = int(item.get("group_order")) if item.get("group_order") is not None else None
                except Exception:
                    gorder = None
                parametersGroupIds.append(gid if gid is not None else 0)
                parametersGroupOrders.append(gorder if gorder is not None else 0)
            # Collect all datasourceIds from all savedQueryDtoList entries
            for dto in saved_query_dtos:
                ds_ids = dto.get("datasourceIds") or []
                for dsid in ds_ids:
                    datasource_ids_set.add(str(dsid))
        datasourceIds = list(datasource_ids_set)
        datasourceNames = [ds_map.get(dsid, "") for dsid in datasourceIds]
        attrs = {
            "qualifiedName": qn,
            "name": name,
            "description": description,
            "type": type_,
            "parametersParameters": parametersParameters,
            "parametersValues": parametersValues,
            "parametersTypes": parametersTypes,
            "parametersMatchStrategies": parametersMatchStrategies,
            "parametersOperators": parametersOperators,
            "parametersGroupIds": parametersGroupIds,
            "parametersGroupOrders": parametersGroupOrders,
            "datasourceIds": datasourceIds,
            "datasourceNames": datasourceNames,
            "dxrTenant": tag.get("tenant", dxr_tenant),
            "tagId": tag_id,
        }
        entity = AtlasEntity(type_name=CUSTOM_TYPE_NAME, attributes=attrs)
        entities.append(entity)
    return AtlasEntitiesWithExtInfo(entities=entities)


def upsert_unstructured_datasets(client: DataMapClient, tags: List[Dict[str, Any]], ds_map: Dict[str, str], *, collection_id: str | None = None) -> Dict[str, str]:
    if not tags:
        logger.info("No tags to upsert this cycle.")
        return {}
    payload = build_entities_payload_for_tags(tags, ds_map)
    # Helper to resolve by qualifiedName if needed
    def _resolve_guids_by_qn(candidates: List[Dict[str, Any]]) -> Dict[str, str]:
        out: Dict[str, str] = {}
        for tag in candidates:
            qn = _to_qualified_name(tag)
            path = f"/datamap/api/atlas/v2/entity/uniqueAttribute/type/{CUSTOM_TYPE_NAME}"
            resp = _atlas_get(path, params={"attr:qualifiedName": qn})
            if resp.status_code == 200:
                try:
                    guid = resp.json().get("guid")
                    if guid:
                        out[qn] = guid
                except Exception:
                    continue
        return out

    try:
        target_collection = collection_id or os.environ.get("PURVIEW_COLLECTION_ID")
        if target_collection:
            result = client.entity.batch_create_or_update(payload, collection_id=target_collection)
        else:
            result = client.entity.batch_create_or_update(payload)
        created = len(result.get("mutatedEntities", {}).get("CREATE", [])) if isinstance(result, dict) else None
        updated = len(result.get("mutatedEntities", {}).get("UPDATE", [])) if isinstance(result, dict) else None
        logger.info("Upserted unstructured_datasets: created=%s updated=%s", created, updated)
        guid_map: Dict[str, str] = {}
        if isinstance(result, dict):
            for sec in ("CREATE", "UPDATE"):
                for ent in result.get("mutatedEntities", {}).get(sec, []):
                    qn = ent.get("attributes", {}).get("qualifiedName")
                    guid = ent.get("guid")
                    if qn and guid:
                        guid_map[qn] = guid
        # If SDK returned success but we couldn't extract GUIDs, fall back to unique-attribute lookups
        if not guid_map:
            logger.debug("SDK upsert returned no GUIDs in payload; resolving by qualifiedName.")
            guid_map = _resolve_guids_by_qn(tags)
        return guid_map
    except Exception as e:
        # Fallback: resolve by unique attribute; this covers cases where SDK errors on empty response bodies.
        logger.warning("batch_create_or_update raised (%s). Falling back to unique-attribute lookups for tags.", e)
        return _resolve_guids_by_qn(tags)

def upsert_unstructured_datasources(client: DataMapClient, tenant: str, ds_full: List[Dict[str, Any]], *, by_collection: Dict[str, List[Dict[str, Any]]] | None = None) -> Dict[str, str]:
    def _build_entities(items: List[Dict[str, Any]]) -> AtlasEntitiesWithExtInfo:
        entities: List[AtlasEntity] = []
        for item in items:
            dsid = str(item.get("id"))
            name = item.get("name", dsid)
            ctype = item.get("connectorTypeName") or item.get("connectorType") or ""
            attrs = {
                "qualifiedName": _qn_datasource(tenant, dsid),
                "name": name,
                "datasourceId": dsid,
                "connectorTypeName": ctype,
                "dxrTenant": tenant,
            }
            entities.append(AtlasEntity(type_name="unstructured_datasource", attributes=attrs))
        return AtlasEntitiesWithExtInfo(entities=entities)

    if not ds_full:
        return {}

    def _resolve_guids_by_qn(items: List[Dict[str, Any]]) -> Dict[str, str]:
        out: Dict[str, str] = {}
        for it in items:
            dsid = str(it.get("id"))
            qn = _qn_datasource(tenant, dsid)
            path = "/datamap/api/atlas/v2/entity/uniqueAttribute/type/unstructured_datasource"
            resp = _atlas_get(path, params={"attr:qualifiedName": qn})
            if resp.status_code == 200:
                try:
                    guid = resp.json().get("guid")
                    if guid:
                        out[qn] = guid
                except Exception:
                    continue
        return out

    guid_map: Dict[str, str] = {}
    try:
        # Group by collection if provided; otherwise single batch (possibly with PURVIEW_COLLECTION_ID)
        if by_collection:
            for coll, items in by_collection.items():
                if not items:
                    continue
                payload = _build_entities(items)
                result = client.entity.batch_create_or_update(payload, collection_id=coll)
                if isinstance(result, dict):
                    for sec in ("CREATE", "UPDATE"):
                        for ent in result.get("mutatedEntities", {}).get(sec, []):
                            qn = ent.get("attributes", {}).get("qualifiedName")
                            gid = ent.get("guid")
                            if qn and gid:
                                guid_map[qn] = gid
        else:
            payload = _build_entities(ds_full)
            collection_id = os.environ.get("PURVIEW_COLLECTION_ID")
            if collection_id:
                result = client.entity.batch_create_or_update(payload, collection_id=collection_id)
            else:
                result = client.entity.batch_create_or_update(payload)
            if isinstance(result, dict):
                for sec in ("CREATE", "UPDATE"):
                    for ent in result.get("mutatedEntities", {}).get(sec, []):
                        qn = ent.get("attributes", {}).get("qualifiedName")
                        gid = ent.get("guid")
                        if qn and gid:
                            guid_map[qn] = gid
        if not guid_map:
            logger.debug("SDK upsert returned no GUIDs for datasources; resolving by qualifiedName.")
            guid_map = _resolve_guids_by_qn(ds_full)
        return guid_map
    except Exception as e:
        logger.warning("batch_create_or_update(datasources) raised (%s). Falling back to unique-attribute lookups.", e)
        return _resolve_guids_by_qn(ds_full)

def resolve_guid_by_qn(type_name: str, qualified_name: str, attempts: int = 8, delay_seconds: float = 1.5) -> str | None:
    """
    Best-effort GUID resolver with retries. Tries the Atlas unique-attribute lookup first,
    then falls back to an advanced search on qualifiedName. Useful to smooth over eventual consistency after upserts.
    """
    # Normalize inputs
    t = (type_name or "").strip()
    qn = (qualified_name or "").strip()
    if not t or not qn:
        return None

    # 1) Retry uniqueAttribute lookup
    for i in range(max(1, attempts)):
        try:
            resp = _atlas_get(f"/datamap/api/atlas/v2/entity/uniqueAttribute/type/{t}", params={"attr:qualifiedName": qn})
            if resp.status_code == 200:
                try:
                    data = resp.json()
                    guid = data.get("guid")
                    if guid:
                        return guid
                except Exception:
                    pass
            elif resp.status_code in (401, 403):
                # permissions issue, don't hammer
                break
        except Exception:
            pass
        time.sleep(delay_seconds)

    # 2) Fallback: advanced search on qualifiedName (exact match)
    try:
        payload = {
            "typeName": t,
            "excludeDeletedEntities": True,
            "where": {
                "attributeName": "qualifiedName",
                "operator": "eq",
                "attributeValue": qn,
            },
            "attributes": ["qualifiedName"],
            "limit": 1,
        }
        resp2 = _atlas_post("/datamap/api/atlas/v2/search/advanced", payload)
        if resp2.status_code == 200:
            try:
                data2 = resp2.json()
                for ent in data2.get("entities", []) or []:
                    g = ent.get("guid") or ent.get("entity", {}).get("guid")
                    if g:
                        return g
            except Exception:
                pass
    except Exception:
        pass
    return None

# ---------- Entity collection helpers ----------
ENTITY_API_VERSION = "2023-09-01"

def _entity_get(path: str, params: Dict[str, Any] | None = None) -> requests.Response:
    url = f"{PURVIEW_ENDPOINT}{path}"
    headers = _atlas_headers()
    return requests.get(url, headers=headers, params=params)


def get_entity_collection_info(guid: str) -> Dict[str, Any] | None:
    """Fetch entity (data plane) metadata that may include collection membership.
    Tries the entity endpoint first (preferred for collection info), then falls back to Atlas if needed.
    Returns a dict with best-effort fields: {"collectionName": str|None, "collectionId": str|None, "raw": <service payload>} or None on hard failure.
    """
    try:
        # Data plane entity endpoint tends to include collection info
        r = _entity_get(f"/datamap/api/entity/guid/{guid}", params={"api-version": ENTITY_API_VERSION})
        if r.status_code == 200:
            try:
                data = r.json()
            except Exception:
                data = {}
            # normalize
            info = {
                "collectionName": data.get("collectionName") or data.get("collection") or data.get("collectionId"),
                "collectionId": data.get("collectionId"),
                "raw": data,
            }
            return info
    except Exception:
        pass

    # Fallback: Atlas entity (may not include collection, but return raw for debugging)
    r2 = _atlas_get(f"/datamap/api/atlas/v2/entity/guid/{guid}")
    if r2.status_code == 200:
        try:
            data2 = r2.json()
        except Exception:
            data2 = {}
        return {"collectionName": None, "collectionId": None, "raw": data2}
    return None


def assert_entity_in_collection(guid: str, expected_collection_name: str) -> bool:
    """Return True if the entity appears to belong to expected_collection_name.
    We check several possible shapes: direct `collectionName`, `collectionId` (string name in some tenants),
    or nested under a `collections` field.
    """
    info = get_entity_collection_info(guid)
    if not info:
        return False
    data = info.get("raw") or {}

    # direct name/id checks
    for key in ("collectionName", "collection", "collectionId"):
        val = data.get(key)
        if isinstance(val, str) and val == expected_collection_name:
            return True

    # Some responses include a list of collections
    collections = data.get("collections") or data.get("collectionHierarchy")
    if isinstance(collections, list):
        for c in collections:
            if isinstance(c, str) and c == expected_collection_name:
                return True
            if isinstance(c, dict):
                name = c.get("name") or c.get("collectionName") or c.get("referenceName")
                if name == expected_collection_name:
                    return True
    return False

def list_existing_tag_qns_for_tenant(tenant: str) -> Set[str]:
    # Advanced search by attribute
    payload = {
        "typeName": CUSTOM_TYPE_NAME,
        "excludeDeletedEntities": True,
        "where": {"attributeName": "dxrTenant", "operator": "eq", "attributeValue": tenant},
        "attributes": ["qualifiedName"],
        "limit": 1000,
    }
    qns: Set[str] = set()
    resp = _atlas_post("/datamap/api/atlas/v2/search/advanced", payload)
    if resp.status_code == 200:
        try:
            data = resp.json()
            for ent in data.get("entities", []) or []:
                qn = ent.get("attributes", {}).get("qualifiedName")
                if qn:
                    qns.add(qn)
        except Exception:
            pass
        return qns
    # Fallback: empty
    return qns

def delete_entities_by_qualified_names(qns: List[str], type_name: str) -> None:
    if not qns:
        return
    # Resolve QN -> GUID and delete in batches
    batch = 50
    to_delete_guids: List[str] = []
    for i in range(0, len(qns), batch):
        chunk = qns[i:i+batch]
        for qn in chunk:
            path = f"/datamap/api/atlas/v2/entity/uniqueAttribute/type/{type_name}"
            resp = _atlas_get(f"{path}", params={"attr:qualifiedName": qn})
            if resp.status_code == 200:
                try:
                    data = resp.json() or {}
                except Exception:
                    data = {}
                # Try common shapes
                guid = data.get("guid") or data.get("entity", {}).get("guid")
                if guid:
                    to_delete_guids.append(str(guid))
                else:
                    logger.debug("delete_entities_by_qualified_names: no guid found for QN %s (type %s)", qn, type_name)
    # Bulk delete by GUIDs
    for i in range(0, len(to_delete_guids), batch):
        chunk = to_delete_guids[i:i+batch]
        chunk_ids = [str(g) for g in chunk if g]
        if not chunk_ids:
            continue
        r = _atlas_delete("/datamap/api/atlas/v2/entity/bulk", params={"guid": ",".join(chunk_ids)})
        if r.status_code not in (200, 204):
            logger.warning("Bulk delete returned %s: %s", r.status_code, r.text[:300])


# ------------ Main loop ------------

def run_once() -> None:
    client = get_purview_client()
    ensure_types_and_relationships()
    tags = get_dxr_tags()
    name_map, ds_full = get_dxr_searchable_datasources()
    logger.info("Fetched %d tags from DXR", len(tags))
    logger.info("Fetched %d datasources from DXR", len(ds_full))

    tenant = os.getenv("DXR_TENANT", "default")

    if not DISABLE_GOVERNANCE:
        # Ensure collections: either under explicit parent collection id, or under a parent derived from the domain name
        child_collections = ensure_collections(PURVIEW_DOMAIN_NAME, ds_full)
        # Ensure a single 'Unstructured Datasets' collection under the same parent
        parent_ref = PURVIEW_PARENT_COLLECTION_ID or PURVIEW_DOMAIN_ID or _slug(PURVIEW_DOMAIN_NAME)
        ud_collection = ensure_unstructured_datasets_collection(parent_ref)
    else:
        child_collections = {}
        ud_collection = os.environ.get("PURVIEW_COLLECTION_ID")

    # Upsert datasources
    # If we have child collections, group datasources by their collection and write directly into those collections
    if not DISABLE_GOVERNANCE and child_collections:
        grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for item in ds_full:
            dsid = str(item.get("id"))
            coll = child_collections.get(dsid)
            if coll:
                grouped[coll].append(item)
        ds_guid_map = upsert_unstructured_datasources(client, tenant, ds_full, by_collection=grouped)
    else:
        ds_guid_map = upsert_unstructured_datasources(client, tenant, ds_full)

    # Datasources are written directly into their target collections via collection_id

    # Upsert tag assets
    tag_guid_map = upsert_unstructured_datasets(client, tags, name_map, collection_id=ud_collection)

    # Move tags to target collections if governance is enabled
    # Tags are written into the UD collection; no replication under datasource collections

    # Relationships: datasource -> tag
    for t in tags:
        qn_tag = _to_qualified_name(t)
        ds_ids = set()
        for dto in (t.get("savedQueryDtoList") or []):
            for dsid in (dto.get("datasourceIds") or []):
                ds_ids.add(str(dsid))
        for dsid in ds_ids:
            qn_ds = _qn_datasource(tenant, dsid)
            create_relationship("unstructured_datasource_has_unstructured_dataset", qn_ds, "unstructured_datasource", qn_tag, "unstructured_dataset")

    # Optional delete sync
    if SYNC_DELETE:
        incoming = {_to_qualified_name(t) for t in tags}
        existing = list_existing_tag_qns_for_tenant(tenant)
        stale = existing - incoming
        if stale:
            delete_entities_by_qualified_names(list(stale), CUSTOM_TYPE_NAME)


def entrypoint() -> None:
    """Run once if RUN_ONCE env var is set (for CI/tests), otherwise loop."""
    if os.getenv("RUN_ONCE"):
        run_once()
        return
    main()


def main() -> None:
    logger.info("Starting DXR→Purview sync loop (every %s seconds)", POLL_SECONDS)
    while True:
        try:
            run_once()
        except Exception as exc:  # noqa: BLE001 - log and continue polling
            logger.exception("Cycle failed: %s", exc)
        finally:
            time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    entrypoint()
