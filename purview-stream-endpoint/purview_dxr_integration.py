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
  DXR_CLASSIFICATIONS_PATH  (optional) default: /api/vbeta/classifications
  DXR_FILES_PATH            (optional) default: /api/vbeta/files

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
import re
import logging
from typing import Dict, Any, List
from typing import Tuple, Set
from collections import defaultdict

import requests
from azure.purview.datamap import DataMapClient
from azure.purview.datamap.models import (
    AtlasEntity,
    AtlasEntitiesWithExtInfo,
)

from dotenv import load_dotenv

# New modular helpers
from pvlib import config as pvconfig
from pvlib import dxr as pvdxr
from pvlib import atlas as pvatlas
from pvlib import typedefs as pvtypes
from pvlib import entities as pvent
from pvlib import relationships as pvrels
from pvlib import governance as pvgo
from pvlib import collections as pvcoll

# Load .env from current working directory (if present),
# and also from this file's directory so running from repo root works
try:
    _HERE = os.path.dirname(os.path.abspath(__file__))
    # First load any process-level .env (cwd), then overlay with folder-local
    load_dotenv()
    load_dotenv(os.path.join(_HERE, ".env"))
except Exception:
    # If dotenv is unavailable or any path issues occur, proceed with OS env only
    pass

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

# Reuse shared session/timeouts and normalizers from pvlib.config
HTTP = pvconfig.HTTP
HTTP_TIMEOUT_SECONDS = pvconfig.HTTP_TIMEOUT_SECONDS

POLL_SECONDS = int(os.getenv("POLL_SECONDS", "60"))
SYNC_DELETE = os.getenv("SYNC_DELETE", "0").lower() in ("1", "true", "yes")
DISABLE_GOVERNANCE = os.getenv("DISABLE_GOVERNANCE", "0").lower() in ("1", "true", "yes")

# ------------ Governance (Catalog Admin) API helpers ------------
GOV_API_VERSION = "2023-09-01"

_governance_base = pvgo.governance_base

_governance_headers = pvgo.governance_headers

# ------------ Collections API helpers (governance vs account-host fallback) ------------

_gov_collections_put = pvgo.gov_collections_put

_gov_collections_get = pvgo.gov_collections_get

_acct_collections_put = pvgo.acct_collections_put
_acct_collections_get = pvgo.acct_collections_get

_normalize_purview_endpoint = pvconfig.normalize_purview_endpoint
_normalize_base_url = pvconfig.normalize_base_url

# DXR env normalization helpers (kept for convenience in this module)
DXR_APP_URL = _normalize_base_url(os.environ.get("DXR_APP_URL"))
DXR_PAT_TOKEN = os.environ.get("DXR_PAT_TOKEN")

PURVIEW_ENDPOINT = _normalize_purview_endpoint(os.environ.get("PURVIEW_ENDPOINT"))
TENANT_ID = os.environ.get("AZURE_TENANT_ID")
CLIENT_ID = os.environ.get("AZURE_CLIENT_ID")
CLIENT_SECRET = os.environ.get("AZURE_CLIENT_SECRET")
PURVIEW_COLLECTION_ID = os.environ.get("PURVIEW_COLLECTION_ID")  # optional (default write target for reduced-permissions mode)
PURVIEW_DOMAIN_ID = os.environ.get("PURVIEW_DOMAIN_ID")  # optional: domain referenceName (alias for parent selection)
PURVIEW_PARENT_COLLECTION_ID = os.environ.get("PURVIEW_PARENT_COLLECTION_ID")  # optional: parent refName under which to create child collections
UNSTRUCTURED_DATASETS_COLLECTION_NAME = os.environ.get("UNSTRUCTURED_DATASETS_COLLECTION_NAME", "Unstructured Datasets")
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

# HTTP session and timeout provided by pvlib.config

def _get_access_token() -> str:
    # Delegate to pvlib.atlas
    return pvatlas._get_access_token()

# ------------ Atlas REST helpers ------------
def _atlas_headers() -> Dict[str, str]:
    return pvatlas._atlas_headers()

def _atlas_get(path: str, params: Dict[str, Any] | None = None) -> requests.Response:
    return pvatlas._atlas_get(path, params)

def _atlas_post(path: str, payload: Dict[str, Any]) -> requests.Response:
    return pvatlas._atlas_post(path, payload)

def _atlas_delete(path: str, params: Dict[str, Any] | None = None) -> requests.Response:
    return pvatlas._atlas_delete(path, params)




def get_dxr_classifications() -> List[Dict[str, Any]]:
    return pvdxr.get_dxr_classifications()


def list_file_datasources_for_label(label_id: str, label_name: str | None = None) -> Tuple[Dict[str, str], List[Dict[str, Any]]]:
    return pvdxr.list_file_datasources_for_label(label_id, label_name)

# Backwards-compat small aliases for legacy tests/helpers
    

# ------------ Purview (Data Map) ------------

def get_purview_client() -> DataMapClient:
    return pvatlas.get_purview_client()


CUSTOM_TYPE_NAME = pvtypes.CUSTOM_TYPE_NAME

def ensure_types_and_relationships() -> None:
    pvtypes.ensure_types_and_relationships()


def _to_qualified_name(tag: Dict[str, Any]) -> str:
    tenant = tag.get("tenant") or os.environ.get("DXR_TENANT", "default")
    tag_id = str(tag.get("id"))
    return f"dxrtag://{tenant}/{tag_id}"

def _qn_datasource(tenant: str, dsid: str) -> str:
    return f"dxrds://{tenant}/{dsid}"
_account_get = pvgo.account_get

_account_put = pvgo.account_put

_account_delete = pvgo.account_delete

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

_normalize_collection_ref = pvcoll._normalize_collection_ref

def ensure_collections(domain_name: str, datasource_items: List[Dict[str, Any]]) -> Dict[str, str]:
    return pvcoll.ensure_collections(domain_name, datasource_items)


# UD collection ref is derived in pvlib.collections

def ensure_unstructured_datasets_collection(parent_reference: str) -> str:
    return pvcoll.ensure_unstructured_datasets_collection(parent_reference, UNSTRUCTURED_DATASETS_COLLECTION_NAME)

    

## Removed local relationship helpers; using pvlib.relationships


    


def upsert_unstructured_datasets(client: DataMapClient, tags: List[Dict[str, Any]], ds_map: Dict[str, str], *, collection_id: str | None = None) -> Dict[str, str]:
    return pvent.upsert_unstructured_datasets(client, tags, ds_map, collection_id=collection_id)

def upsert_unstructured_datasources(
    client: DataMapClient,
    tenant: str,
    ds_full: List[Dict[str, Any]],
    *,
    by_collection: Dict[str, List[Dict[str, Any]]] | None = None,
    collection_id: str | None = None,
) -> Dict[str, str]:
    return pvent.upsert_unstructured_datasources(client, tenant, ds_full, by_collection=by_collection, collection_id=collection_id)

def resolve_guid_by_qn(type_name: str, qualified_name: str, attempts: int = 8, delay_seconds: float = 1.5) -> str | None:
    return pvatlas.resolve_guid_by_qn(type_name, qualified_name, attempts, delay_seconds)

## Removed local collection helpers; use pvlib.atlas versions if needed

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

## Removed local delete_entities_by_qualified_names; using pvlib.atlas alias at bottom


## Removed local retry; use pvlib.relationships.create_relationship_retry

def _link_label_to_datasource(qn_tag: str, qn_ds: str) -> None:
    # Create the standard 'has' relationship — visible from both sides and widely supported
    ok_has = pvrels.create_relationship_retry(
        "unstructured_datasource_has_unstructured_dataset",
        qn_ds,
        "unstructured_datasource",
        qn_tag,
        "unstructured_dataset",
        attempts=6,
        delay_seconds=1.0,
    )


# ------------ Main loop ------------

def run_once() -> None:
    client = get_purview_client()
    ensure_types_and_relationships()
    classifications = get_dxr_classifications()
    # Only keep labels; ignore annotators and other types
    labels = [c for c in (classifications or []) if str(c.get("type")).upper() == "LABEL"]
    # Optional limiting for quicker test cycles
    try:
        tlim = int(os.getenv("DXR_TAGS_LIMIT", "0"))
        if tlim > 0:
            labels = labels[:tlim]
    except Exception:
        pass
    logger.info("Fetched %d classifications; %d labels after filter", len(classifications), len(labels))

    tenant = os.getenv("DXR_TENANT", "default")

    # Discover datasources with hits per classification via files API
    all_ds_by_id: Dict[str, Dict[str, Any]] = {}
    ds_name_map: Dict[str, str] = {}
    label_to_dsids: Dict[str, Set[str]] = {}
    for c in labels:
        lid = str(c.get("id"))
        name_map, ds_items = list_file_datasources_for_label(lid, c.get("name"))
        logger.info("Label %s (%s): discovered %d datasources with hits", c.get("name", lid), lid, len(ds_items))
        ds_name_map.update(name_map)
        label_to_dsids[lid] = set()
        for it in ds_items:
            dsid = str(it.get("id"))
            all_ds_by_id[dsid] = it
            label_to_dsids[lid].add(dsid)

    ds_full = list(all_ds_by_id.values())
    logger.info("Resolved %d unique datasources across labels", len(ds_full))

    parent_ref = PURVIEW_PARENT_COLLECTION_ID or PURVIEW_DOMAIN_ID or _slug(PURVIEW_DOMAIN_NAME)
    # Ensure UD collection under parent_ref when possible
    if not DISABLE_GOVERNANCE:
        try:
            ud_collection = ensure_unstructured_datasets_collection(parent_ref)
        except Exception as e:
            logger.warning("Unable to ensure UD collection under parent %s: %s", parent_ref, e)
            ud_collection = os.environ.get("PURVIEW_COLLECTION_ID")
    else:
        ud_collection = os.environ.get("PURVIEW_COLLECTION_ID")

    # Upsert datasource entities into the DXR instance parent if provided; otherwise fallback
    if ds_full:
        if PURVIEW_PARENT_COLLECTION_ID:
            logger.info("Upserting %d datasources into parent collection %s", len(ds_full), parent_ref)
            upsert_unstructured_datasources(client, tenant, ds_full, collection_id=parent_ref)
        else:
            if not DISABLE_GOVERNANCE and PURVIEW_DOMAIN_ID:
                child_collections = ensure_collections(PURVIEW_DOMAIN_NAME, ds_full)
                grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
                for item in ds_full:
                    dsid = str(item.get("id"))
                    coll = child_collections.get(dsid)
                    if coll:
                        grouped[coll].append(item)
                logger.info("Ensuring %d child collections; will upsert %d datasources", len(child_collections), sum(len(v) for v in grouped.values()))
                upsert_unstructured_datasources(client, tenant, ds_full, by_collection=grouped)
            else:
                logger.info("Upserting %d datasources into fallback collection %s", len(ds_full), (ud_collection or os.environ.get("PURVIEW_COLLECTION_ID") or "(none)"))
                upsert_unstructured_datasources(client, tenant, ds_full, collection_id=ud_collection or os.environ.get("PURVIEW_COLLECTION_ID"))
    else:
        logger.info("No datasource hits discovered; skipping datasource upsert")

    # Upsert labels into the Unstructured Datasets collection
    if ud_collection:
        upsert_unstructured_datasets(client, labels, ds_name_map, collection_id=ud_collection)
    else:
        logger.warning("No UD or explicit collection found; attempting label upsert without collection (may fail)")
        upsert_unstructured_datasets(client, labels, ds_name_map)

    # (Legacy removed) We no longer enrich labels with hitDatasource* arrays.

    # Create relationships (datasource -> label)
    # Choose a collection context for lineage processes
    lineage_collection = ud_collection or parent_ref or os.environ.get("PURVIEW_COLLECTION_ID")
    for c in labels:
        qn_tag = _to_qualified_name(c)
        lid = str(c.get("id"))
        for dsid in label_to_dsids.get(lid, set()):
            qn_ds = _qn_datasource(tenant, dsid)
            _link_label_to_datasource(qn_tag, qn_ds)
            try:
                # Prefer SDK with collection context to avoid 403 on Process create
                ds_name = ds_name_map.get(dsid, dsid)
                label_name = c.get("name") or lid
                label_desc = c.get("description") or ""
                proc_name = f"DXR processing: {ds_name} -> {label_name}"
                # Build description with DXR context and label metadata
                proc_desc = label_desc or f"DXR processing that derived label '{label_name}' from datasource '{ds_name}' (id {dsid})."
                # Append optional metadata if available
                meta_lines: List[str] = []
                ltype = c.get("type") or "LABEL"
                lsub = c.get("subtype") or ""
                if ltype:
                    meta_lines.append(f"Label type: {ltype}")
                if lsub:
                    meta_lines.append(f"Label subtype: {lsub}")
                created_at = c.get("createdAt")
                updated_at = c.get("updatedAt")
                if created_at:
                    meta_lines.append(f"Created at: {created_at}")
                if updated_at:
                    meta_lines.append(f"Updated at: {updated_at}")
                search_link_rel = c.get("searchLink") or ""
                if search_link_rel:
                    full_search_url = f"{DXR_APP_URL.rstrip('/')}{search_link_rel}"
                    meta_lines.append(f"DXR Search: {full_search_url}")
                if meta_lines:
                    proc_desc = f"{proc_desc}\n" + "\n".join(meta_lines)
                # Connector type from datasource item, if available
                _ds_item = all_ds_by_id.get(dsid, {})
                connector_type_name = _ds_item.get("connectorTypeName") or _ds_item.get("connectorType") or None
                connector_id = _ds_item.get("connectorId")
                connector_name = _ds_item.get("connectorName")
                pvrels.create_lineage_with_client(
                    client,
                    qn_ds,
                    qn_tag,
                    collection_id=lineage_collection,
                    process_name=proc_name,
                    process_description=proc_desc,
                    process_search_link=(full_search_url if search_link_rel else None),
                    label_type=(c.get("type") or "LABEL"),
                    label_subtype=(c.get("subtype") or None),
                    created_at=c.get("createdAt"),
                    updated_at=c.get("updatedAt"),
                    datasource_id=dsid,
                    datasource_name=ds_name,
                    connector_type_name=connector_type_name,
                    connector_id=connector_id,
                    connector_name=connector_name,
                )
            except Exception as e:
                logger.debug("Lineage creation failed for %s -> %s: %s", qn_ds, qn_tag, e)

    # Optional delete sync
    if SYNC_DELETE:
        incoming = {_to_qualified_name(t) for t in labels}
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

# ---- pvlib wiring aliases (override in-module implementations) ----
create_relationship = pvrels.create_relationship
_get_entity_relationship_map_for_label = pvrels._get_entity_relationship_map_for_label
_create_or_update_relationship_with_attrs = pvrels._create_or_update_relationship_with_attrs
_delete_relationship_by_guid = pvrels._delete_relationship_by_guid
upsert_unstructured_datasets = pvent.upsert_unstructured_datasets
upsert_unstructured_datasources = pvent.upsert_unstructured_datasources
resolve_guid_by_qn = pvatlas.resolve_guid_by_qn
delete_entities_by_qualified_names = pvatlas.delete_entities_by_qualified_names
