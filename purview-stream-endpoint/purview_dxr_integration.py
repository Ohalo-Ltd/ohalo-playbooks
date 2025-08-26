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
from pvlib import stats as pvstats
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
ENABLE_LABEL_STATS = os.getenv("ENABLE_LABEL_STATS", "1").lower() in ("1", "true", "yes")
# Use a large default so we don't miss labels; can override via env
LABEL_STATS_LIMIT = int(os.getenv("LABEL_STATS_LIMIT", "100000"))

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

# Keep legacy env wiring; modules also read directly from env
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




def get_dxr_tags() -> List[Dict[str, Any]]:
    return pvdxr.get_dxr_tags()


def get_dxr_searchable_datasources() -> Tuple[Dict[str, str], List[Dict[str, Any]]]:
    return pvdxr.get_dxr_searchable_datasources()

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

# ------------ DXR Label Statistics (per-datasource) ------------

def get_label_statistics_for_datasource(dsid: str, limit: int) -> List[Dict[str, Any]]:
    url = f"{DXR_APP_URL}/api/dashboard/label-statistics?datasources={dsid}&limit={limit}"
    headers = {
        "Authorization": f"Bearer {DXR_PAT_TOKEN}",
        "Accept": "application/json, text/plain, */*",
        "Referer": DXR_APP_URL,
    }
    try:
        resp = HTTP.get(url, headers=headers, timeout=HTTP_TIMEOUT_SECONDS)
    except Exception as e:
        logger.error("Label stats request failed for ds=%s: %s", dsid, e)
        return []
    if resp.status_code >= 400:
        logger.warning("Label stats HTTP %s for ds=%s: %s", resp.status_code, dsid, resp.text[:200])
        return []
    try:
        data = resp.json()
        return data if isinstance(data, list) else []
    except Exception:
        return []

def update_unstructured_datasource_hit_stats(client: DataMapClient, tenant: str, ds_items: List[Dict[str, Any]], child_collections: Dict[str, str] | None = None) -> None:
    if not ds_items:
        return
    # Build updates per collection for efficient upsert
    per_coll: Dict[str, List[AtlasEntity]] = defaultdict(list)
    now_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    for item in ds_items:
        dsid = str(item.get("id"))
        qn = _qn_datasource(tenant, dsid)
        stats = get_label_statistics_for_datasource(dsid, LABEL_STATS_LIMIT)
        # Keep only labels with documentCount > 0
        lbl_ids: List[str] = []
        lbl_names: List[str] = []
        lbl_counts: List[int] = []
        for row in stats or []:
            try:
                cnt = int(row.get("documentCount", 0))
            except Exception:
                cnt = 0
            if cnt > 0:
                lbl_ids.append(str(row.get("labelId")))
                lbl_names.append(str(row.get("labelName", "")))
                lbl_counts.append(cnt)
        attrs = {
            "qualifiedName": qn,
            "hitLabelIds": lbl_ids,
            "hitLabelNames": lbl_names,
            "hitDocumentCounts": lbl_counts,
            "statsUpdatedAt": now_iso,
        }
        per_coll[(child_collections or {}).get(dsid) or os.environ.get("PURVIEW_COLLECTION_ID") or ""].append(
            AtlasEntity(type_name="unstructured_datasource", attributes=attrs)
        )

    # Submit per collection (empty key means no explicit collection_id)
    for coll, entities in per_coll.items():
        if not entities:
            continue
        payload = AtlasEntitiesWithExtInfo(entities=entities)
        try:
            if coll:
                client.entity.batch_create_or_update(payload, collection_id=coll)
            else:
                client.entity.batch_create_or_update(payload)
        except Exception as e:
            logger.warning("Update hit stats upsert failed for collection=%s: %s", coll or "(none)", e)

def _fetch_label_stats(ds_items: List[Dict[str, Any]]) -> tuple[dict[str, list[dict[str, Any]]], dict[str, dict[str, Any]]]:
    """Fetch label statistics per datasource and build reverse map per label.
    Returns (per_ds_hits, per_label_hits) where:
      per_ds_hits[dsid] = list of rows {labelId,labelName,documentCount}
      per_label_hits[labelId] = { 'labelName': str, 'by_ds': { dsid: count, ... } }
    """
    per_ds: dict[str, list[dict[str, Any]]] = {}
    per_label: dict[str, dict[str, Any]] = {}
    for item in ds_items or []:
        dsid = str(item.get("id"))
        rows = get_label_statistics_for_datasource(dsid, LABEL_STATS_LIMIT)
        hits = []
        for row in rows or []:
            try:
                cnt = int(row.get("documentCount", 0))
            except Exception:
                cnt = 0
            if cnt <= 0:
                continue
            lid = str(row.get("labelId"))
            lname = str(row.get("labelName", ""))
            hits.append({"labelId": lid, "labelName": lname, "documentCount": cnt})
            pl = per_label.setdefault(lid, {"labelName": lname, "by_ds": {}})
            pl["by_ds"][dsid] = cnt
        per_ds[dsid] = hits
    return per_ds, per_label

def update_unstructured_dataset_hit_stats_and_relationships(client: DataMapClient, tags: List[Dict[str, Any]], ds_items: List[Dict[str, Any]], ds_name_map: Dict[str, str], ud_collection: str | None) -> None:
    """Update label-side hit stats and ensure relationships only for datasources with hits.
    Removes relationships that no longer have hits. Ensures minimal entities exist before linking."""
    if not tags:
        return
    # Ensure types/relationships exist (idempotent)
    try:
        ensure_types_and_relationships()
    except Exception:
        pass
    per_ds, per_label = _fetch_label_stats(ds_items)
    now_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    # Build upserts for label entities
    entities: List[AtlasEntity] = []
    for tag in tags:
        tag_id = str(tag.get("id"))
        qn = _to_qualified_name(tag)
        inv = per_label.get(tag_id) or {"by_ds": {}, "labelName": tag.get("name", "")}
        dsids = list(inv["by_ds"].keys())
        counts = [inv["by_ds"][d] for d in dsids]
        dsnames = [ds_name_map.get(d, "") for d in dsids]
        attrs = {
            "qualifiedName": qn,
            # Include required attributes to allow create-if-missing
            "name": str(tag.get("name") or tag_id),
            "type": str(tag.get("type") or "SMART"),
            "dxrTenant": str(tag.get("tenant") or os.getenv("DXR_TENANT", "default")),
            "tagId": tag_id,
            "hitDatasourceIds": dsids,
            "hitDatasourceNames": dsnames,
            "hitDocumentCounts": counts,
            "statsUpdatedAt": now_iso,
        }
        entities.append(AtlasEntity(type_name=CUSTOM_TYPE_NAME, attributes=attrs))

    if entities:
        payload = AtlasEntitiesWithExtInfo(entities=entities)
        try:
            if ud_collection:
                client.entity.batch_create_or_update(payload, collection_id=ud_collection)
            else:
                client.entity.batch_create_or_update(payload)
        except Exception as e:
            logger.warning("Label-side stats upsert failed: %s", e)

    # Ensure datasource entities exist minimally for linkage
    try:
        tenant = os.getenv("DXR_TENANT", "default")
        if ds_items:
            # Ensure datasource entities exist; write into UD collection if no explicit collection is available
            upsert_unstructured_datasources(client, tenant, ds_items, collection_id=ud_collection)
    except Exception as e:
        logger.debug("Ensuring datasource entities before linking failed: %s", e)

    # Relationship management per label: create/update for hits; delete absent
    tenant = os.getenv("DXR_TENANT", "default")
    for tag in tags:
        tag_id = str(tag.get("id"))
        qn_tag = _to_qualified_name(tag)
        tag_guid = resolve_guid_by_qn(CUSTOM_TYPE_NAME, qn_tag)
        if not tag_guid:
            continue
        current = per_label.get(tag_id, {}).get("by_ds", {})
        existing = _get_entity_relationship_map_for_label(tag_guid)
        # Create or update relationships for current hits
        dsid_to_guid = {}
        for dsid, cnt in current.items():
            ds_qn = _qn_datasource(tenant, dsid)
            guid = resolve_guid_by_qn("unstructured_datasource", ds_qn)
            dsid_to_guid[dsid] = guid
            if not guid:
                continue
            attrs = {"documentCount": int(cnt), "statsUpdatedAt": now_iso}
            _create_or_update_relationship_with_attrs(
                "unstructured_datasource_hits_unstructured_dataset",
                ds_qn, "unstructured_datasource",
                qn_tag, CUSTOM_TYPE_NAME,
                attrs,
            )
        # Delete relationships for datasources no longer in current hits
        keep_guids = {g for g in dsid_to_guid.values() if g}
        for dsg, relg in existing.items():
            if dsg not in keep_guids:
                # Verify relationship type before deletion
                rr = _atlas_get(f"/datamap/api/atlas/v2/relationship/guid/{relg}")
                if rr.status_code == 200 and (rr.json() or {}).get("typeName") == "unstructured_datasource_hits_unstructured_dataset":
                    _delete_relationship_by_guid(relg)

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

def _atlas_put(path: str, payload: Dict[str, Any]) -> requests.Response:
    url = f"{PURVIEW_ENDPOINT}{path}"
    return requests.put(url, headers=_atlas_headers(), json=payload, timeout=HTTP_TIMEOUT_SECONDS)

def _get_entity_relationship_map_for_label(label_guid: str) -> Dict[str, str]:
    """Return mapping of related datasource GUID -> relationship GUID for a given label entity.
    Best-effort parsing of Atlas entity payload."""
    rels: Dict[str, str] = {}
    try:
        r = _atlas_get(f"/datamap/api/atlas/v2/entity/guid/{label_guid}")
        if r.status_code != 200:
            return rels
        data = r.json() or {}
        ra = data.get("relationshipAttributes") or {}
        # end name could be 'datasource', 'datasources', or 'hitDatasources' depending on typedefs
        ds_rel = ra.get("hitDatasources") or ra.get("datasources") or ra.get("datasource")
        items = []
        if isinstance(ds_rel, list):
            items = ds_rel
        elif isinstance(ds_rel, dict):
            items = [ds_rel]
        for it in items:
            ds_guid = it.get("guid") or (it.get("entity") or {}).get("guid")
            rel_guid = it.get("relationshipGuid") or it.get("relationshipId") or (it.get("relationship") or {}).get("guid")
            if ds_guid and rel_guid:
                rels[str(ds_guid)] = str(rel_guid)
    except Exception:
        return rels
    return rels

def _create_or_update_relationship_with_attrs(rel_type: str, end1_qn: str, end1_type: str, end2_qn: str, end2_type: str, attrs: Dict[str, Any]) -> None:
    payload = {
        "typeName": rel_type,
        "attributes": attrs,
        "end1": {"typeName": end1_type, "uniqueAttributes": {"qualifiedName": end1_qn}},
        "end2": {"typeName": end2_type, "uniqueAttributes": {"qualifiedName": end2_qn}},
    }
    resp = _atlas_post("/datamap/api/atlas/v2/relationship", payload)
    if resp.status_code in (200, 201):
        return
    if resp.status_code == 409:
        # Find existing relationship and update attributes
        label_guid = resolve_guid_by_qn(end2_type, end2_qn) if end2_type == "unstructured_dataset" else resolve_guid_by_qn(end1_type, end1_qn)
        if not label_guid:
            return
        rels = _get_entity_relationship_map_for_label(label_guid)
        # Resolve datasource guid to pick correct relationship
        ds_qn = end1_qn if end1_type == "unstructured_datasource" else end2_qn
        ds_guid = resolve_guid_by_qn("unstructured_datasource", ds_qn)
        if not ds_guid:
            return
        rel_guid = rels.get(ds_guid)
        if not rel_guid:
            return
        # GET existing, ensure type matches, then update attributes
        r = _atlas_get(f"/datamap/api/atlas/v2/relationship/guid/{rel_guid}")
        if r.status_code != 200:
            return
        try:
            rel_obj = r.json() or {}
        except Exception:
            rel_obj = {}
        if rel_obj.get("typeName") and rel_obj.get("typeName") != rel_type:
            return
        rel_obj["attributes"] = rel_obj.get("attributes", {}) | attrs
        pr = _atlas_put(f"/datamap/api/atlas/v2/relationship/guid/{rel_guid}", rel_obj)
        if pr.status_code not in (200, 201):
            logger.debug("Relationship attribute update failed: %s %s", pr.status_code, pr.text[:200])
        return
    logger.debug("Relationship create failed (%s): %s", resp.status_code, resp.text[:200])

def _delete_relationship_by_guid(rel_guid: str) -> None:
    try:
        r = _atlas_delete(f"/datamap/api/atlas/v2/relationship/guid/{rel_guid}")
        if r.status_code not in (200, 204):
            logger.debug("Delete relationship %s -> %s %s", rel_guid, r.status_code, r.text[:200])
    except Exception:
        pass


def build_entities_payload_for_tags(tags: List[Dict[str, Any]], ds_map: Dict[str, str]) -> AtlasEntitiesWithExtInfo:
    entities: List[AtlasEntity] = []
    dxr_tenant = os.environ.get("DXR_TENANT", "default")
    for tag in tags:
        qn = _to_qualified_name(tag)
        tag_id = str(tag.get("id"))
        # Some tenants may not return 'name' consistently; provide a sensible fallback
        name = tag.get("name") or tag.get("labelName") or f"dxr_label_{tag_id}"
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

# ---------- Entity collection helpers ----------
ENTITY_API_VERSION = "2023-09-01"

def _entity_get(path: str, params: Dict[str, Any] | None = None) -> requests.Response:
    url = f"{PURVIEW_ENDPOINT}{path}"
    headers = _atlas_headers()
    return requests.get(url, headers=headers, params=params, timeout=HTTP_TIMEOUT_SECONDS)


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
    """Return True if the entity appears to belong to the expected collection.
    Accepts either a collection referenceName (e.g., sdaqff) or a friendlyName (e.g., Unstructured Datasets).
    We check several possible shapes in the entity payload and also normalize expected by resolving the
    friendlyName from the account-host collections API when possible.
    """
    # Build accepted names for comparison: provided value and its friendlyName (if resolvable)
    expected = set([expected_collection_name])
    try:
        r = _acct_collections_get(expected_collection_name)
        if r.status_code == 200:
            try:
                data = r.json() or {}
                fn = data.get("friendlyName")
                if isinstance(fn, str) and fn:
                    expected.add(fn)
            except Exception:
                pass
    except Exception:
        pass

    info = get_entity_collection_info(guid)
    if not info:
        return False
    data = info.get("raw") or {}

    # direct name/id checks
    for key in ("collectionName", "collection", "collectionId"):
        val = data.get(key)
        if isinstance(val, str) and val in expected:
            return True

    # Some responses include a list of collections
    collections = data.get("collections") or data.get("collectionHierarchy")
    if isinstance(collections, list):
        for c in collections:
            if isinstance(c, str) and c == expected_collection_name:
                return True
            if isinstance(c, dict):
                name = c.get("name") or c.get("collectionName") or c.get("referenceName")
                if name in expected:
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
    return pvatlas.delete_entities_by_qualified_names(qns, type_name)


# ------------ Main loop ------------

def run_once() -> None:
    client = get_purview_client()
    ensure_types_and_relationships()
    tags = get_dxr_tags()
    name_map, ds_full = get_dxr_searchable_datasources()
    # Optional limiting for quicker test cycles
    try:
        tlim = int(os.getenv("DXR_TAGS_LIMIT", "0"))
        if tlim > 0:
            tags = tags[:tlim]
    except Exception:
        pass
    try:
        dlim = int(os.getenv("DXR_DATASOURCES_LIMIT", "0"))
        if dlim > 0:
            ds_full = ds_full[:dlim]
    except Exception:
        pass
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

    # Stats-driven enrichment
    if ENABLE_LABEL_STATS:
        # 1) Update datasource-side stats arrays
        try:
            update_unstructured_datasource_hit_stats(client, tenant, ds_full, child_collections)
        except Exception as e:
            logger.warning("Update datasource hit stats failed: %s", e)
        # 2) Update label-side stats arrays and ensure relationships only where hits exist
        try:
            update_unstructured_dataset_hit_stats_and_relationships(client, tags, ds_full, name_map, ud_collection)
        except Exception as e:
            logger.warning("Update dataset hit stats/relationships failed: %s", e)
    else:
        # Legacy relationship creation based on savedQueryDtoList
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

# ---- pvlib wiring aliases (override in-module implementations) ----
# These assignments ensure external callers/tests use the refactored implementations.
update_unstructured_datasource_hit_stats = pvstats.update_unstructured_datasource_hit_stats
update_unstructured_dataset_hit_stats_and_relationships = pvstats.update_unstructured_dataset_hit_stats_and_relationships
create_relationship = pvrels.create_relationship
_get_entity_relationship_map_for_label = pvrels._get_entity_relationship_map_for_label
_create_or_update_relationship_with_attrs = pvrels._create_or_update_relationship_with_attrs
_delete_relationship_by_guid = pvrels._delete_relationship_by_guid
upsert_unstructured_datasets = pvent.upsert_unstructured_datasets
upsert_unstructured_datasources = pvent.upsert_unstructured_datasources
resolve_guid_by_qn = pvatlas.resolve_guid_by_qn
delete_entities_by_qualified_names = pvatlas.delete_entities_by_qualified_names
