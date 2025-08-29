from typing import Any, Dict, List, Optional, Set
import time

import requests
from azure.identity import ClientSecretCredential
from azure.purview.datamap import DataMapClient

from .config import purview_env, HTTP_TIMEOUT_SECONDS, logger


def _get_access_token() -> str:
    env = purview_env()
    cred = ClientSecretCredential(
        tenant_id=env["AZURE_TENANT_ID"],
        client_id=env["AZURE_CLIENT_ID"],
        client_secret=env["AZURE_CLIENT_SECRET"],
    )
    return cred.get_token("https://purview.azure.net/.default").token


def _atlas_headers() -> Dict[str, str]:
    token = _get_access_token()
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def _endpoint() -> str:
    return purview_env()["PURVIEW_ENDPOINT"]


def _atlas_get(path: str, params: Optional[Dict[str, Any]] = None) -> requests.Response:
    url = f"{_endpoint()}{path}"
    return requests.get(url, headers=_atlas_headers(), params=params, timeout=HTTP_TIMEOUT_SECONDS)


def _atlas_post(path: str, payload: Dict[str, Any]) -> requests.Response:
    url = f"{_endpoint()}{path}"
    return requests.post(url, headers=_atlas_headers(), json=payload, timeout=HTTP_TIMEOUT_SECONDS)

def _atlas_put(path: str, payload: Dict[str, Any]) -> requests.Response:
    url = f"{_endpoint()}{path}"
    return requests.put(url, headers=_atlas_headers(), json=payload, timeout=HTTP_TIMEOUT_SECONDS)


def _atlas_delete(path: str, params: Optional[Dict[str, Any]] = None) -> requests.Response:
    url = f"{_endpoint()}{path}"
    return requests.delete(url, headers=_atlas_headers(), params=params, timeout=HTTP_TIMEOUT_SECONDS)


def resolve_guid_by_qn(type_name: str, qualified_name: str, attempts: int = 8, delay_seconds: float = 1.5) -> Optional[str]:
    t = (type_name or "").strip()
    qn = (qualified_name or "").strip()
    if not t or not qn:
        return None
    for _ in range(max(1, attempts)):
        resp = _atlas_get(f"/datamap/api/atlas/v2/entity/uniqueAttribute/type/{t}", params={"attr:qualifiedName": qn})
        if resp.status_code == 200:
            try:
                data = resp.json() or {}
                guid = data.get("guid") or data.get("entity", {}).get("guid")
                if guid:
                    return guid
            except Exception:
                pass
        elif resp.status_code in (401, 403):
            break
        time.sleep(delay_seconds)
    # Fallback advanced search
    payload = {
        "typeName": t,
        "excludeDeletedEntities": True,
        "where": {"attributeName": "qualifiedName", "operator": "eq", "attributeValue": qn},
        "attributes": ["qualifiedName"],
        "limit": 1,
    }
    resp2 = _atlas_post("/datamap/api/atlas/v2/search/advanced", payload)
    if resp2.status_code == 200:
        try:
            data2 = resp2.json() or {}
            for ent in data2.get("entities", []) or []:
                g = ent.get("guid") or ent.get("entity", {}).get("guid")
                if g:
                    return g
        except Exception:
            pass
    return None


ENTITY_API_VERSION = "2023-09-01"


def _entity_get(path: str, params: Optional[Dict[str, Any]] = None) -> requests.Response:
    url = f"{_endpoint()}{path}"
    return requests.get(url, headers=_atlas_headers(), params=params, timeout=HTTP_TIMEOUT_SECONDS)


def get_entity_collection_info(guid: str) -> Optional[Dict[str, Any]]:
    try:
        r = _entity_get(f"/datamap/api/entity/guid/{guid}", params={"api-version": ENTITY_API_VERSION})
        if r.status_code == 200:
            data = {}
            try:
                data = r.json() or {}
            except Exception:
                pass
            return {
                "collectionName": data.get("collectionName") or data.get("collection") or data.get("collectionId"),
                "collectionId": data.get("collectionId"),
                "raw": data,
            }
    except Exception:
        pass
    r2 = _atlas_get(f"/datamap/api/atlas/v2/entity/guid/{guid}")
    if r2.status_code == 200:
        try:
            data2 = r2.json() or {}
        except Exception:
            data2 = {}
        return {"collectionName": None, "collectionId": None, "raw": data2}
    return None


def assert_entity_in_collection(guid: str, expected_collection_name: str) -> bool:
    expected = set([expected_collection_name])
    # Best-effort: can't easily resolve friendlyName without governance here; rely on raw entity payload
    info = get_entity_collection_info(guid)
    if not info:
        return False
    data = info.get("raw") or {}
    for key in ("collectionName", "collection", "collectionId"):
        val = data.get(key)
        if isinstance(val, str) and val in expected:
            return True
    collections = data.get("collections") or data.get("collectionHierarchy")
    if isinstance(collections, list):
        for c in collections:
            if isinstance(c, str) and c in expected:
                return True
            if isinstance(c, dict):
                name = c.get("name") or c.get("collectionName") or c.get("referenceName")
                if name in expected:
                    return True
    return False


def delete_entities_by_qualified_names(qns: List[str], type_name: str) -> None:
    if not qns:
        return
    to_delete_guids: List[str] = []
    for qn in qns:
        resp = _atlas_get(f"/datamap/api/atlas/v2/entity/uniqueAttribute/type/{type_name}", params={"attr:qualifiedName": qn})
        if resp.status_code == 200:
            try:
                data = resp.json() or {}
                guid = data.get("guid") or data.get("entity", {}).get("guid")
                if guid:
                    to_delete_guids.append(str(guid))
            except Exception:
                continue
    if not to_delete_guids:
        return
    r = _atlas_delete("/datamap/api/atlas/v2/entity/bulk", params={"guid": ",".join(to_delete_guids)})
    if r.status_code not in (200, 204):
        logger.warning("Bulk delete returned %s: %s", r.status_code, r.text[:300])


def search_entity_qns_by_attribute(
    type_name: str,
    attribute_name: str,
    attribute_value: str,
    *,
    limit: int = 1000,
) -> Set[str]:
    """Return a set of qualifiedNames for the given type where attribute==value.

    Uses Atlas advanced search. Best-effort; returns an empty set on failures.
    """
    payload: Dict[str, Any] = {
        "typeName": type_name,
        "excludeDeletedEntities": True,
        "where": {
            "attributeName": attribute_name,
            "operator": "eq",
            "attributeValue": attribute_value,
        },
        "attributes": ["qualifiedName"],
        "limit": int(limit or 1000),
    }
    out: Set[str] = set()
    resp = _atlas_post("/datamap/api/atlas/v2/search/advanced", payload)
    if resp.status_code == 200:
        try:
            data = resp.json() or {}
            for ent in data.get("entities", []) or []:
                qn = (ent.get("attributes") or {}).get("qualifiedName")
                if isinstance(qn, str) and qn:
                    out.add(qn)
        except Exception:
            return out
    return out


def get_purview_client() -> DataMapClient:
    env = purview_env()
    cred = ClientSecretCredential(
        tenant_id=env["AZURE_TENANT_ID"],
        client_id=env["AZURE_CLIENT_ID"],
        client_secret=env["AZURE_CLIENT_SECRET"],
    )
    return DataMapClient(endpoint=env["PURVIEW_ENDPOINT"], credential=cred)
