from typing import Any, Dict, List
import os
import re

from . import governance as pvgo
from .config import logger


_slug_re = re.compile(r"[^a-z0-9-]+")


def _slug(name: str) -> str:
    s = (name or "").strip().lower()
    s = s.replace(" ", "-")
    s = _slug_re.sub("-", s)
    s = re.sub(r"-+", "-", s)
    s = s.strip("-") or "unnamed"
    return s[:90]


def _make_collection_ref_for_datasource(dsname: str, dsid: str, max_len: int = 36) -> str:
    base = _slug(dsname)
    id_str = str(dsid)
    if len(id_str) > max_len - 3:
        import hashlib
        id_str = hashlib.sha1(id_str.encode("utf-8")).hexdigest()[:8]
    suffix = f"-{id_str}"
    keep = max_len - len(suffix)
    base_part = (base[:keep].strip("-") or "c") if keep >= 1 else "c"
    ref = f"{base_part}{suffix}"
    return (ref + "xxx")[:3] if len(ref) < 3 else ref


def _normalize_collection_ref(raw: str, max_len: int = 36) -> str:
    ref = _slug(raw)
    if len(ref) > max_len:
        ref = ref[:max_len]
    if len(ref) < 3:
        ref = (ref + "xxx")[:3]
    return ref


def _make_ud_collection_ref(parent_reference: str, name: str = "Unstructured Datasets", max_len: int = 36) -> str:
    base = _slug(name)
    suffix = f"-{parent_reference}"
    keep = max_len - len(suffix)
    base = (base[:keep] or "ud") if keep >= 1 else base[: max_len - 2]
    ref = f"{base}{suffix}"
    return (ref + "xxx")[:3] if len(ref) < 3 else ref


def ensure_collections(domain_name: str, datasource_items: List[Dict[str, Any]]) -> Dict[str, str]:
    # Determine parent collection ref
    parent_env = os.getenv("PURVIEW_PARENT_COLLECTION_ID")
    domain_id = os.getenv("PURVIEW_DOMAIN_ID")
    if parent_env:
        parent_name = parent_env
    elif domain_id:
        parent_name = _slug(domain_name)
        resp = pvgo.gov_collections_put(parent_name, {"friendlyName": domain_name})
        if resp.status_code not in (200, 201):
            logger.warning("Governance parent ensure failed (%s). Trying account-host fallback for parent '%s'...", resp.status_code, parent_name)
            acct = pvgo.acct_collections_put(parent_name, {"friendlyName": domain_name})
            if acct.status_code not in (200, 201):
                logger.warning("Account-host parent ensure failed (%s). Set PURVIEW_PARENT_COLLECTION_ID to an existing collection refName.", acct.status_code)
                raise requests.HTTPError(response=acct)  # type: ignore[name-defined]
    else:
        parent_name = _slug(domain_name)
        resp = pvgo.gov_collections_put(parent_name, {"friendlyName": domain_name})
        if resp.status_code not in (200, 201):
            logger.warning("Governance parent ensure failed (%s). Trying account-host fallback for parent '%s'...", resp.status_code, parent_name)
            acct = pvgo.acct_collections_put(parent_name, {"friendlyName": domain_name})
            if acct.status_code not in (200, 201):
                logger.warning("Account-host parent ensure failed (%s). Set PURVIEW_PARENT_COLLECTION_ID to an existing collection refName to proceed.", acct.status_code)
                raise requests.HTTPError(response=acct)  # type: ignore[name-defined]

    child_map: Dict[str, str] = {}
    for item in datasource_items:
        dsid = str(item.get("id"))
        dsname = item.get("name", dsid)
        child_name = _make_collection_ref_for_datasource(dsname, dsid)
        body = {"friendlyName": dsname, "parentCollection": {"referenceName": parent_name}}
        r = pvgo.gov_collections_put(child_name, body)
        if r.status_code not in (200, 201):
            r2 = pvgo.acct_collections_put(child_name, body)
            if r2.status_code not in (200, 201):
                b1 = getattr(r, "text", "")[:180]
                b2 = getattr(r2, "text", "")[:180]
                logger.error("Failed to ensure child collection '%s': gov=%s acct=%s govBody=%s acctBody=%s", child_name, r.status_code, r2.status_code, b1, b2)
                r2.raise_for_status()
        child_map[dsid] = child_name
    return child_map


def ensure_unstructured_datasets_collection(parent_reference: str, name: str = "Unstructured Datasets") -> str:
    ref = _make_ud_collection_ref(parent_reference, name)
    body = {"friendlyName": name, "parentCollection": {"referenceName": parent_reference}}
    r = pvgo.gov_collections_put(ref, body)
    if r.status_code not in (200, 201):
        variants = [
            {"friendlyName": name, "parentCollection": {"referenceName": parent_reference}},
            {"name": name, "parentCollection": {"referenceName": parent_reference}},
            {"friendlyName": name, "parentCollection": {"type": "CollectionReference", "referenceName": parent_reference}},
            {"name": name, "parentCollection": {"type": "CollectionReference", "referenceName": parent_reference}},
        ]
        last = None
        for body2 in variants:
            r2 = pvgo.acct_collections_put(ref, body2)
            if r2.status_code in (200, 201):
                return ref
            last = r2
        if last is not None:
            b1 = getattr(r, "text", "")[:180]
            b2 = getattr(last, "text", "")[:180]
            logger.error("Failed to ensure UD collection '%s': gov=%s acct=%s govBody=%s acctBody=%s", ref, r.status_code, last.status_code, b1, b2)
            last.raise_for_status()
    return ref

