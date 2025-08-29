from typing import Any, Dict, List, Tuple
import os
import json

import requests

from .config import HTTP, HTTP_TIMEOUT_SECONDS, dxr_env, logger

def get_dxr_classifications() -> List[Dict[str, Any]]:
    """Fetch the classification catalog: GET /api/vbeta/classifications -> {"data": [...]}"""
    env = dxr_env()
    url = f"{env['DXR_APP_URL']}{env['DXR_CLASSIFICATIONS_PATH']}"
    verify = not str(env.get("DXR_VERIFY_SSL", "1")).lower() in ("0", "false", "no")
    headers = {
        "Authorization": f"Bearer {env['DXR_PAT_TOKEN']}",
        "Accept": "application/json",
        "Referer": env["DXR_APP_URL"],
    }
    resp = HTTP.get(url, headers=headers, timeout=HTTP_TIMEOUT_SECONDS, verify=verify)
    if resp.status_code in (401, 403):
        raise SystemExit("Unauthorized fetching DXR classifications; check DXR_PAT_TOKEN")
    resp.raise_for_status()
    try:
        data = resp.json() or {}
        items = data.get("data", [])
        return items if isinstance(items, list) else []
    except Exception:
        return []


def list_file_datasources_for_label(label_id: str, label_name: str | None = None) -> Tuple[Dict[str, str], List[Dict[str, Any]]]:
    """Stream JSONL from GET /api/vbeta/files with label filter and extract unique datasources.
    Primary filter: label.id:"{label_id}". If zero results and label_name provided, fallback to label.name:"{label_name}".
    Returns (name_map, items) where items are minimal dicts: {id,name,connectorTypeName}.
    """
    env = dxr_env()
    from urllib.parse import quote

    def _fetch(query_str: str):
        qs = f"?q={quote(query_str)}"
        url = f"{env['DXR_APP_URL']}{env['DXR_FILES_PATH']}{qs}"
        return url

    url = _fetch(f'labels.id: "{label_id}"')
    headers = {
        "Authorization": f"Bearer {env['DXR_PAT_TOKEN']}",
        "Accept": "application/x-ndjson, application/json",
        "Referer": env["DXR_APP_URL"],
    }
    verify = not str(env.get("DXR_VERIFY_SSL", "1")).lower() in ("0", "false", "no")
    try:
        resp = HTTP.get(url, headers=headers, timeout=HTTP_TIMEOUT_SECONDS, stream=True, verify=verify)
    except Exception as e:
        logger.warning("DXR files request failed for label=%s: %s", label_id, e)
        return {}, []
    if resp.status_code >= 400:
        logger.warning("DXR files HTTP %s for label=%s: %s", resp.status_code, label_id, resp.text[:200])
        return {}, []
    seen: Dict[str, Dict[str, Any]] = {}
    def _consume(stream_resp):
        count = 0
        try:
            for line in stream_resp.iter_lines(decode_unicode=True):
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except Exception:
                    continue
                ds = obj.get("datasource") or {}
                dsid = str(ds.get("id")) if ds.get("id") is not None else None
                if not dsid or dsid in seen:
                    continue
                name = ds.get("name") or dsid
                conn = ds.get("connector") or {}
                ctype = conn.get("type") or ""
                cname = conn.get("name") or None
                cid = conn.get("id") or conn.get("connectorId") or None
                seen[dsid] = {
                    "id": dsid,
                    "name": name,
                    "connectorTypeName": ctype,
                    "connectorName": cname,
                    "connectorId": cid,
                }
                count += 1
        except Exception:
            pass
        return count

    consumed = _consume(resp)
    # Fallback: try by label.name if no results and name provided
    if consumed == 0 and label_name:
        url2 = _fetch(f'labels.name: "{label_name}"')
        try:
            resp2 = HTTP.get(url2, headers=headers, timeout=HTTP_TIMEOUT_SECONDS, stream=True, verify=verify)
            if resp2.status_code < 400:
                _consume(resp2)
        except Exception:
            pass
    items = list(seen.values())
    name_map = {d["id"]: d["name"] for d in items}
    return name_map, items
