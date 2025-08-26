from typing import Any, Dict, List, Tuple
import os

import requests

from .config import HTTP, HTTP_TIMEOUT_SECONDS, dxr_env, logger


def get_dxr_tags() -> List[Dict[str, Any]]:
    env = dxr_env()
    url = f"{env['DXR_APP_URL']}{env['DXR_TAGS_PATH']}"
    headers = {
        "Authorization": f"Bearer {env['DXR_PAT_TOKEN']}",
        "Accept": "application/json, text/plain, */*",
        "Referer": env['DXR_APP_URL'],
    }
    resp = HTTP.get(url, headers=headers, timeout=HTTP_TIMEOUT_SECONDS)
    if resp.status_code in (401, 403):
        raise SystemExit("Unauthorized fetching DXR tags; check DXR_PAT_TOKEN")
    resp.raise_for_status()
    try:
        data = resp.json()
    except Exception:
        return []
    if isinstance(data, list):
        return data
    if isinstance(data, dict) and isinstance(data.get("items"), list):
        return data["items"]
    return []


def get_dxr_searchable_datasources() -> Tuple[Dict[str, str], List[Dict[str, Any]]]:
    env = dxr_env()
    url = f"{env['DXR_APP_URL']}{env['DXR_SEARCHABLE_DATASOURCES_PATH']}"
    headers = {
        "Authorization": f"Bearer {env['DXR_PAT_TOKEN']}",
        "Accept": "application/json, text/plain, */*",
        "Referer": env['DXR_APP_URL'],
    }
    try:
        resp = HTTP.get(url, headers=headers, timeout=HTTP_TIMEOUT_SECONDS)
    except Exception as e:
        logger.warning("DXR datasources request failed: %s", e)
        return {}, []
    if resp.status_code >= 400:
        logger.warning("DXR datasources HTTP %s: %s", resp.status_code, resp.text[:200])
        return {}, []
    try:
        data = resp.json()
    except Exception:
        return {}, []
    items = data if isinstance(data, list) else data.get("items", [])
    name_map = {str(i.get("id")): i.get("name", "") for i in items if isinstance(i, dict) and "id" in i}
    return name_map, items


def get_label_statistics_for_datasource(dsid: str, limit: int) -> List[Dict[str, Any]]:
    env = dxr_env()
    url = f"{env['DXR_APP_URL']}/api/dashboard/label-statistics?datasources={dsid}&limit={int(limit)}"
    headers = {
        "Authorization": f"Bearer {env['DXR_PAT_TOKEN']}",
        "Accept": "application/json, text/plain, */*",
        "Referer": env['DXR_APP_URL'],
    }
    try:
        resp = HTTP.get(url, headers=headers, timeout=HTTP_TIMEOUT_SECONDS)
    except Exception as e:
        logger.warning("DXR label-statistics failed for ds=%s: %s", dsid, e)
        return []
    if resp.status_code >= 400:
        logger.warning("DXR label-statistics HTTP %s for ds=%s: %s", resp.status_code, dsid, resp.text[:200])
        return []
    try:
        data = resp.json()
        return data if isinstance(data, list) else []
    except Exception:
        return []

