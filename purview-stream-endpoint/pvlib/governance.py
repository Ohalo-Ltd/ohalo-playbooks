from typing import Any, Dict, Optional
import os

import requests
from azure.identity import ClientSecretCredential

from .config import purview_env, logger, HTTP_TIMEOUT_SECONDS
from . import atlas as pvatlas


GOV_API_VERSION = "2023-09-01"
ACCOUNT_COLLECTIONS_API_VERSION = "2019-11-01-preview"


def governance_base() -> str:
    base = os.environ.get("PURVIEW_GOV_BASE")
    if base:
        return base.rstrip("/")
    return "https://api.purview-service.microsoft.com"


def governance_headers() -> Dict[str, str]:
    env = purview_env()
    cred = ClientSecretCredential(
        tenant_id=env["AZURE_TENANT_ID"],
        client_id=env["AZURE_CLIENT_ID"],
        client_secret=env["AZURE_CLIENT_SECRET"],
    )
    scopes_to_try = []
    gov_resource = os.environ.get("PURVIEW_GOV_RESOURCE")
    if gov_resource:
        scope = gov_resource.strip()
        if not scope.endswith("/.default"):
            scope = scope[:-1] + ".default" if scope.endswith("/") else scope + "/.default"
        scopes_to_try.append(scope)
    scopes_to_try.append("https://api.purview-service.microsoft.com/.default")
    scopes_to_try.append("https://purview.azure.net/.default")

    token = None
    last_exc: Optional[Exception] = None
    for scope in scopes_to_try:
        try:
            token = cred.get_token(scope).token
            break
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            continue
    if not token:
        if last_exc:
            raise last_exc
        raise SystemExit("Failed to acquire governance token")
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def gov_collections_put(name: str, body: Dict[str, Any]) -> requests.Response:
    base = governance_base()
    headers = governance_headers()
    url = f"{base}/catalog/api/collections/{name}?api-version={GOV_API_VERSION}"
    return requests.put(url, headers=headers, json=body, timeout=HTTP_TIMEOUT_SECONDS)


def gov_collections_get(name: str) -> requests.Response:
    base = governance_base()
    headers = governance_headers()
    url = f"{base}/catalog/api/collections/{name}?api-version={GOV_API_VERSION}"
    return requests.get(url, headers=headers, timeout=HTTP_TIMEOUT_SECONDS)


def acct_collections_put(name: str, body: Dict[str, Any]) -> requests.Response:
    url = f"{pvatlas._endpoint()}/account/collections/{name}?api-version={ACCOUNT_COLLECTIONS_API_VERSION}"
    headers = pvatlas._atlas_headers()
    return requests.put(url, headers=headers, json=body, timeout=HTTP_TIMEOUT_SECONDS)


def acct_collections_get(name: str) -> requests.Response:
    url = f"{pvatlas._endpoint()}/account/collections/{name}?api-version={ACCOUNT_COLLECTIONS_API_VERSION}"
    headers = pvatlas._atlas_headers()
    return requests.get(url, headers=headers, timeout=HTTP_TIMEOUT_SECONDS)


def account_get(path: str) -> requests.Response:
    base = governance_base()
    headers = governance_headers()
    p = path if path.startswith("/") else f"/{path}"
    url = f"{base}/catalog/api{p}?api-version={GOV_API_VERSION}"
    return requests.get(url, headers=headers, timeout=HTTP_TIMEOUT_SECONDS)


def account_put(path: str, payload: Dict[str, Any]) -> requests.Response:
    base = governance_base()
    headers = governance_headers()
    p = path if path.startswith("/") else f"/{path}"
    url = f"{base}/catalog/api{p}?api-version={GOV_API_VERSION}"
    return requests.put(url, headers=headers, json=payload, timeout=HTTP_TIMEOUT_SECONDS)


def account_delete(path: str) -> requests.Response:
    base = governance_base()
    headers = governance_headers()
    p = path if path.startswith("/") else f"/{path}"
    url = f"{base}/catalog/api{p}?api-version={GOV_API_VERSION}"
    return requests.delete(url, headers=headers, timeout=HTTP_TIMEOUT_SECONDS)

