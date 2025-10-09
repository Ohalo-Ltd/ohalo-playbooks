"""Lightweight Atlan REST client built on Application SDK configuration."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional

import httpx

from application_sdk.clients.atlan_auth import AtlanAuthClient
from application_sdk.constants import ATLAN_API_KEY, ATLAN_BASE_URL
from application_sdk.observability.logger_adaptor import get_logger

logger = get_logger(__name__)


class AtlanRequestError(RuntimeError):
    """Raised when the Atlan REST API returns an error response."""

    def __init__(self, message: str, *, status_code: int, details: Any | None = None):
        super().__init__(message)
        self.status_code = status_code
        self.details = details


class AtlanRESTClient:
    """Simple synchronous client for interacting with Atlan's REST API."""

    def __init__(
        self,
        *,
        base_url: str | None = None,
        api_key: str | None = None,
        timeout: float = 120.0,
    ) -> None:
        self._base_url = (base_url or ATLAN_BASE_URL or "").rstrip("/")
        if not self._base_url:
            raise ValueError("ATLAN_BASE_URL must be configured to use AtlanRESTClient.")

        self._auth = AtlanAuthClient()
        self._fallback_headers: Dict[str, str] = {}
        if api_key or ATLAN_API_KEY:
            token = api_key or ATLAN_API_KEY
            self._fallback_headers["Authorization"] = f"Bearer {token}"

        self._client = httpx.Client(timeout=timeout)

    def close(self) -> None:
        self._client.close()

    def _build_headers(self) -> Dict[str, str]:
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        if self._fallback_headers:
            headers.update(self._fallback_headers)
        return headers

    def _authenticated_headers(self) -> Dict[str, str]:
        headers = self._build_headers()
        auth_headers = {}
        try:
            auth_headers = httpx.CaseInsensitiveDict(self._auth.get_authenticated_headers())  # type: ignore[arg-type]
        except Exception:  # pragma: no cover - best effort
            logger.debug("Falling back to static API key authentication.", exc_info=True)
        if auth_headers:
            headers.update(dict(auth_headers))
        return headers

    def request(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        json_body: Any | None = None,
    ) -> Any:
        url = f"{self._base_url}{path}"
        headers = self._authenticated_headers()

        response = self._client.request(
            method,
            url,
            params=params,
            json=json_body,
            headers=headers,
        )

        if response.status_code >= 400:
            details = _safe_json(response)
            raise AtlanRequestError(
                f"Atlan API request to {path} failed with status {response.status_code}",
                status_code=response.status_code,
                details=details,
            )

        return _safe_json(response)

    # Asset helpers ------------------------------------------------------------------
    def upsert_assets(self, assets: List[Dict[str, Any]]) -> Dict[str, Any]:
        payload = {
            "entities": assets,
            "mutualAuth": False,
        }
        return self.request("POST", "/api/meta/atlas/entity/bulk", json_body=payload)

    def get_asset(self, type_name: str, qualified_name: str) -> Optional[Dict[str, Any]]:
        params = {
            "attr:qualifiedName": qualified_name,
            "ignoreRelationships": "true",
        }
        try:
            return self.request(
                "GET",
                f"/api/meta/atlas/entity/uniqueAttribute/type/{type_name}",
                params=params,
            )
        except AtlanRequestError as exc:
            if exc.status_code == 404:
                return None
            raise

    def search_assets(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self.request(
            "POST",
            "/api/meta/atlas/search/indexsearch",
            json_body=payload,
        )

    def delete_asset(self, guid: str) -> None:
        self.request("DELETE", f"/api/meta/atlas/entity/guid/{guid}")

    def purge_asset(self, guid: str) -> None:
        self.request("POST", f"/api/meta/atlas/entity/guid/{guid}/purge")

    def classify_asset(self, guid: str, tag_names: Iterable[str]) -> None:
        payload = [{"typeName": name} for name in tag_names]
        self.request(
            "POST",
            f"/api/meta/atlas/entity/guid/{guid}/classifications",
            json_body=payload,
        )

    def remove_classification(self, guid: str, tag_name: str) -> None:
        self.request(
            "DELETE",
            f"/api/meta/atlas/entity/guid/{guid}/classification/{tag_name}",
        )

    # Typedef helpers ----------------------------------------------------------------
    def create_typedefs(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self.request(
            "POST",
            "/api/meta/atlas/types/typedefs",
            json_body=payload,
        )


def _safe_json(response: httpx.Response) -> Any:
    try:
        if response.text and response.text.strip():
            return response.json()
    except json.JSONDecodeError:
        logger.debug("Response from %s is not JSON: %s", response.request.url, response.text)
    return None
