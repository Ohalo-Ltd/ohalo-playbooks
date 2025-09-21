"""Client for interacting with the Data X-Ray API."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Dict, Generator, List, Optional
from urllib.parse import urljoin

import requests

LOGGER = logging.getLogger(__name__)


@dataclass
class Classification:
    """Representation of a DXR classification (label)."""

    identifier: str
    name: str
    type: Optional[str]
    subtype: Optional[str]
    description: Optional[str]
    link: Optional[str]
    search_link: Optional[str]

    @classmethod
    def from_dict(cls, data: Dict[str, object]) -> "Classification":
        identifier = str(data.get("id")) if data.get("id") is not None else None
        if not identifier:
            raise ValueError("Classification payload missing 'id'.")
        return cls(
            identifier=identifier,
            name=str(data.get("name") or identifier),
            type=_safe_upper(data.get("type")),
            subtype=_safe_upper(data.get("subtype")),
            description=_safe_str(data.get("description")),
            link=_safe_str(data.get("link")),
            search_link=_safe_str(data.get("searchLink")),
        )


class DXRClient:
    """Thin wrapper around DXR's HTTP API."""

    def __init__(self, base_url: str, pat_token: str, *, timeout: int = 60) -> None:
        self._base_url = base_url.rstrip("/") + "/"
        self._timeout = timeout
        self._session = requests.Session()
        self._session.headers.update(
            {
                "Authorization": f"Bearer {pat_token}",
                "Accept": "application/json, application/x-ndjson",
            }
        )

    def close(self) -> None:
        self._session.close()

    def __enter__(self) -> "DXRClient":  # pragma: no cover - context manager glue
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # pragma: no cover - context manager glue
        self.close()

    def fetch_classifications(self) -> List[Classification]:
        """Retrieve all classifications (labels) from DXR."""

        url = urljoin(self._base_url, "vbeta/classifications")
        LOGGER.debug("Fetching classifications from %s", url)
        response = self._session.get(url, timeout=self._timeout)
        response.raise_for_status()
        payload = response.json()
        items = payload.get("data", []) if isinstance(payload, dict) else []
        classifications = []
        for item in items:
            try:
                classifications.append(Classification.from_dict(item))
            except ValueError as exc:
                LOGGER.warning("Skipping malformed classification payload: %s", exc)
        LOGGER.info("Fetched %d classifications from DXR", len(classifications))
        return classifications

    def stream_files(self) -> Generator[Dict[str, object], None, None]:
        """Stream file metadata as dictionaries from DXR."""

        url = urljoin(self._base_url, "vbeta/files")
        LOGGER.debug("Streaming files from %s", url)
        with self._session.get(url, stream=True, timeout=self._timeout) as response:
            response.raise_for_status()
            yield from self._iter_stream(response)

    def fetch_files_for_label(
        self,
        label_id: str,
        *,
        label_name: Optional[str] = None,
        max_items: Optional[int] = None,
    ) -> List[Dict[str, object]]:
        """Retrieve files associated with a specific DXR label."""

        if not label_id:
            return []

        queries = [f'labels.id: "{label_id}"']
        if label_name:
            queries.append(f'labels.name: "{label_name}"')

        results: List[Dict[str, object]] = []
        for query in queries:
            url = urljoin(self._base_url, "vbeta/files")
            params = {"q": query}
            LOGGER.debug("Fetching files for label %s with query %s", label_id, query)
            try:
                with self._session.get(
                    url,
                    params=params,
                    stream=True,
                    timeout=self._timeout,
                ) as response:
                    response.raise_for_status()
                    for obj in self._iter_stream(response):
                        results.append(obj)
                        if max_items and max_items > 0 and len(results) >= max_items:
                            return results
            except requests.HTTPError as exc:
                LOGGER.warning(
                    "DXR files request failed for label %s query %s: %s",
                    label_id,
                    query,
                    exc,
                )
                continue
            if results:
                break
        return results

    @staticmethod
    def _iter_stream(response: requests.Response) -> Generator[Dict[str, object], None, None]:
        for line in response.iter_lines(decode_unicode=True):
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                LOGGER.warning("Skipping malformed JSONL line from DXR: %s", line)


def _safe_str(value: object) -> Optional[str]:
    if value is None:
        return None
    text = str(value)
    return text if text else None


def _safe_upper(value: object) -> Optional[str]:
    text = _safe_str(value)
    return text.upper() if text else None


__all__ = ["Classification", "DXRClient"]
