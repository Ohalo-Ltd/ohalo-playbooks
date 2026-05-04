"""Minimal streaming client for GET /api/v1/files."""

from __future__ import annotations

import json
import logging
from typing import Generator, Optional

import requests

logger = logging.getLogger(__name__)

FILES_ENDPOINT = "/api/v1/files"


class DataXRayClient:
    """Streams JSONL file metadata records from Data X-Ray's /api/v1/files endpoint.

    The endpoint returns one JSON object per line (NDJSON).  Large tenants can
    have millions of file records, so this client always streams rather than
    loading the full response into memory.

    Args:
        base_url: Root URL of the Data X-Ray tenant (e.g. ``https://dxr.example.com``).
        bearer_token: Personal Access Token (PAT) or service-account token.
        http_timeout: Seconds before the initial connection times out. The
            streaming read itself does not time out — rows arrive continuously.
        verify_ssl: Set to ``False`` to skip TLS verification (dev/private installs).
        user_agent: Value sent in the ``User-Agent`` request header.
    """

    def __init__(
        self,
        base_url: str,
        bearer_token: str,
        *,
        http_timeout: int = 120,
        verify_ssl: bool = True,
        user_agent: str = "dxr-bi-csv-export/0.1.0",
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.bearer_token = bearer_token
        self.http_timeout = http_timeout
        self.verify_ssl = verify_ssl
        self.user_agent = user_agent

    def stream_files(
        self,
        query: Optional[str] = None,
        record_cap: Optional[int] = None,
    ) -> Generator[dict, None, None]:
        """Yield file metadata records as Python dicts, one per file.

        Args:
            query: Optional KQL filter string forwarded as ``?q=<query>``.
                Example: ``"datasource.id:123 AND labels.name:CONFIDENTIAL"``
            record_cap: Stop after this many rows (useful for testing/sampling).

        Yields:
            Parsed JSON objects, one per file record.
        """
        url = f"{self.base_url}{FILES_ENDPOINT}"
        headers = {
            "Authorization": f"Bearer {self.bearer_token}",
            "Accept": "application/x-ndjson, application/jsonlines, application/json",
            "User-Agent": self.user_agent,
        }
        params: dict = {}
        if query:
            params["q"] = query

        logger.info("Streaming from %s  query=%s", url, query or "<none>")
        response = requests.get(
            url,
            headers=headers,
            params=params or None,
            timeout=self.http_timeout,
            stream=True,
            verify=self.verify_ssl,
        )
        response.raise_for_status()

        yielded = 0
        for raw_line in response.iter_lines(decode_unicode=True):
            if not raw_line:
                continue
            try:
                record = json.loads(raw_line)
            except json.JSONDecodeError:
                logger.debug("Skipping non-JSON line: %s", raw_line[:120])
                continue
            yield record
            yielded += 1
            if record_cap is not None and yielded >= record_cap:
                logger.info("Record cap %d reached — stopping stream", record_cap)
                break

        logger.info("Stream complete: %d file records received", yielded)
