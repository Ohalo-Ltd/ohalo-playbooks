import json
import logging
from typing import Generator, Optional
from urllib.parse import urljoin

import requests

logger = logging.getLogger(__name__)

FILES_ENDPOINT = "/api/v1/files"


class DataXRayClient:
    """Minimal client for streaming file metadata from Data X-Ray."""

    def __init__(
        self,
        base_url: str,
        bearer_token: str,
        *,
        http_timeout: int = 120,
        verify_ssl: bool = True,
        user_agent: str = "query-data-xray-data-in-databricks/0.1.0",
    ) -> None:
        self.base_url = base_url.rstrip("/") + "/"
        self.files_path = FILES_ENDPOINT.lstrip("/")
        self.bearer_token = bearer_token
        self.http_timeout = http_timeout
        self.verify_ssl = verify_ssl
        self.user_agent = user_agent

    def stream_file_metadata(self, query: Optional[str] = None, record_cap: Optional[int] = None) -> Generator[dict, None, None]:
        """Stream JSONL rows from GET /api/v1/files (optionally filtered)."""
        target_url = urljoin(self.base_url, self.files_path)
        headers = {
            "Authorization": f"Bearer {self.bearer_token}",
            "Accept": "application/x-ndjson, application/jsonlines, application/json",
            "User-Agent": self.user_agent,
        }
        params = {"q": query} if query else None
        logger.info("Requesting %s", target_url)
        resp = requests.get(
            target_url,
            headers=headers,
            params=params,
            timeout=self.http_timeout,
            stream=True,
            verify=self.verify_ssl,
        )
        resp.raise_for_status()
        yielded = 0
        for line in resp.iter_lines(decode_unicode=True):
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                logger.debug("Skipping non-JSON line: %s", line[:80])
                continue
            yield payload
            yielded += 1
            if record_cap is not None and yielded >= record_cap:
                logger.info("Record cap %s reached; stopping stream", record_cap)
                break
        logger.info("Stream finished after %s rows", yielded)
