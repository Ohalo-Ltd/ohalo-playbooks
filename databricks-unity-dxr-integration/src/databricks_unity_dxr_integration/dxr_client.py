from __future__ import annotations

import time
from dataclasses import dataclass
from typing import BinaryIO, Dict, Iterable, List, Tuple

import requests
from requests import Response
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential
import urllib3
from urllib3.exceptions import InsecureRequestWarning

from .config import DataXRayConfig


class DataXRayError(RuntimeError):
    """Raised when Data X-Ray returns an unexpected response."""


@dataclass
class SubmittedJob:
    job_id: str
    datasource_scan_id: int | None = None


@dataclass
class FileUpload:
    """Payload describing a file to be uploaded to Data X-Ray."""

    filename: str
    file_handle: BinaryIO
    mime_type: str = "application/octet-stream"

    def to_form_tuple(self) -> Tuple[str, Tuple[str, BinaryIO, str]]:
        return ("files", (self.filename, self.file_handle, self.mime_type))


class DataXRayClient:
    """Client for interacting with Data X-Ray On-Demand Classifier APIs."""

    def __init__(self, config: DataXRayConfig, api_key: str):
        self._config = config
        self._session = requests.Session()
        self._session.headers.update({"Authorization": f"Bearer {api_key}"})
        verify_setting: bool | str
        if config.ca_bundle_path:
            verify_setting = config.ca_bundle_path
        else:
            verify_setting = config.verify_ssl
        self._session.verify = verify_setting
        if verify_setting is False:
            urllib3.disable_warnings(InsecureRequestWarning)

    def submit_job(self, uploads: Iterable[FileUpload]) -> SubmittedJob:
        """Submit an ODC job for the provided files."""
        file_entries = [upload.to_form_tuple() for upload in uploads]
        if not file_entries:
            raise ValueError("At least one file must be supplied to submit a job.")

        response = self._session.post(
            self._build_url(f"on-demand-classifiers/{self._config.datasource_id}/jobs"),
            files=file_entries,
            timeout=300,
        )
        _raise_for_status(response)
        payload = response.json()

        return SubmittedJob(
            job_id=str(payload["id"]),
            datasource_scan_id=payload.get("datasourceScanId"),
        )

    def get_job(self, job_id: str) -> dict:
        """Fetch the status of an On-Demand Classifier job."""
        response = self._session.get(
            self._build_url(f"on-demand-classifiers/{self._config.datasource_id}/jobs/{job_id}"),
            timeout=30,
        )
        _raise_for_status(response)
        return response.json()

    def wait_for_completion(self, job_id: str, poll_interval_seconds: int) -> dict:
        """Poll until the job reaches a terminal state."""
        while True:
            job = self.get_job(job_id)
            state = job.get("state")
            if self._config.debug:
                print(f"Job {job_id} state: {state}")
            if state in {"FINISHED", "FAILED"}:
                return job
            time.sleep(max(poll_interval_seconds, 1))

    @retry(
        retry=retry_if_exception_type(DataXRayError),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        stop=stop_after_attempt(5),
        reraise=True,
    )
    def search_by_scan_id(self, scan_id: int, page_size: int = 100) -> List[Dict]:
        """Fetch files that belong to the supplied datasource scan id."""
        payload = {
            "mode": "DXR_JSON_QUERY",
            "datasourceIds": [],
            "pageNumber": 0,
            "pageSize": page_size,
            "filter": {
                "query_items": [
                    {
                        "parameter": "dxr#datasource_scan_id",
                        "value": scan_id,
                        "type": "number",
                        "match_strategy": "exact",
                        "operator": "AND",
                        "group_id": 0,
                        "group_order": 0,
                    }
                ]
            },
            "sort": [{"property": "_score", "order": "DESCENDING"}],
        }
        response = self._session.post(
            self._build_url("indexed-files/search"),
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=60,
        )
        _raise_for_status(response)
        return response.json().get("hits", {}).get("hits", [])

    def _build_url(self, path: str) -> str:
        base = self._config.base_url.rstrip("/")
        prefix = self._config.api_prefix
        segments = [base]
        if prefix:
            segments.append(prefix.lstrip("/"))
        segments.append(path.lstrip("/"))
        return "/".join(segment for segment in segments if segment)


def _raise_for_status(response: Response) -> None:
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        body = (response.text or "").strip()
        message = f"{exc}"
        if body:
            snippet = body if len(body) < 512 else f"{body[:512]}..."
            message = f"{message}; response body: {snippet}"
        raise DataXRayError(message) from exc
    def _build_url(self, path: str) -> str:
        base = self._config.base_url.rstrip("/")
        prefix = self._config.api_prefix
        segments = [base]
        if prefix:
            segments.append(prefix.lstrip("/"))
        segments.append(path.lstrip("/"))
        return "/".join(segment for segment in segments if segment)
