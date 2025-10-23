from __future__ import annotations

import responses

from databricks_unity_dxr_integration.config import DataXRayConfig
from databricks_unity_dxr_integration.dxr_client import DataXRayClient, FileUpload


def build_client() -> DataXRayClient:
    config = DataXRayConfig(
        base_url="https://dxr.example/api",
        api_key="token",
        datasource_id="123",
        poll_interval_seconds=5,
        max_bytes_per_job=1024,
    )
    return DataXRayClient(config)


@responses.activate
def test_submit_job_returns_job_id():
    client = build_client()
    responses.add(
        responses.POST,
        "https://dxr.example/api/on-demand-classifiers/123/jobs",
        json={"id": "job-1", "datasourceScanId": 99},
        status=202,
    )

    job = client.submit_job([FileUpload(filename="file.txt", data=b"hello", mime_type="text/plain")])

    assert job.job_id == "job-1"
    assert job.datasource_scan_id == 99


@responses.activate
def test_search_by_scan_id_returns_hits():
    client = build_client()
    responses.add(
        responses.POST,
        "https://dxr.example/api/api/indexed-files/search",
        json={"hits": {"hits": [{"_source": {"id": "file"}}]}},
        status=200,
    )

    hits = client.search_by_scan_id(scan_id=99, page_size=1)

    assert hits == [{"_source": {"id": "file"}}]
