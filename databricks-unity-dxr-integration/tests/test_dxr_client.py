from __future__ import annotations

import io

import responses

from databricks_unity_dxr_integration.config import DataXRayConfig
from databricks_unity_dxr_integration.dxr_client import DataXRayClient, FileUpload


def build_client() -> DataXRayClient:
    config = DataXRayConfig(
        base_url="https://dxr.example",
        datasource_id="123",
        poll_interval_seconds=5,
        max_bytes_per_job=1024,
    )
    return DataXRayClient(config, api_key="token")


@responses.activate
def test_submit_job_returns_job_id():
    client = build_client()
    responses.add(
        responses.POST,
        "https://dxr.example/api/on-demand-classifiers/123/jobs",
        json={"id": "job-1", "datasourceScanId": 99},
        status=202,
    )

    job = client.submit_job([FileUpload(filename="file.txt", file_handle=io.BytesIO(b"hello"))])

    assert job.job_id == "job-1"
    assert job.datasource_scan_id == 99


@responses.activate
def test_search_by_scan_id_returns_hits():
    client = build_client()
    # First page returns results
    responses.add(
        responses.POST,
        "https://dxr.example/api/indexed-files/search",
        json={"hits": {"hits": [{"_source": {"id": "file"}}]}},
        status=200,
    )
    # Second page returns empty to stop pagination
    responses.add(
        responses.POST,
        "https://dxr.example/api/indexed-files/search",
        json={"hits": {"hits": []}},
        status=200,
    )

    hits = client.search_by_scan_id(scan_id=99, page_size=1)

    assert hits == [{"_source": {"id": "file"}}]


@responses.activate
def test_get_file_metadata_returns_file_details():
    client = build_client()
    responses.add(
        responses.GET,
        "https://dxr.example/api/v1/files/file-123",
        json={
            "fileId": "file-123",
            "fileName": "test.pdf",
            "extractedMetadata": [
                {"name": "Title", "value": "Test Document", "type": "TEXT"}
            ],
            "owner": {"name": "John Doe", "email": "john@example.com"},
            "scanDepth": "DISCOVERY_AND_CLASSIFICATION",
        },
        status=200,
    )

    file_details = client.get_file_metadata("file-123")

    assert file_details["fileId"] == "file-123"
    assert file_details["fileName"] == "test.pdf"
    assert len(file_details["extractedMetadata"]) == 1
    assert file_details["extractedMetadata"][0]["name"] == "Title"
    assert file_details["owner"]["email"] == "john@example.com"
