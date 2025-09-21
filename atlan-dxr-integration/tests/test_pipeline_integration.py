"""Integration-style tests for the DXR → Atlan pipeline."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Iterable, Iterator, List

import pytest

from atlan_dxr_integration import atlan_uploader, pipeline
from atlan_dxr_integration.dxr_client import Classification


class _FakeDXRClient:
    """Stub DXR client for pipeline integration tests."""

    def __init__(self, classifications: Iterable[Classification], files: Iterable[dict]):
        self._classifications = list(classifications)
        self._files = list(files)

    def __enter__(self) -> "_FakeDXRClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # pragma: no cover - cleanup hook
        return None

    def close(self) -> None:  # pragma: no cover - compatibility shim
        return None

    def fetch_classifications(self) -> List[Classification]:
        return list(self._classifications)

    def stream_files(self) -> Iterator[dict]:
        yield from self._files


@pytest.mark.integration
def test_pipeline_runs_end_to_end(monkeypatch: pytest.MonkeyPatch) -> None:
    """Pipeline builds datasets from DXR metadata and upserts them into Atlan."""

    classifications = [
        Classification(
            identifier="classification-123",
            name="Heart Failure",
            type="ANNOTATOR",
            subtype="REGEX",
            description="Files classified under the disease Heart Failure",
            link="/resource/classification-123",
            search_link="/resource-search/classification-123",
        )
    ]
    files = [
        {
            "id": "file-1",
            "name": "patient-data.csv",
            "path": "s3://bucket/patient-data.csv",
            "labels": [{"id": "classification-123"}],
        },
        {
            "id": "file-2",
            "filename": "lab-results.parquet",
            "filepath": "s3://bucket/lab-results.parquet",
            "labels": [{"classificationId": "classification-123"}],
        },
    ]

    fake_client = _FakeDXRClient(classifications, files)

    def _client_factory(base_url: str, pat_token: str) -> _FakeDXRClient:
        assert base_url == "https://dxr.example.com/api"
        assert pat_token == "dxr-token"
        return fake_client

    monkeypatch.setenv("DXR_BASE_URL", "https://dxr.example.com/api")
    monkeypatch.setenv("DXR_PAT", "dxr-token")
    monkeypatch.setenv("DXR_CLASSIFICATION_TYPES", "ANNOTATOR")
    monkeypatch.setenv("DXR_SAMPLE_FILE_LIMIT", "2")

    monkeypatch.setenv("ATLAN_BASE_URL", "https://atlan.example.com")
    monkeypatch.setenv("ATLAN_API_TOKEN", "atlan-token")
    monkeypatch.setenv("ATLAN_CONNECTION_QUALIFIED_NAME", "default/connection")
    monkeypatch.setenv("ATLAN_CONNECTION_NAME", "dxr-connection")
    monkeypatch.setenv("ATLAN_CONNECTOR_NAME", "custom-connector")
    monkeypatch.setenv("ATLAN_DATASET_PATH_PREFIX", "dxr")
    monkeypatch.setenv("ATLAN_BATCH_SIZE", "5")
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")

    monkeypatch.setattr(pipeline, "DXRClient", lambda base_url, pat: _client_factory(base_url, pat))

    captured_batches: list = []

    class _FakeAssetClient:
        def save(self, assets):
            captured_batches.append(list(assets))
            return SimpleNamespace(request_id="req-1")

    class _FakeAtlanClient:
        def __init__(self, *args, **kwargs):
            self.asset = _FakeAssetClient()

    monkeypatch.setattr(atlan_uploader, "AtlanClient", _FakeAtlanClient)

    pipeline.run()

    assert captured_batches, "Expected datasets to be pushed into Atlan"
    assert len(captured_batches[0]) == 1
    dataset = captured_batches[0][0]
    attrs = dataset.attributes
    assert attrs.qualified_name == "default/connection/dxr/classification-123"
    assert attrs.name == "Heart Failure"
    assert attrs.description == "Files classified under the disease Heart Failure"
    assert "DXR dataset contains 2 file(s)." in attrs.user_description
    assert "patient-data.csv" in attrs.user_description
    assert "lab-results.parquet" in attrs.user_description
    assert attrs.source_url == "https://dxr.example.com/resource-search/classification-123"


@pytest.mark.integration
def test_pipeline_skips_upload_when_no_records(monkeypatch: pytest.MonkeyPatch) -> None:
    """No upload is attempted if no dataset records are generated."""

    classifications = [
        Classification(
            identifier="classification-456",
            name="Cardiology",
            type="ANNOTATOR",
            subtype=None,
            description=None,
            link=None,
            search_link=None,
        )
    ]

    fake_client = _FakeDXRClient(classifications, files=[])

    monkeypatch.setenv("DXR_BASE_URL", "https://dxr.example.com/api")
    monkeypatch.setenv("DXR_PAT", "dxr-token")
    monkeypatch.setenv("DXR_CLASSIFICATION_TYPES", "NON_MATCHING_TYPE")

    monkeypatch.setenv("ATLAN_BASE_URL", "https://atlan.example.com")
    monkeypatch.setenv("ATLAN_API_TOKEN", "atlan-token")
    monkeypatch.setenv("ATLAN_CONNECTION_QUALIFIED_NAME", "default/connection")
    monkeypatch.setenv("ATLAN_CONNECTION_NAME", "dxr-connection")
    monkeypatch.setenv("ATLAN_CONNECTOR_NAME", "custom-connector")

    monkeypatch.setattr(pipeline, "DXRClient", lambda *args, **kwargs: fake_client)

    uploader_called = False

    class _FakeUploader:
        def __init__(self, config):
            pass

        def upsert(self, records):  # pragma: no cover - should not be invoked
            nonlocal uploader_called
            uploader_called = True

    monkeypatch.setattr(pipeline, "AtlanUploader", _FakeUploader)

    pipeline.run()

    assert uploader_called is False
