"""Integration-style tests for the DXR → Atlan pipeline."""

from __future__ import annotations

from collections import defaultdict
from types import SimpleNamespace
from typing import Dict, Iterable, Iterator, List

import pytest

from atlan_dxr_integration import atlan_uploader, pipeline
from atlan_dxr_integration.dxr_client import Classification


class _FakeMutationResponse:
    def __init__(self, assets):
        self._assets = assets
        self.request_id = "req-1"

    def assets_created(self, asset_type):
        return [asset for asset in self._assets if isinstance(asset, asset_type)]

    def assets_updated(self, asset_type):
        return []

    def assets_partially_updated(self, asset_type):
        return []


class _FakeDXRClient:
    """Stub DXR client for pipeline integration tests."""

    def __init__(self, classifications: Iterable[Classification], files: Iterable[dict]):
        self._classifications = list(classifications)
        self._files_by_label: Dict[str, List[dict]] = defaultdict(list)
        for payload in files:
            labels = payload.get("labels") or []
            for label in labels:
                label_id = label.get("id") or label.get("classificationId")
                if label_id:
                    self._files_by_label[str(label_id)].append(payload)

    def __enter__(self) -> "_FakeDXRClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # pragma: no cover - cleanup hook
        return None

    def close(self) -> None:  # pragma: no cover - compatibility shim
        return None

    def fetch_classifications(self) -> List[Classification]:
        return list(self._classifications)

    def stream_files(self) -> Iterator[dict]:
        for items in self._files_by_label.values():
            yield from items

    def fetch_files_for_label(
        self,
        label_id: str,
        *,
        label_name: str | None = None,
        max_items: int | None = None,
    ) -> List[dict]:
        items = list(self._files_by_label.get(label_id, []))
        if max_items and max_items > 0:
            return items[:max_items]
        if not items and label_name:
            for key, value in self._files_by_label.items():
                if key.lower() == (label_name or "").lower():
                    return list(value)
        return items


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
            batch = list(assets)
            captured_batches.append(batch)
            return _FakeMutationResponse(batch)

        def get_by_qualified_name(self, qualified_name, asset_type, **kwargs):
            return SimpleNamespace(attributes=SimpleNamespace(qualified_name=qualified_name))

    class _FakeAtlanClient:
        def __init__(self, *args, **kwargs):
            self.asset = _FakeAssetClient()

    monkeypatch.setattr(atlan_uploader, "AtlanClient", _FakeAtlanClient)
    monkeypatch.setattr(
        atlan_uploader.AtlanUploader,
        "_ensure_connection_exists",
        lambda self: (
            atlan_uploader.AtlanConnectorType.CUSTOM,
            "default/connection",
        ),
    )

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
    assert attrs.connector_name == "custom"


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
    monkeypatch.setattr(
        atlan_uploader.AtlanUploader,
        "_ensure_connection_exists",
        lambda self: (
            atlan_uploader.AtlanConnectorType.CUSTOM,
            "default/connection",
        ),
    )

    pipeline.run()

    assert uploader_called is False


@pytest.mark.integration
def test_pipeline_exits_when_upload_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    """Pipeline surfaces upload failures as a non-zero exit code."""

    classifications = [
        Classification(
            identifier="classification-789",
            name="Radiology",
            type="ANNOTATOR",
            subtype=None,
            description=None,
            link=None,
            search_link=None,
        )
    ]

    fake_client = _FakeDXRClient(classifications, files=[])

    monkeypatch.setattr(pipeline, "DXRClient", lambda *args, **kwargs: fake_client)

    monkeypatch.setenv("DXR_BASE_URL", "https://dxr.example.com/api")
    monkeypatch.setenv("DXR_PAT", "dxr-token")
    monkeypatch.setenv("ATLAN_BASE_URL", "https://atlan.example.com")
    monkeypatch.setenv("ATLAN_API_TOKEN", "atlan-token")
    monkeypatch.setenv("ATLAN_CONNECTION_QUALIFIED_NAME", "default/connection")
    monkeypatch.setenv("ATLAN_CONNECTION_NAME", "dxr-connection")
    monkeypatch.setenv("ATLAN_CONNECTOR_NAME", "custom-connector")

    class _FailingUploader:
        def __init__(self, config):
            pass

        def upsert(self, records):
            raise atlan_uploader.AtlanUploadError("permission denied")

    monkeypatch.setattr(pipeline, "AtlanUploader", _FailingUploader)
    monkeypatch.setattr(
        atlan_uploader.AtlanUploader,
        "_ensure_connection_exists",
        lambda self: (
            atlan_uploader.AtlanConnectorType.CUSTOM,
            "default/connection",
        ),
    )

    with pytest.raises(SystemExit) as exc_info:
        pipeline.run()

    assert exc_info.value.code == 1
