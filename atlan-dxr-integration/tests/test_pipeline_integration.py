"""Integration-style tests for the DXR → Atlan pipeline with stub collaborators."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Dict, List

import pytest

from atlan_dxr_integration import pipeline
from atlan_dxr_integration.connection_utils import ConnectionHandle
from atlan_dxr_integration.tag_registry import TagHandle
from atlan_dxr_integration.dxr_client import Classification
from pyatlan.model.assets.core.file import File
from pyatlan.model.assets.core.table import Table
from pyatlan.model.enums import AtlanConnectorType, FileType


class _FakeDXRClient:
    def __init__(self, classifications, files):
        self._classifications = list(classifications)
        self._files = list(files)

    def __enter__(self) -> "_FakeDXRClient":
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def fetch_classifications(self):
        return list(self._classifications)

    def stream_files(self):
        yield from self._files


class _StubTagRegistry:
    def __init__(self, client, namespace):
        self.namespace = namespace

    def ensure(self, *, slug_parts, display_name, **_):
        slug = "/".join(slug_parts)
        name = f"{self.namespace} :: {display_name}"
        return TagHandle(slug=slug, name=name)


class _StubGlobalAttributeManager:
    def __init__(self, client, tag_registry):
        self._registry = tag_registry

    def ensure_classification_tags(self, classifications):
        mapping = {}
        for classification in classifications:
            identifier = classification.identifier
            display = classification.name or identifier
            handle = self._registry.ensure(
                slug_parts=["classification", identifier],
                display_name=f"Classification :: {display}",
            )
            mapping[identifier] = handle
        return mapping


class _StubFileAssetFactory:
    def __init__(self, *, tag_registry, tag_namespace, dxr_base_url=None):
        self._registry = tag_registry

    def build(
        self,
        payload: Dict[str, object],
        *,
        connection_qualified_name: str,
        connection_name: str,
        classification_tags,
    ) -> File:
        identifier = payload.get("id") or payload.get("fileId") or "file"
        asset = File.creator(
            name=payload.get("name") or payload.get("fileName") or str(identifier),
            connection_qualified_name=connection_qualified_name,
            file_type=FileType.TXT,
        )
        attrs = asset.attributes
        attrs.qualified_name = f"{connection_qualified_name}/{identifier}"
        attrs.connection_name = connection_name
        attrs.asset_tags = [handle.name for handle in classification_tags.values()]
        return asset


class _StubConnectionProvisioner:
    def __init__(self, client):
        self.client = client

    def ensure_connection(
        self,
        *,
        qualified_name: str,
        connection_name: str,
        connector_name: str,
        domain_name: str | None = None,
    ) -> ConnectionHandle:
        connection = SimpleNamespace(attributes=SimpleNamespace(name=connection_name))
        return ConnectionHandle(
            connector_type=AtlanConnectorType.CUSTOM,
            qualified_name=qualified_name,
            connection=connection,
        )

    def ensure_database(self, *, name: str, qualified_name: str, **_):
        return qualified_name

    def ensure_schema(self, *, name: str, qualified_name: str, **_):
        return qualified_name


class _StubDatasourceCoordinator:
    def __init__(self, *, config, client, provisioner, factory):
        self.records: List[Dict[str, object]] = []
        self._factory = factory
        self.flushed = False

    def consume(self, payload, *, classification_tags):
        self.records.append(payload)
        self._factory.build(
            payload,
            connection_qualified_name="default/custom/dxr-datasource",
            connection_name="dxr-datasource",
            classification_tags=classification_tags,
        )

    def flush(self):
        self.flushed = True


class _StubUploader:
    def __init__(self, config):
        self.client = SimpleNamespace()
        self.upserts: List[List[Table]] = []

    def upsert(self, records):
        batch = []
        for record in records:
            table = Table.creator(
                name=record.identifier,
                schema_qualified_name="default/custom/dxr-unstructured-attributes/dxr/labels",
                database_qualified_name="default/custom/dxr-unstructured-attributes/dxr",
                database_name="dxr",
                schema_name="labels",
                connection_qualified_name="default/custom/dxr-unstructured-attributes",
            )
            batch.append(table)
        self.upserts.append(batch)


def _prepare_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DXR_BASE_URL", "https://dxr.example.com/api")
    monkeypatch.setenv("DXR_PAT", "dxr-token")
    monkeypatch.setenv("ATLAN_BASE_URL", "https://atlan.example.com")
    monkeypatch.setenv("ATLAN_API_TOKEN", "atlan-token")
    monkeypatch.setenv(
        "ATLAN_GLOBAL_CONNECTION_QUALIFIED_NAME",
        "default/custom/dxr-unstructured-attributes",
    )
    monkeypatch.setenv("ATLAN_GLOBAL_CONNECTION_NAME", "dxr-unstructured-attributes")
    monkeypatch.setenv("ATLAN_GLOBAL_CONNECTOR_NAME", "custom-connector")
    monkeypatch.setenv("ATLAN_GLOBAL_DOMAIN", "DXR Unstructured")
    monkeypatch.setenv("ATLAN_DATASOURCE_CONNECTION_PREFIX", "dxr-datasource")
    monkeypatch.setenv("ATLAN_BATCH_SIZE", "5")
    monkeypatch.setenv("ATLAN_DATABASE_NAME", "dxr")
    monkeypatch.setenv("ATLAN_SCHEMA_NAME", "labels")
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")


@pytest.mark.integration
def test_pipeline_runs_end_to_end(monkeypatch: pytest.MonkeyPatch) -> None:
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

    _prepare_env(monkeypatch)
    captured_files: list[Dict[str, object]] = []

    def _ds_factory(**kwargs):
        coordinator = _StubDatasourceCoordinator(**kwargs)
        original_consume = coordinator.consume

        def _consume(payload, *, classification_tags):
            captured_files.append(payload)
            original_consume(payload, classification_tags=classification_tags)

        coordinator.consume = _consume  # type: ignore
        return coordinator

    monkeypatch.setattr(pipeline, "DXRClient", lambda *args, **kwargs: _FakeDXRClient(classifications, files))
    monkeypatch.setattr(pipeline, "AtlanUploader", _StubUploader)
    monkeypatch.setattr(pipeline, "ConnectionProvisioner", _StubConnectionProvisioner)
    monkeypatch.setattr(pipeline, "TagRegistry", _StubTagRegistry)
    monkeypatch.setattr(pipeline, "GlobalAttributeManager", _StubGlobalAttributeManager)
    monkeypatch.setattr(pipeline, "FileAssetFactory", _StubFileAssetFactory)
    monkeypatch.setattr(pipeline, "DatasourceIngestionCoordinator", _ds_factory)

    pipeline.run()

    assert captured_files == files


@pytest.mark.integration
def test_pipeline_skips_upload_when_no_records(monkeypatch: pytest.MonkeyPatch) -> None:
    classifications = []
    files: list[dict] = []

    _prepare_env(monkeypatch)

    holder: dict[str, _StubUploader] = {}

    def _uploader_factory(config):
        uploader = _StubUploader(config)
        holder["instance"] = uploader
        return uploader

    monkeypatch.setattr(pipeline, "DXRClient", lambda *args, **kwargs: _FakeDXRClient(classifications, files))
    monkeypatch.setattr(pipeline, "AtlanUploader", _uploader_factory)
    monkeypatch.setattr(pipeline, "ConnectionProvisioner", _StubConnectionProvisioner)
    monkeypatch.setattr(pipeline, "TagRegistry", _StubTagRegistry)
    monkeypatch.setattr(pipeline, "GlobalAttributeManager", _StubGlobalAttributeManager)
    monkeypatch.setattr(pipeline, "FileAssetFactory", _StubFileAssetFactory)
    monkeypatch.setattr(pipeline, "DatasourceIngestionCoordinator", lambda **kwargs: _StubDatasourceCoordinator(**kwargs))

    pipeline.run()

    assert holder["instance"].upserts == []


@pytest.mark.integration
def test_pipeline_exits_when_upload_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    _prepare_env(monkeypatch)

    classifications = [
        Classification(
            identifier="classification-999",
            name="Test",
            type="ANNOTATOR",
            subtype=None,
            description=None,
            link=None,
            search_link=None,
        )
    ]
    monkeypatch.setattr(
        pipeline,
        "DXRClient",
        lambda *args, **kwargs: _FakeDXRClient(classifications, []),
    )

    class _FailingUploader(_StubUploader):
        def upsert(self, records):
            raise pipeline.AtlanUploadError("boom")

    monkeypatch.setattr(pipeline, "AtlanUploader", _FailingUploader)
    monkeypatch.setattr(pipeline, "ConnectionProvisioner", _StubConnectionProvisioner)
    monkeypatch.setattr(pipeline, "TagRegistry", _StubTagRegistry)
    monkeypatch.setattr(pipeline, "GlobalAttributeManager", _StubGlobalAttributeManager)
    monkeypatch.setattr(pipeline, "FileAssetFactory", _StubFileAssetFactory)
    monkeypatch.setattr(pipeline, "DatasourceIngestionCoordinator", lambda **kwargs: _StubDatasourceCoordinator(**kwargs))

    with pytest.raises(SystemExit) as exc_info:
        pipeline.run()

    assert exc_info.value.code == 1
