"""Integration-style tests for the DXR → Atlan pipeline with stub collaborators."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Dict, List

import pytest

from atlan_dxr_integration import pipeline
from atlan_dxr_integration.config import Config
from atlan_dxr_integration.dxr_client import Classification


class _StubUploader:
    instances: List["_StubUploader"] = []

    def __init__(self, config):
        self.records: List = []
        self.file_assets: List[Dict] = []
        self.connection_qualified_name = config.atlan_global_connection_qualified_name
        self.rest_client = SimpleNamespace(
            search_assets=lambda payload: {"entities": []},
            delete_asset=lambda guid: None,
            purge_asset=lambda guid: None,
        )
        self.provisioner = SimpleNamespace(
            ensure_connection=lambda **_: SimpleNamespace(
                connector_name=config.atlan_global_connector_name,
                qualified_name=config.atlan_global_connection_qualified_name,
                connection=SimpleNamespace(attributes=SimpleNamespace(name=config.atlan_global_connection_name)),
            ),
            ensure_database=lambda **kwargs: kwargs["qualified_name"],
            ensure_schema=lambda **kwargs: kwargs["qualified_name"],
        )
        _StubUploader.instances.append(self)

    def upsert(self, records):
        self.records.extend(records)

    def upsert_files(self, assets):
        self.file_assets.extend(assets)


class _StubTagRegistry:
    def __init__(self, *_, **__):
        pass

    def ensure(self, *, slug_parts, display_name, **_):
        slug = "/".join(slug_parts)
        return SimpleNamespace(slug=slug, display_name=f"DXR :: {display_name}", hashed_name=f"DXR :: {display_name}")


class _StubGlobalAttributeManager:
    def __init__(self, tag_registry):
        self._registry = tag_registry

    def ensure_classification_tags(self, classifications):
        mapping = {}
        for classification in classifications:
            handle = self._registry.ensure(
                slug_parts=["classification", classification.identifier],
                display_name=f"Classification :: {classification.name}",
            )
            mapping[classification.identifier] = handle
        return mapping


class _StubFileAssetFactory:
    def __init__(self, *_, **__):
        pass

    def build(self, payload, *, connection_qualified_name, connection_name, classification_tags):
        asset = {
            "typeName": "File",
            "attributes": {
                "qualifiedName": f"{connection_qualified_name}/{payload.get('id', 'file')}",
                "name": payload.get("name", "file"),
            },
        }
        handles = tuple(classification_tags.values())
        return SimpleNamespace(asset=asset, tag_handles=handles)


class _StubDatasourceCoordinator:
    def __init__(self, *, config, uploader, **_):
        self._uploader = uploader
        self._buffer: List[Dict] = []

    def consume(self, payload, *, classification_tags):
        self._buffer.append({"payload": payload, "tags": classification_tags})

    def flush(self):
        if not self._buffer:
            return
        for item in self._buffer:
            self._uploader.upsert_files([
                {
                    "typeName": "File",
                    "attributes": {
                        "qualifiedName": f"qn::{item['payload'].get('id', 'file')}",
                    },
                }
            ])
        self._buffer.clear()


class _StubDXRClient:
    def __init__(self, classifications, files):
        self._classifications = classifications
        self._files = files

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def fetch_classifications(self):
        return list(self._classifications)

    def stream_files(self):
        yield from self._files


def _build_config() -> Config:
    return Config(
        dxr_base_url="https://dxr.example.com",
        dxr_pat="dxr-token",
        dxr_classification_types=None,
        dxr_sample_file_limit=5,
        dxr_file_fetch_limit=200,
        atlan_base_url="https://atlan.example.com",
        atlan_api_token="atlan-token",
        atlan_global_connection_qualified_name="default/custom/dxr-unstructured-attributes",
        atlan_global_connection_name="dxr-unstructured-attributes",
        atlan_global_connector_name="custom-connector",
        atlan_global_domain_name="DXR Unstructured",
        atlan_datasource_connection_prefix="dxr-datasource",
        atlan_datasource_domain_prefix="DXR",
        atlan_database_name="dxr",
        atlan_schema_name="labels",
        atlan_dataset_path_prefix="dxr",
        atlan_batch_size=10,
        atlan_tag_namespace="DXR",
        atlan_connection_admin_user=None,
        log_level="INFO",
    )


@pytest.mark.parametrize("file_count", [0, 2])
def test_pipeline_runs(monkeypatch: pytest.MonkeyPatch, file_count: int) -> None:
    classifications = [
        Classification(
            identifier="cls-1",
            name="Sensitive",
            type="ANNOTATOR",
            subtype=None,
            description="desc",
            link=None,
            search_link=None,
        )
    ]
    files = [{"id": f"file-{i}", "name": f"File {i}"} for i in range(file_count)]

    monkeypatch.setattr(pipeline, "DXRClient", lambda *_, **__: _StubDXRClient(classifications, files))
    monkeypatch.setattr(pipeline, "AtlanUploader", _StubUploader)
    monkeypatch.setattr(pipeline, "TagRegistry", _StubTagRegistry)
    monkeypatch.setattr(pipeline, "GlobalAttributeManager", _StubGlobalAttributeManager)
    monkeypatch.setattr(pipeline, "FileAssetFactory", _StubFileAssetFactory)
    monkeypatch.setattr(pipeline, "DatasourceIngestionCoordinator", _StubDatasourceCoordinator)
    monkeypatch.setattr(pipeline.Config, "from_env", staticmethod(_build_config))

    _StubUploader.instances.clear()
    pipeline.run()

    assert _StubUploader.instances, "Uploader should have been instantiated"
    uploader = _StubUploader.instances[-1]
    if file_count:
        assert uploader.file_assets, "Expected file assets to be upserted"
    else:
        assert not uploader.file_assets
