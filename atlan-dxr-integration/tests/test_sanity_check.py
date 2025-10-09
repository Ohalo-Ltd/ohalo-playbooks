"""Tests for the sanity check helper."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from scripts import sanity_check
from atlan_dxr_integration.config import Config
from atlan_dxr_integration.dxr_client import Classification


class _FakeDXR:
    def __init__(self, classifications, files):
        self._classifications = classifications
        self._files = files

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):  # pragma: no cover - no cleanup
        return False

    def fetch_classifications(self):
        return list(self._classifications)

    def stream_files(self):
        yield from self._files

    def fetch_files_for_label(self, label_id, *, label_name=None, max_items=None):
        if max_items is not None and max_items > 0:
            return self._files[:max_items]
        return list(self._files)


class _FakeRESTClient:
    def __init__(self):
        self.assets = {
            "default/custom/dxr-unstructured-attributes/dxr/labels/classification-1": {
                "entity": {
                    "attributes": {
                        "qualifiedName": "default/custom/dxr-unstructured-attributes/dxr/labels/classification-1",
                        "name": "Sanity Dataset",
                    }
                }
            }
        }

    def get_asset(self, type_name, qualified_name):
        return self.assets.get(qualified_name)


class _FakeUploader:
    def __init__(self, cfg):
        self.rest_client = _FakeRESTClient()
        self._schema_qualified_name = cfg.schema_qualified_name
        self.upserts = []

    def upsert(self, records):
        self.upserts.append([r.identifier for r in records])

    def table_qualified_name(self, record):
        return f"{self._schema_qualified_name}/{record.identifier}"


def test_sanity_check_runs_with_sample_data(monkeypatch: pytest.MonkeyPatch) -> None:
    config = Config(
        dxr_base_url="https://dxr.example.com/api",
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
        log_level="INFO",
    )

    classification = Classification(
        identifier="classification-1",
        name="Sample Label",
        type="ANNOTATOR",
        subtype=None,
        description="Test label",
        link=None,
        search_link=None,
    )
    files = [
        {
            "id": "file-1",
            "name": "sample.csv",
            "path": "s3://bucket/sample.csv",
            "labels": [{"id": "classification-1"}],
        }
    ]

    fake_dxr = _FakeDXR([classification], files)
    monkeypatch.setattr(sanity_check.Config, "from_env", lambda: config)
    monkeypatch.setattr(sanity_check, "DXRClient", lambda *args, **kwargs: fake_dxr)
    monkeypatch.setattr(sanity_check, "AtlanUploader", _FakeUploader)

    sanity_check.run(sample_labels=1, max_files=10)

