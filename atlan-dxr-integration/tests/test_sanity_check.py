"""Tests for the sanity check helper."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from atlan_dxr_integration import sanity_check
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


def test_sanity_check_runs_with_sample_data(monkeypatch: pytest.MonkeyPatch) -> None:
    """Sanity check uploads records and verifies them in Atlan."""

    config = Config(
        dxr_base_url="https://dxr.example.com/api",
        dxr_pat="dxr-token",
        dxr_classification_types=None,
        dxr_sample_file_limit=5,
        dxr_file_fetch_limit=200,
        atlan_base_url="https://atlan.example.com",
        atlan_api_token="atlan-token",
        atlan_connection_qualified_name="default/connection",
        atlan_connection_name="dxr-connection",
        atlan_connector_name="custom-connector",
        atlan_database_name="dxr",
        atlan_schema_name="labels",
        atlan_dataset_path_prefix="dxr",
        atlan_batch_size=10,
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

    captured = {}

    class _FakeUploader:
        def __init__(self, cfg):
            captured["config"] = cfg
            self._schema_qualified_name = cfg.schema_qualified_name
            self._lookups: list[str] = []
            captured["uploader"] = self
            self.client = SimpleNamespace(
                asset=SimpleNamespace(
                    get_by_qualified_name=self._record_lookup,
                )
            )

        def upsert(self, records):
            captured.setdefault("upserted", []).append([r.identifier for r in records])

        def table_qualified_name(self, record):
            return f"{self._schema_qualified_name}/{record.identifier}"

        def _record_lookup(self, qualified_name, asset_type, **kwargs):
            captured.setdefault("lookups", []).append(qualified_name)
            return SimpleNamespace(attributes=SimpleNamespace(name="Sanity Dataset"))

    monkeypatch.setattr(sanity_check, "AtlanUploader", _FakeUploader)

    sanity_check.run(sample_labels=1, max_files=10)

    assert captured["upserted"] == [["classification-1"]]
    assert captured["lookups"] == ["default/connection/dxr/labels/classification-1"]
