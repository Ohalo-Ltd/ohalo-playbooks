"""Tests for connection management helpers."""

from __future__ import annotations

from typing import Any

import pytest

from atlan_dxr_integration import connection_manager
from atlan_dxr_integration.config import Config


class _StubRESTClient:
    def __init__(self, *, connections=None, typedefs=None):
        self.connections = list(connections or [])
        self.typedefs = list(typedefs or [])
        self.deleted: list[str] = []
        self.purged: list[str] = []
        self.purged_typedefs: list[str] = []

    def search_assets(self, payload: dict[str, Any]):
        must = payload.get("dsl", {}).get("query", {}).get("bool", {}).get("must", [])
        values = {
            term.get("term", {}).get("__typeName.keyword")
            for term in must
            if isinstance(term, dict)
        }
        if "Connection" in values:
            return {"entities": list(self.connections)}
        return {"entities": []}

    def delete_asset(self, guid: str):
        self.deleted.append(guid)

    def purge_asset(self, guid: str):
        self.purged.append(guid)

    def list_classification_typedefs(self):
        return list(self.typedefs)

    def purge_typedef(self, name: str):
        self.purged_typedefs.append(name)


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


def test_purge_connection_soft_then_hard(monkeypatch: pytest.MonkeyPatch) -> None:
    config = _build_config()
    connection = {
        "guid": "conn-guid",
        "attributes": {
            "name": "dxr-unstructured-attributes",
            "qualifiedName": "default/custom/dxr-unstructured-attributes",
        },
    }
    client = _StubRESTClient(connections=[connection])

    connection_manager.purge_connection(
        config,
        delete_type=connection_manager.DeleteType.PURGE,
        soft_delete_first=True,
        client=client,
    )

    assert client.deleted == ["conn-guid"]
    assert client.purged == ["conn-guid"]


def test_purge_connection_skips_soft_delete(monkeypatch: pytest.MonkeyPatch) -> None:
    config = _build_config()
    connection = {
        "guid": "conn-guid",
        "attributes": {
            "name": "dxr-unstructured-attributes",
            "qualifiedName": "default/custom/dxr-unstructured-attributes",
        },
    }
    client = _StubRESTClient(connections=[connection])

    connection_manager.purge_connection(
        config,
        delete_type=connection_manager.DeleteType.HARD,
        soft_delete_first=False,
        client=client,
    )

    assert client.deleted == []
    assert client.purged == ["conn-guid"]


def test_purge_connection_purges_namespace_tags(monkeypatch: pytest.MonkeyPatch) -> None:
    config = _build_config()
    connection = {
        "guid": "conn-guid",
        "attributes": {
            "name": "dxr-unstructured-attributes",
            "qualifiedName": "default/custom/dxr-unstructured-attributes",
        },
    }
    typedefs = [
        {"name": "hash-1", "displayName": "DXR :: Example"},
        {"name": "hash-2", "displayName": "Other :: Skip"},
    ]
    client = _StubRESTClient(connections=[connection], typedefs=typedefs)

    connection_manager.purge_connection(
        config,
        delete_type=connection_manager.DeleteType.PURGE,
        soft_delete_first=False,
        purge_tags=True,
        client=client,
    )

    assert "hash-1" in client.purged_typedefs
    assert "hash-2" not in client.purged_typedefs


def test_purge_connection_raises_when_missing():
    config = _build_config()
    client = _StubRESTClient(connections=[])

    with pytest.raises(SystemExit) as exc_info:
        connection_manager.purge_connection(config, client=client)

    assert "nothing to purge" in str(exc_info.value).lower()


def test_purge_connection_requires_guid():
    config = _build_config()
    connection = {
        "guid": None,
        "attributes": {
            "name": "dxr-datasource-foo",
            "qualifiedName": "default/custom/dxr-datasource-foo",
        },
    }
    client = _StubRESTClient(connections=[connection])

    with pytest.raises(SystemExit) as exc_info:
        connection_manager.purge_connection(config, client=client)

    assert "does not expose a guid" in str(exc_info.value).lower()


def test_purge_environment_wrapper(monkeypatch: pytest.MonkeyPatch) -> None:
    config = _build_config()
    calls: dict[str, Any] = {}

    def _mock_purge_connection(*args, **kwargs):
        calls.update(kwargs)

    monkeypatch.setattr(connection_manager, "purge_connection", _mock_purge_connection)

    connection_manager.purge_environment(config, client="client")

    assert calls["delete_type"] == connection_manager.DeleteType.PURGE
    assert calls["soft_delete_first"] is False
    assert calls["purge_tags"] is True
    assert calls["client"] == "client"
