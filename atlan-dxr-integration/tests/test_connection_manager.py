"""Tests for connection management helpers."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from pyatlan.errors import ErrorCode, NotFoundError
from pyatlan.model.enums import AtlanDeleteType

from atlan_dxr_integration.connection_manager import purge_connection
from atlan_dxr_integration.config import Config


class _FakeAssetService:
    def __init__(self, *, connections=None):
        self.connections = list(connections or [])
        self.deleted: list[str] = []
        self.purged: list[tuple[str, AtlanDeleteType]] = []

    def search(self, request):  # pragma: no cover - exercised indirectly
        return SimpleNamespace(entities=list(self.connections))

    def delete_by_guid(self, guid):
        self.deleted.append(guid)
        return SimpleNamespace(request_id="soft-req")

    def purge_by_guid(self, guid, delete_type: AtlanDeleteType):
        self.purged.append((guid, delete_type))
        return SimpleNamespace(request_id="hard-req")


class _FakeAtlanClient:
    def __init__(self, asset_service: _FakeAssetService):
        self.asset = asset_service


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
        log_level="INFO",
    )


def test_purge_connection_soft_then_hard(monkeypatch: pytest.MonkeyPatch) -> None:
    config = _build_config()
    connection = SimpleNamespace(
        guid="conn-guid",
        attributes=SimpleNamespace(
            name="dxr-unstructured-attributes",
            qualified_name="default/custom/dxr-unstructured-attributes",
        ),
    )
    asset_service = _FakeAssetService(connections=[connection])
    client = _FakeAtlanClient(asset_service)

    purge_connection(
        config,
        delete_type=AtlanDeleteType.PURGE,
        soft_delete_first=True,
        client=client,
    )

    assert asset_service.deleted == ["conn-guid"]
    assert asset_service.purged == [("conn-guid", AtlanDeleteType.PURGE)]


def test_purge_connection_skips_soft_delete(monkeypatch: pytest.MonkeyPatch) -> None:
    config = _build_config()
    connection = SimpleNamespace(
        guid="conn-guid",
        attributes=SimpleNamespace(
            name="dxr-unstructured-attributes",
            qualified_name="default/custom/dxr-unstructured-attributes",
        ),
    )
    asset_service = _FakeAssetService(connections=[connection])
    client = _FakeAtlanClient(asset_service)

    purge_connection(
        config,
        delete_type=AtlanDeleteType.HARD,
        soft_delete_first=False,
        client=client,
    )

    assert asset_service.deleted == []
    assert asset_service.purged == [("conn-guid", AtlanDeleteType.HARD)]


def test_purge_connection_raises_when_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    config = _build_config()
    asset_service = _FakeAssetService(connections=[])
    client = _FakeAtlanClient(asset_service)

    with pytest.raises(SystemExit) as exc_info:
        purge_connection(config, client=client)

    assert "nothing to purge" in str(exc_info.value).lower()


def test_purge_connection_requires_guid(monkeypatch: pytest.MonkeyPatch) -> None:
    config = _build_config()
    connection = SimpleNamespace(
        guid=None,
        attributes=SimpleNamespace(name="dxr-datasource-foo", qualified_name="default/custom/dxr-datasource-foo"),
    )
    asset_service = _FakeAssetService(connections=[connection])
    client = _FakeAtlanClient(asset_service)

    with pytest.raises(SystemExit) as exc_info:
        purge_connection(config, client=client)

    assert "does not expose a guid" in str(exc_info.value).lower()
