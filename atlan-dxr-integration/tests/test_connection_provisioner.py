"""Behavioural tests for ConnectionProvisioner interacting with the REST shim."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import pytest

from atlan_dxr_integration.atlan_service import AtlanRequestError
from atlan_dxr_integration.connection_utils import ConnectionProvisioner


class _StubRESTClient:
    """Minimal stub that mimics the AtlanRESTClient surface used by ConnectionProvisioner."""

    def __init__(
        self,
        *,
        assets: Optional[Dict[Tuple[str, str], Dict[str, Any]]] = None,
        role_id: Optional[str] = None,
        upsert_response: Optional[Dict[str, Any]] = None,
        upsert_error: Optional[Exception] = None,
        fallback_after_upsert: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.assets = assets or {}
        self.role_id = role_id
        self.upsert_response = upsert_response
        self.upsert_error = upsert_error
        self.fallback_after_upsert = fallback_after_upsert

        self.upsert_calls: List[List[Dict[str, Any]]] = []
        self.get_asset_calls: List[Tuple[str, str]] = []

    @staticmethod
    def _key(type_name: str, qualified_name: str) -> Tuple[str, str]:
        return (type_name, qualified_name)

    def get_asset(self, type_name: str, qualified_name: str) -> Optional[Dict[str, Any]]:
        self.get_asset_calls.append((type_name, qualified_name))
        return self.assets.get(self._key(type_name, qualified_name))

    def upsert_assets(self, assets: List[Dict[str, Any]]) -> Dict[str, Any]:
        self.upsert_calls.append(list(assets))

        if self.upsert_error is not None:
            if self.fallback_after_upsert is not None:
                entity = assets[0]
                key = self._key(entity["typeName"], entity["attributes"]["qualifiedName"])
                self.assets[key] = self.fallback_after_upsert
            raise self.upsert_error

        return self.upsert_response or {}

    def get_role_id(self, role_name: str) -> Optional[str]:
        return self.role_id if role_name == "$admin" else None


def _connection_entity(
    *,
    qualified_name: str,
    name: str = "dxr-unstructured-attributes",
    guid: str = "connection-guid",
    admin_users: Optional[List[str]] = None,
) -> Dict[str, Any]:
    return {
        "entity": {
            "typeName": "Connection",
            "guid": guid,
            "attributes": {
                "qualifiedName": qualified_name,
                "name": name,
                "adminUsers": admin_users or [],
            },
        }
    }


def test_ensure_connection_reuses_existing_asset() -> None:
    qualified_name = "default/custom/dxr-unstructured-attributes"
    client = _StubRESTClient(
        assets={("Connection", qualified_name): _connection_entity(qualified_name=qualified_name)}
    )
    provisioner = ConnectionProvisioner(client)

    handle = provisioner.ensure_connection(
        qualified_name=qualified_name,
        connection_name="dxr-unstructured-attributes",
        connector_name="Custom",
    )

    assert handle.qualified_name == qualified_name
    assert handle.connection.guid == "connection-guid"
    assert client.upsert_calls == []


def test_ensure_connection_creates_missing_asset_with_admin_role() -> None:
    qualified_name = "default/custom/dxr-unstructured-attributes"
    admin_guid = "admin-role-guid"
    upsert_response = {
        "mutatedEntities": {
            "CREATE": [
                {
                    "typeName": "Connection",
                    "guid": "new-connection-guid",
                    "attributes": {
                        "qualifiedName": qualified_name,
                        "name": "dxr-unstructured-attributes",
                    },
                }
            ]
        }
    }
    client = _StubRESTClient(role_id=admin_guid, upsert_response=upsert_response)
    provisioner = ConnectionProvisioner(client, default_admin_user="token-user")

    handle = provisioner.ensure_connection(
        qualified_name=qualified_name,
        connection_name="dxr-unstructured-attributes",
        connector_name="custom",
    )

    assert client.upsert_calls, "Connection creation should call upsert_assets"
    submitted_entity = client.upsert_calls[0][0]
    assert submitted_entity["attributes"]["adminRoles"] == [admin_guid]
    assert submitted_entity["attributes"]["adminUsers"] == ["token-user"]
    assert handle.connection.guid == "new-connection-guid"


def test_ensure_connection_falls_back_when_upsert_forbidden() -> None:
    qualified_name = "default/custom/dxr-unstructured-attributes"
    fallback_asset = _connection_entity(qualified_name=qualified_name, guid="fallback-guid")
    client = _StubRESTClient(
        upsert_error=AtlanRequestError(
            "Forbidden",
            status_code=403,
            details={"errorId": "ATLAS-403-00-001"},
        ),
        fallback_after_upsert=fallback_asset,
    )
    provisioner = ConnectionProvisioner(client)

    handle = provisioner.ensure_connection(
        qualified_name=qualified_name,
        connection_name="dxr-unstructured-attributes",
        connector_name="custom",
    )

    assert handle.connection.guid == "fallback-guid"
    assert handle.connection.attributes.qualifiedName == qualified_name
    assert client.upsert_calls, "Upsert should still be attempted before falling back"


def test_ensure_connection_handles_upsert_without_mutation() -> None:
    qualified_name = "default/custom/dxr-unstructured-attributes"
    # Successful response without mutated entities triggers the fallback path.
    client = _StubRESTClient(
        upsert_response={"mutatedEntities": {}},
        assets={},  # subsequent retrieval will yield 403 via AtlanRequestError stub
    )
    # Make get_asset raise 403 by configuring upsert_error fallback behaviour.
    client.upsert_error = None

    # Monkeypatch get_asset to simulate HTTP 403.
    def _deny_fetch(type_name: str, qn: str):
        raise AtlanRequestError("Forbidden", status_code=403, details=None)

    client.get_asset = _deny_fetch  # type: ignore[assignment]

    provisioner = ConnectionProvisioner(
        client,
        default_admin_user="token-user",
    )

    handle = provisioner.ensure_connection(
        qualified_name=qualified_name,
        connection_name="dxr-unstructured-attributes",
        connector_name="custom",
    )

    assert handle.connection.attributes.qualifiedName == qualified_name
    assert handle.connection.guid is None
    assert handle.connection.attributes.adminUsers == ["token-user"]
