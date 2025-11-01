"""Live tests for ConnectionProvisioner against a real Atlan tenant.

The test is gated behind environment variables so it only runs when explicit
credentials are supplied. Configure the following variables before invoking:

* ``ATLAN_TEST_BASE_URL`` – tenant base URL (for example ``https://xyz.atlan.com``)
* ``ATLAN_TEST_API_TOKEN`` – API token with connection-admin scope
"""

from __future__ import annotations

import os
import uuid
from typing import Optional

import pytest

from atlan_dxr_integration.atlan_service import AtlanRESTClient
from atlan_dxr_integration.connection_utils import ConnectionProvisioner

pytestmark = pytest.mark.integration


def _get_env(name: str) -> Optional[str]:
    value = os.getenv(name)
    return value.strip() if value else None


def _require_env(name: str) -> str:
    value = _get_env(name)
    if not value:
        pytest.skip(f"Environment variable {name} not set; skipping live Atlan test.")
    return value


def test_ensure_connection_live_roundtrip() -> None:
    base_url = _require_env("ATLAN_TEST_BASE_URL")
    api_token = _require_env("ATLAN_TEST_API_TOKEN")

    admin_user = _get_env("ATLAN_TEST_API_TOKEN_USER")

    client = AtlanRESTClient(base_url=base_url, api_key=api_token)
    provisioner = ConnectionProvisioner(client, default_admin_user=admin_user)

    random_suffix = uuid.uuid4().hex[:12]
    qualified_name = f"default/custom/dxr-integration-test-{random_suffix}"
    connection_name = f"dxr-integration-test-{random_suffix}"

    try:
        handle = provisioner.ensure_connection(
            qualified_name=qualified_name,
            connection_name=connection_name,
            connector_name="custom",
        )

        assert handle.qualified_name == qualified_name
        assert handle.connection.guid, "Expected Atlan to return a GUID for the connection."

        # Reusing the same qualified name should avoid another creation call.
        second_handle = provisioner.ensure_connection(
            qualified_name=qualified_name,
            connection_name=connection_name,
            connector_name="custom",
        )
        assert second_handle.connection.guid == handle.connection.guid
    finally:
        try:
            guid = client.get_asset("Connection", qualified_name)
        except Exception:
            guid = None
        entity = guid.get("entity") if isinstance(guid, dict) else None
        conn_guid = entity.get("guid") if isinstance(entity, dict) else None
        if conn_guid:
            client.delete_asset(conn_guid)
