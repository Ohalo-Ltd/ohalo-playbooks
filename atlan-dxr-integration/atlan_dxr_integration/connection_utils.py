"""Helpers for provisioning Atlan connections, databases, schemas, and domains."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, Dict, Optional, List

from .atlan_service import AtlanRESTClient, AtlanRequestError

LOGGER = logging.getLogger(__name__)


def normalise_connector_name(name: str) -> str:
    return (name or "custom").strip() or "custom"


@dataclass
class ConnectionRecord:
    guid: Optional[str]
    attributes: SimpleNamespace


@dataclass
class ConnectionHandle:
    connector_name: str
    qualified_name: str
    connection: ConnectionRecord


class ConnectionProvisioner:
    """Create or reuse Atlan assets needed for the DXR integrations."""

    def __init__(
        self,
        client: AtlanRESTClient,
        default_admin_user: Optional[str] = None,
    ) -> None:
        self._client = client
        self._default_admin_user = default_admin_user

    def ensure_connection(
        self,
        *,
        qualified_name: str,
        connection_name: str,
        connector_name: str,
        domain_name: Optional[str] = None,
        admin_user: Optional[str] = None,
    ) -> ConnectionHandle:
        connector = normalise_connector_name(connector_name)
        connection = self._get_entity("Connection", qualified_name)
        resolved_admin_user = admin_user or self._default_admin_user
        admin_users: list[str] = [resolved_admin_user] if resolved_admin_user else []
        admin_roles: list[str] = []
        admin_role_guid = self._client.get_role_id("$admin")
        if admin_role_guid:
            admin_roles.append(admin_role_guid)

        if not connection:
            LOGGER.info("Connection '%s' not found; creating it", qualified_name)
            payload = {
                "typeName": "Connection",
                "attributes": {
                    "qualifiedName": qualified_name,
                    "name": connection_name,
                    "connectorName": connector,
                    "connectionType": connector,
                    "adminRoles": admin_roles,
                    "adminGroups": [],
                    "adminUsers": admin_users,
                },
            }
            connection = self._upsert_single(payload, "Connection")

        attributes = dict(connection.get("attributes") or {})
        actual_qn = attributes.get("qualifiedName") or qualified_name
        if actual_qn != qualified_name:
            LOGGER.warning(
                "Configured connection QN '%s' differs from Atlan record '%s'. Using returned value.",
                qualified_name,
                actual_qn,
            )
            qualified_name = actual_qn

        needs_update = False
        existing_roles = set(attributes.get("adminRoles") or [])
        for role in admin_roles:
            if role and role not in existing_roles:
                existing_roles.add(role)
                needs_update = True

        existing_users = set(attributes.get("adminUsers") or [])
        for user in admin_users:
            if user and user not in existing_users:
                existing_users.add(user)
                needs_update = True

        if needs_update:
            attributes["adminRoles"] = list(existing_roles)
            attributes["adminUsers"] = list(existing_users)
            update_payload = {
                "typeName": "Connection",
                "guid": connection.get("guid"),
                "attributes": attributes,
            }
            connection = self._upsert_single(update_payload, "Connection")

        record = _to_record(connection)
        if domain_name:
            LOGGER.debug(
                "Domain assignment for connection '%s' requested but currently not implemented; skipping.",
                qualified_name,
            )

        return ConnectionHandle(
            connector_name=connector,
            qualified_name=qualified_name,
            connection=record,
        )

    def ensure_database(
        self,
        *,
        name: str,
        qualified_name: str,
        connection_handle: ConnectionHandle,
    ) -> str:
        database = self._get_entity("Database", qualified_name)
        if database:
            return database["attributes"].get("qualifiedName") or qualified_name

        LOGGER.info("Creating database '%s'", qualified_name)
        payload = {
            "typeName": "Database",
            "attributes": {
                "qualifiedName": qualified_name,
                "name": name,
                "connectionQualifiedName": connection_handle.qualified_name,
                "connectionName": connection_handle.connection.attributes.name,
            },
        }
        entity = self._upsert_single(payload, "Database")
        return entity["attributes"].get("qualifiedName") or qualified_name

    def ensure_schema(
        self,
        *,
        name: str,
        qualified_name: str,
        database_qualified_name: str,
        connection_handle: ConnectionHandle,
    ) -> str:
        schema = self._get_entity("Schema", qualified_name)
        if schema:
            return schema["attributes"].get("qualifiedName") or qualified_name

        LOGGER.info("Creating schema '%s'", qualified_name)
        payload = {
            "typeName": "Schema",
            "attributes": {
                "qualifiedName": qualified_name,
                "name": name,
                "connectionQualifiedName": connection_handle.qualified_name,
                "connectionName": connection_handle.connection.attributes.name,
                "databaseQualifiedName": database_qualified_name,
                "databaseName": name,
            },
        }
        entity = self._upsert_single(payload, "Schema")
        return entity["attributes"].get("qualifiedName") or qualified_name

    def _get_entity(self, type_name: str, qualified_name: str) -> Optional[Dict[str, Any]]:
        try:
            data = self._client.get_asset(type_name, qualified_name)
        except AtlanRequestError as exc:
            if exc.status_code in (403, 404):
                LOGGER.warning(
                    "Unable to fetch %s '%s' due to %s: treating as missing.",
                    type_name,
                    qualified_name,
                    f"HTTP {exc.status_code}",
                )
                return None
            raise
        return _extract_entity(data)

    def _upsert_single(self, entity: Dict[str, Any], type_name: str) -> Dict[str, Any]:
        try:
            response = self._client.upsert_assets([entity])
        except AtlanRequestError as exc:
            if exc.status_code == 403:
                qualified_name = entity.get("attributes", {}).get("qualifiedName", "")
                LOGGER.warning(
                    "Insufficient privileges to upsert %s '%s'; assuming it already exists.",
                    type_name,
                    qualified_name,
                )
                fallback = self._get_entity(type_name, qualified_name)
                if fallback:
                    return fallback
                return {
                    "typeName": type_name,
                    "attributes": dict(entity.get("attributes", {})),
                }
            raise
        mutated = _extract_mutated_entity(response, type_name)
        if mutated:
            return mutated
        retrieved = self._get_entity(type_name, entity["attributes"]["qualifiedName"])
        if retrieved:
            return retrieved
        LOGGER.warning(
            "Unable to confirm %s '%s' after upsert; proceeding with submitted attributes.",
            type_name,
            entity["attributes"]["qualifiedName"],
        )
        return {
            "typeName": type_name,
            "attributes": dict(entity.get("attributes", {})),
        }


def _extract_entity(payload: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(payload, dict):
        return None
    if "entity" in payload and isinstance(payload["entity"], dict):
        return payload["entity"]
    if "entities" in payload and isinstance(payload["entities"], list):
        for entity in payload["entities"]:
            if isinstance(entity, dict):
                return entity
    return None


def _extract_mutated_entity(response: Any, type_name: str) -> Optional[Dict[str, Any]]:
    if not isinstance(response, dict):
        return None
    mutated = response.get("mutatedEntities") or {}
    for bucket in ("CREATE", "UPDATE", "PARTIAL_UPDATE"):
        entities = mutated.get(bucket) or []
        for entity in entities:
            if isinstance(entity, dict) and entity.get("typeName") == type_name:
                return entity
    return None


def _to_record(entity: Dict[str, Any]) -> ConnectionRecord:
    attrs = entity.get("attributes") or {}
    return ConnectionRecord(
        guid=entity.get("guid"),
        attributes=SimpleNamespace(**attrs),
    )


__all__ = ["ConnectionProvisioner", "ConnectionHandle", "normalise_connector_name"]
