"""Helpers for provisioning Atlan connections, databases, schemas, and domains."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Dict, Optional, Tuple

from pyatlan.client.atlan import AtlanClient
from pyatlan.errors import AtlanError, NotFoundError
from pyatlan.model.assets.connection import Connection
from pyatlan.model.assets.core.database import Database
from pyatlan.model.assets.core.schema import Schema
from pyatlan.model.assets.core.data_domain import DataDomain
from pyatlan.model.enums import AtlanConnectorType

LOGGER = logging.getLogger(__name__)


def resolve_connector_type(name: str) -> AtlanConnectorType:
    normalised = (name or "").strip()
    if not normalised:
        return AtlanConnectorType.CUSTOM

    token = normalised.replace("-", "_").replace(" ", "_").upper()
    if token in AtlanConnectorType.__members__:
        return AtlanConnectorType[token]

    lowered = normalised.lower()
    for member in AtlanConnectorType:
        if member.value.lower() == lowered:
            return member

    LOGGER.debug(
        "Unknown connector '%s'; defaulting to CUSTOM",
        name,
    )
    return AtlanConnectorType.CUSTOM


class DomainManager:
    """Ensure data domains exist and expose their GUIDs."""

    def __init__(self, client: AtlanClient) -> None:
        self._client = client
        self._cache: Dict[str, DataDomain] = {}

    def ensure(self, name: str) -> DataDomain:
        normalized = name.strip()
        if not normalized:
            raise ValueError("Domain name must be non-empty")

        if normalized in self._cache:
            return self._cache[normalized]

        try:
            domain = self._client.asset.find_domain_by_name(normalized)
        except NotFoundError:
            LOGGER.info("Creating Atlan domain '%s'", normalized)
            domain = DataDomain.creator(name=normalized)
            response = self._client.asset.save(domain)
            created = response.assets_created(DataDomain)
            if created:
                domain = created[0]
        self._cache[normalized] = domain
        return domain


@dataclass
class ConnectionHandle:
    connector_type: AtlanConnectorType
    qualified_name: str
    connection: Connection


class ConnectionProvisioner:
    """Create or reuse Atlan assets needed for the DXR integrations."""

    def __init__(self, client: AtlanClient) -> None:
        self._client = client
        self._domain_manager = DomainManager(client)

    def ensure_connection(
        self,
        *,
        qualified_name: str,
        connection_name: str,
        connector_name: str,
        domain_name: Optional[str] = None,
    ) -> ConnectionHandle:
        connector_type = resolve_connector_type(connector_name)

        try:
            connection = self._client.asset.get_by_qualified_name(
                qualified_name,
                Connection,
                ignore_relationships=True,
            )
            actual_qn = connection.attributes.qualified_name or qualified_name
            if actual_qn != qualified_name:
                LOGGER.warning(
                    "Configured connection QN '%s' differs from Atlan record '%s'. Using returned value.",
                    qualified_name,
                    actual_qn,
                )
                qualified_name = actual_qn
            connection_handle = ConnectionHandle(
                connector_type=connector_type,
                qualified_name=qualified_name,
                connection=connection,
            )
        except NotFoundError:
            LOGGER.info("Connection '%s' not found; creating it", qualified_name)
            admin_role_guid = str(
                self._client.role_cache.get_id_for_name("$admin")
            )
            connection = Connection.creator(
                client=self._client,
                name=connection_name,
                connector_type=connector_type,
                admin_roles=[admin_role_guid],
            )
            attrs = connection.attributes
            attrs.qualified_name = qualified_name
            attrs.name = connection_name
            response = self._client.asset.save(connection)
            created = response.assets_created(Connection)
            if created:
                connection = created[0]
            connection_handle = ConnectionHandle(
                connector_type=connector_type,
                qualified_name=qualified_name,
                connection=connection,
            )
        except AtlanError as exc:
            raise RuntimeError(
                f"Unable to resolve connection '{qualified_name}': {exc}"
            ) from exc

        if domain_name:
            self._ensure_domain_assignment(connection_handle.connection, domain_name)

        return connection_handle

    def ensure_database(
        self,
        *,
        name: str,
        qualified_name: str,
        connection_handle: ConnectionHandle,
    ) -> str:
        try:
            database = self._client.asset.get_by_qualified_name(
                qualified_name,
                Database,
                ignore_relationships=True,
            )
            actual_qn = database.attributes.qualified_name or qualified_name
            return actual_qn
        except NotFoundError:
            LOGGER.info("Creating database '%s'", qualified_name)
            database = Database.creator(
                name=name,
                connection_qualified_name=connection_handle.qualified_name,
            )
            attrs = database.attributes
            attrs.qualified_name = qualified_name
            attrs.connection_name = connection_handle.connection.attributes.name
            attrs.connection_qualified_name = connection_handle.qualified_name
            response = self._client.asset.save(database)
            created = response.assets_created(Database)
            if created:
                database = created[0]
            return database.attributes.qualified_name

    def ensure_schema(
        self,
        *,
        name: str,
        qualified_name: str,
        database_qualified_name: str,
        connection_handle: ConnectionHandle,
    ) -> str:
        try:
            schema = self._client.asset.get_by_qualified_name(
                qualified_name,
                Schema,
                ignore_relationships=True,
            )
            actual_qn = schema.attributes.qualified_name or qualified_name
            return actual_qn
        except NotFoundError:
            LOGGER.info("Creating schema '%s'", qualified_name)
            schema = Schema.creator(
                name=name,
                connection_qualified_name=connection_handle.qualified_name,
                database_qualified_name=database_qualified_name,
            )
            attrs = schema.attributes
            attrs.qualified_name = qualified_name
            attrs.connection_name = connection_handle.connection.attributes.name
            attrs.connection_qualified_name = connection_handle.qualified_name
            attrs.database_name = name if not attrs.database_name else attrs.database_name
            attrs.database_qualified_name = database_qualified_name
            response = self._client.asset.save(schema)
            created = response.assets_created(Schema)
            if created:
                schema = created[0]
            return schema.attributes.qualified_name

    def ensure_domain(self, name: str) -> DataDomain:
        return self._domain_manager.ensure(name)

    def _ensure_domain_assignment(
        self, connection: Connection, domain_name: str
    ) -> None:
        domain = self._domain_manager.ensure(domain_name)
        attrs = connection.attributes
        existing = set(attrs.domain_g_u_i_ds or [])
        if domain.guid and str(domain.guid) not in existing:
            attrs.domain_g_u_i_ds = existing.union({str(domain.guid)})
            try:
                self._client.asset.save(connection)
            except AtlanError as exc:  # pragma: no cover - defensive
                LOGGER.warning(
                    "Failed to update domain assignment for connection '%s': %s",
                    attrs.qualified_name,
                    exc,
                )

__all__ = ["ConnectionProvisioner", "ConnectionHandle", "DomainManager", "resolve_connector_type"]
