"""Utilities for writing DXR-derived tables into Atlan."""

from __future__ import annotations

import logging
from typing import Iterable, List, Optional

from pyatlan.client.atlan import AtlanClient
from pyatlan.errors import (
    AtlanError,
    NotFoundError,
    PermissionError as AtlanPermissionError,
)
from pyatlan.model.assets.connection import Connection
from pyatlan.model.assets.core.database import Database
from pyatlan.model.assets.core.schema import Schema
from pyatlan.model.assets.core.table import Table
from pyatlan.model.enums import AtlanConnectorType

from .config import Config
from .dataset_builder import DatasetRecord

LOGGER = logging.getLogger(__name__)


class AtlanUploadError(RuntimeError):
    """Raised when Atlan rejects a table upsert request."""


class AtlanUploader:
    """Wrapper around Atlan's Asset API for upserting DXR tables."""

    def __init__(self, config: Config) -> None:
        self._config = config
        self._client = AtlanClient(
            base_url=config.atlan_base_url,
            api_key=config.atlan_api_token,
        )
        connector_type, connection_qn = self._ensure_connection_exists()
        self._connector_type = connector_type or AtlanConnectorType.CUSTOM
        self._connection_qualified_name = connection_qn
        self._database_qualified_name = self._ensure_database_exists(connection_qn)
        self._schema_qualified_name = self._ensure_schema_exists(
            self._database_qualified_name, connection_qn
        )

    @property
    def client(self) -> AtlanClient:
        """Expose the underlying Atlan client for verification helpers."""

        return self._client

    def upsert(self, records: Iterable[DatasetRecord]) -> None:
        batch: List[Table] = []
        for record in records:
            table = self._build_table(record)
            batch.append(table)
            if len(batch) >= self._config.atlan_batch_size:
                self._save_batch(batch)
                batch = []
        if batch:
            self._save_batch(batch)

    def table_qualified_name(self, record: DatasetRecord) -> str:
        return f"{self._schema_qualified_name}/{self._table_name(record)}"

    def _build_table(self, record: DatasetRecord) -> Table:
        table_name = self._table_name(record)
        table = Table.creator(
            name=table_name,
            schema_qualified_name=self._schema_qualified_name,
            schema_name=self._config.atlan_schema_name,
            database_name=self._config.atlan_database_name,
            database_qualified_name=self._database_qualified_name,
            connection_qualified_name=self._connection_qualified_name,
        )

        attrs = table.attributes
        attrs.qualified_name = self.table_qualified_name(record)
        attrs.display_name = record.name
        attrs.description = record.classification.description
        attrs.user_description = record.description
        attrs.source_url = record.source_url
        attrs.connector_name = self._connector_type.value
        attrs.connection_name = self._config.atlan_connection_name

        return table

    def _save_batch(self, batch: List[Table]) -> None:
        try:
            response = self._client.asset.save(batch)
            created = response.assets_created(Table)
            updated = response.assets_updated(Table)
            partial = response.assets_partially_updated(Table)
            mutated_count = len(created) + len(updated) + len(partial)
            if mutated_count == 0:
                if self._tables_exist(batch):
                    LOGGER.info(
                        "No DXR table changes detected in Atlan (request id: %s); assets already up to date.",
                        getattr(response, "request_id", "unknown"),
                    )
                    return
                message = (
                    "Atlan acknowledged the upsert request but did not mutate any "
                    "tables. Verify the connection, connector name, and service "
                    "permissions."
                )
                LOGGER.error(message)
                raise AtlanUploadError(message)
            LOGGER.info(
                "Upserted %d DXR table(s) into Atlan (request id: %s)",
                mutated_count,
                getattr(response, "request_id", "unknown"),
            )
        except AtlanPermissionError as exc:
            message = (
                "Atlan rejected the table upsert due to insufficient permissions. "
                "Verify that the provided API token can create table assets for "
                f"connection '{self._config.atlan_connection_name}'."
            )
            LOGGER.error("%s (original error: %s)", message, exc)
            raise AtlanUploadError(message) from exc
        except AtlanError as exc:
            message = "Atlan rejected the table upsert request."
            LOGGER.error("%s (original error: %s)", message, exc)
            raise AtlanUploadError(message) from exc

    def _tables_exist(self, batch: Iterable[Table]) -> bool:
        """Confirm each table in the batch exists in Atlan."""

        for table in batch:
            qualified_name = table.attributes.qualified_name
            try:
                self._client.asset.get_by_qualified_name(
                    qualified_name,
                    Table,
                    ignore_relationships=True,
                )
            except AtlanError:
                return False
        return True

    def _ensure_connection_exists(self) -> tuple[AtlanConnectorType, str]:
        qualified_name = self._config.atlan_connection_qualified_name
        connection_name = self._config.atlan_connection_name

        connector_type = self._resolve_connector_type(
            self._config.atlan_connector_name
        )

        try:
            connection = self._client.asset.get_by_qualified_name(
                qualified_name,
                Connection,
                ignore_relationships=True,
            )
            actual_qn = connection.attributes.qualified_name
            connector_name = connection.attributes.connector_name
            resolved_type = (
                self._resolve_connector_type(connector_name)
                if connector_name
                else connector_type
            )
            if actual_qn and actual_qn != qualified_name:
                LOGGER.warning(
                    "Configured connection qualified name '%s' differs from Atlan record '%s'. "
                    "Using the value reported by Atlan.",
                    qualified_name,
                    actual_qn,
                )
                qualified_name = actual_qn
            LOGGER.debug(
                "Atlan connection '%s' already exists; skipping creation.",
                qualified_name,
            )
            return resolved_type, qualified_name
        except NotFoundError:
            LOGGER.info(
                "Atlan connection '%s' not found; attempting to create it.",
                qualified_name,
            )
        except AtlanError as exc:
            message = (
                f"Unable to verify existence of Atlan connection '{qualified_name}'. "
                "Ensure the API token can read connections or pre-create the connection."
            )
            LOGGER.error("%s (original error: %s)", message, exc)
            raise AtlanUploadError(message) from exc

        try:
            admin_role_guid = str(self._client.role_cache.get_id_for_name("$admin"))
        except AtlanError as exc:
            message = (
                "Unable to resolve the '$admin' role while creating the connection. "
                "Provide an API token with admin privileges or create the connection manually."
            )
            LOGGER.error("%s (original error: %s)", message, exc)
            raise AtlanUploadError(message) from exc
        except AttributeError as exc:
            message = (
                "Atlan client is missing role cache access; cannot auto-create the connection."
            )
            LOGGER.error(message)
            raise AtlanUploadError(message) from exc

        try:
            connection = Connection.create(
                client=self._client,
                name=connection_name,
                connector_type=connector_type,
                admin_roles=[admin_role_guid],
            )
            connection.attributes.qualified_name = qualified_name
            connection.attributes.connector_name = connector_type.value
            connection.attributes.category = connector_type.category.value
            response = self._client.asset.save(connection)
            created = response.assets_created(Connection)
            if created:
                qualified_name = created[0].attributes.qualified_name
            LOGGER.info(
                "Created Atlan connection '%s' with qualified name '%s'.",
                connection_name,
                qualified_name,
            )
            return connector_type, qualified_name
        except AtlanPermissionError as exc:
            message = (
                f"Insufficient permissions to create Atlan connection '{qualified_name}'. "
                "Grant the API token connection-admin rights or create the connection manually."
            )
            LOGGER.error("%s (original error: %s)", message, exc)
            raise AtlanUploadError(message) from exc
        except AtlanError as exc:
            message = f"Failed to create Atlan connection '{qualified_name}'."
            LOGGER.error("%s (original error: %s)", message, exc)
            raise AtlanUploadError(message) from exc

    def _ensure_database_exists(self, connection_qn: str) -> str:
        qualified_name = self._config.database_qualified_name
        name = self._config.atlan_database_name

        try:
            database = self._client.asset.get_by_qualified_name(
                qualified_name,
                Database,
                ignore_relationships=True,
            )
            actual_qn = database.attributes.qualified_name
            if actual_qn and actual_qn != qualified_name:
                LOGGER.warning(
                    "Configured database qualified name '%s' differs from Atlan record '%s'. "
                    "Using the value reported by Atlan.",
                    qualified_name,
                    actual_qn,
                )
                qualified_name = actual_qn
            LOGGER.debug(
                "Atlan database '%s' already exists; skipping creation.",
                qualified_name,
            )
            return qualified_name
        except NotFoundError:
            LOGGER.info(
                "Atlan database '%s' not found; attempting to create it.",
                qualified_name,
            )
        except AtlanError as exc:
            message = (
                f"Unable to verify existence of Atlan database '{qualified_name}'. "
                "Ensure the API token can read database assets or pre-create it."
            )
            LOGGER.error("%s (original error: %s)", message, exc)
            raise AtlanUploadError(message) from exc

        database = Database.creator(
            name=name,
            connection_qualified_name=connection_qn,
        )
        database.attributes.qualified_name = qualified_name
        database.attributes.connector_name = self._connector_type.value
        database.attributes.connection_name = self._config.atlan_connection_name
        try:
            response = self._client.asset.save(database)
            created = response.assets_created(Database)
            if created:
                qualified_name = created[0].attributes.qualified_name
            LOGGER.info(
                "Created Atlan database '%s' with qualified name '%s'.",
                name,
                qualified_name,
            )
            return qualified_name
        except AtlanPermissionError as exc:
            message = (
                f"Insufficient permissions to create Atlan database '{qualified_name}'. "
                "Grant the API token database-admin rights or create it manually."
            )
            LOGGER.error("%s (original error: %s)", message, exc)
            raise AtlanUploadError(message) from exc
        except AtlanError as exc:
            message = f"Failed to create Atlan database '{qualified_name}'."
            LOGGER.error("%s (original error: %s)", message, exc)
            raise AtlanUploadError(message) from exc

    def _ensure_schema_exists(
        self, database_qn: str, connection_qn: str
    ) -> str:
        qualified_name = self._config.schema_qualified_name
        name = self._config.atlan_schema_name

        try:
            schema = self._client.asset.get_by_qualified_name(
                qualified_name,
                Schema,
                ignore_relationships=True,
            )
            actual_qn = schema.attributes.qualified_name
            if actual_qn and actual_qn != qualified_name:
                LOGGER.warning(
                    "Configured schema qualified name '%s' differs from Atlan record '%s'. "
                    "Using the value reported by Atlan.",
                    qualified_name,
                    actual_qn,
                )
                qualified_name = actual_qn
            LOGGER.debug(
                "Atlan schema '%s' already exists; skipping creation.",
                qualified_name,
            )
            return qualified_name
        except NotFoundError:
            LOGGER.info(
                "Atlan schema '%s' not found; attempting to create it.",
                qualified_name,
            )
        except AtlanError as exc:
            message = (
                f"Unable to verify existence of Atlan schema '{qualified_name}'. "
                "Ensure the API token can read schema assets or pre-create it."
            )
            LOGGER.error("%s (original error: %s)", message, exc)
            raise AtlanUploadError(message) from exc

        schema = Schema.creator(
            name=name,
            database_qualified_name=database_qn,
            database_name=self._config.atlan_database_name,
            connection_qualified_name=connection_qn,
        )
        schema.attributes.qualified_name = qualified_name
        schema.attributes.connector_name = self._connector_type.value
        schema.attributes.connection_name = self._config.atlan_connection_name
        try:
            response = self._client.asset.save(schema)
            created = response.assets_created(Schema)
            if created:
                qualified_name = created[0].attributes.qualified_name
            LOGGER.info(
                "Created Atlan schema '%s' with qualified name '%s'.",
                name,
                qualified_name,
            )
            return qualified_name
        except AtlanPermissionError as exc:
            message = (
                f"Insufficient permissions to create Atlan schema '{qualified_name}'. "
                "Grant the API token schema-admin rights or create it manually."
            )
            LOGGER.error("%s (original error: %s)", message, exc)
            raise AtlanUploadError(message) from exc
        except AtlanError as exc:
            message = f"Failed to create Atlan schema '{qualified_name}'."
            LOGGER.error("%s (original error: %s)", message, exc)
            raise AtlanUploadError(message) from exc

    @staticmethod
    def _resolve_connector_type(name: Optional[str]) -> AtlanConnectorType:
        if not name:
            return AtlanConnectorType.CUSTOM

        normalised = name.replace("-", "_").replace(" ", "_").upper()
        for candidate in (normalised, name.upper()):
            if candidate in AtlanConnectorType.__members__:
                return AtlanConnectorType[candidate]

        lower_name = name.lower()
        for member in AtlanConnectorType:
            if member.value.lower() == lower_name:
                return member

        LOGGER.debug(
            "Falling back to CUSTOM connector type for unrecognised connector name '%s'.",
            name,
        )
        return AtlanConnectorType.CUSTOM

    @staticmethod
    def _table_name(record: DatasetRecord) -> str:
        identifier = record.identifier.strip() if record.identifier else None
        if identifier:
            sanitized = identifier.replace("/", "_").replace(" ", "_")
            return sanitized or identifier
        raise AtlanUploadError(
            "DXR classification missing a stable identifier; cannot derive table name."
        )


__all__ = ["AtlanUploadError", "AtlanUploader"]
