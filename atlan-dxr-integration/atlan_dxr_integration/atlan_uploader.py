"""Utilities for writing DXR-derived tables into Atlan."""

from __future__ import annotations

import logging
from typing import Iterable, List, Type, TypeVar

from pyatlan.client.atlan import AtlanClient
from pyatlan.errors import AtlanError, PermissionError as AtlanPermissionError
from pyatlan.model.assets.core.asset import Asset
from pyatlan.model.assets.core.file import File
from pyatlan.model.assets.core.table import Table
from pyatlan.model.enums import AtlanConnectorType

from .config import Config
from .connection_utils import ConnectionHandle, ConnectionProvisioner
from .dataset_builder import DatasetRecord

AssetType = TypeVar("AssetType", bound=Asset)

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
        self._provisioner = ConnectionProvisioner(self._client)

        self._connection_handle = self._ensure_connection_exists()
        self._connector_type = (
            self._connection_handle.connector_type or AtlanConnectorType.CUSTOM
        )
        self._connection_qualified_name = self._connection_handle.qualified_name
        self._connection_name = (
            self._connection_handle.connection.attributes.name
            if self._connection_handle.connection
            and self._connection_handle.connection.attributes
            else self._config.atlan_global_connection_name
        )

        self._database_qualified_name = self._ensure_database_exists(
            self._connection_handle
        )
        self._schema_qualified_name = self._ensure_schema_exists(
            self._database_qualified_name,
            self._connection_handle,
        )

    @property
    def connection_qualified_name(self) -> str:
        return self._connection_qualified_name

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
                self._save_assets(batch, Table, "tables", "table")
                batch = []
        if batch:
            self._save_assets(batch, Table, "tables", "table")

    def table_qualified_name(self, record: DatasetRecord) -> str:
        return f"{self._schema_qualified_name}/{self._table_name(record)}"

    def upsert_files(self, assets: Iterable[File]) -> None:
        batch: List[File] = []
        for asset in assets:
            attrs = asset.attributes
            attrs.connection_name = self._connection_name
            attrs.connector_name = self._connector_type.value
            if not attrs.connection_qualified_name:
                attrs.connection_qualified_name = self._connection_qualified_name
            if not attrs.qualified_name:
                attrs.qualified_name = (
                    f"{self._connection_qualified_name}/{attrs.name or asset.guid}"
                )
            batch.append(asset)
            if len(batch) >= self._config.atlan_batch_size:
                self._save_assets(batch, File, "file assets", "file asset")
                batch = []
        if batch:
            self._save_assets(batch, File, "file assets", "file asset")

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
        attrs.connection_name = self._connection_name

        return table

    def _save_assets(
        self,
        batch: List[AssetType],
        asset_type: Type[AssetType],
        noun_plural: str,
        noun_singular: str,
    ) -> None:
        try:
            response = self._client.asset.save(batch)
            created = response.assets_created(asset_type)
            updated = response.assets_updated(asset_type)
            partial = response.assets_partially_updated(asset_type)
            mutated_count = len(created) + len(updated) + len(partial)
            if mutated_count == 0:
                if self._assets_exist(batch, asset_type):
                    LOGGER.info(
                        "No DXR %s were changed in Atlan (request id: %s); assets already up to date.",
                        noun_plural,
                        getattr(response, "request_id", "unknown"),
                    )
                    return
                message = (
                    "Atlan acknowledged the upsert request but did not mutate any "
                    f"{noun_plural}. Verify the connection, connector name, and service "
                    "permissions."
                )
                LOGGER.error(message)
                raise AtlanUploadError(message)
            LOGGER.info(
                "Upserted %d DXR %s into Atlan (request id: %s)",
                mutated_count,
                noun_plural,
                getattr(response, "request_id", "unknown"),
            )
        except AtlanPermissionError as exc:
            message = (
                f"Atlan rejected the {noun_singular} upsert due to insufficient permissions. "
                "Verify that the provided API token can create "
                f"{noun_plural} for "
                f"connection '{self._connection_name}'."
            )
            LOGGER.error("%s (original error: %s)", message, exc)
            raise AtlanUploadError(message) from exc
        except AtlanError as exc:
            message = f"Atlan rejected the {noun_singular} upsert request."
            LOGGER.error("%s (original error: %s)", message, exc)
            raise AtlanUploadError(message) from exc

    def _assets_exist(
        self,
        batch: Iterable[AssetType],
        asset_type: Type[AssetType],
    ) -> bool:
        """Confirm each asset in the batch exists in Atlan."""

        for asset in batch:
            attrs = asset.attributes
            qualified_name = getattr(attrs, "qualified_name", None)
            try:
                if not qualified_name:
                    return False
                self._client.asset.get_by_qualified_name(
                    qualified_name,
                    asset_type,
                    ignore_relationships=True,
                )
            except AtlanError:
                return False
        return True

    def _ensure_connection_exists(self) -> ConnectionHandle:
        try:
            return self._provisioner.ensure_connection(
                qualified_name=self._config.atlan_global_connection_qualified_name,
                connection_name=self._config.atlan_global_connection_name,
                connector_name=self._config.atlan_global_connector_name,
                domain_name=self._config.atlan_global_domain_name,
            )
        except RuntimeError as exc:  # pragma: no cover - escalated upstream
            raise AtlanUploadError(str(exc)) from exc

    def _ensure_database_exists(self, handle: ConnectionHandle) -> str:
        database_qn = f"{handle.qualified_name.rstrip('/')}/{self._config.atlan_database_name}"
        return self._provisioner.ensure_database(
            name=self._config.atlan_database_name,
            qualified_name=database_qn,
            connection_handle=handle,
        )

    def _ensure_schema_exists(
        self, database_qn: str, handle: ConnectionHandle
    ) -> str:
        schema_qn = f"{database_qn.rstrip('/')}/{self._config.atlan_schema_name}"
        return self._provisioner.ensure_schema(
            name=self._config.atlan_schema_name,
            qualified_name=schema_qn,
            database_qualified_name=database_qn,
            connection_handle=handle,
        )

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
