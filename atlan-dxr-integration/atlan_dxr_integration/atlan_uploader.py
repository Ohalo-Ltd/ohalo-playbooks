"""Utilities for writing dataset records into Atlan."""

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
from pyatlan.model.assets.data_set import DataSet
from pyatlan.model.enums import AtlanConnectorType

from .config import Config
from .dataset_builder import DatasetRecord

LOGGER = logging.getLogger(__name__)


class AtlanUploadError(RuntimeError):
    """Raised when Atlan rejects a dataset upsert request."""


class AtlanUploader:
    """Wrapper around Atlan's Asset API for upserting datasets."""

    def __init__(self, config: Config) -> None:
        self._config = config
        self._client = AtlanClient(
            base_url=config.atlan_base_url,
            api_key=config.atlan_api_token,
        )
        connector_type, connection_qn = self._ensure_connection_exists()
        self._connector_type = connector_type or AtlanConnectorType.CUSTOM
        self._connection_qualified_name = connection_qn
        self._qualified_name_prefix = self._build_dataset_prefix(connection_qn)

    @property
    def client(self) -> AtlanClient:
        """Expose the underlying Atlan client for verification helpers."""

        return self._client

    def upsert(self, records: Iterable[DatasetRecord]) -> None:
        batch: List[DataSet] = []
        for record in records:
            dataset = self._build_dataset(record)
            batch.append(dataset)
            if len(batch) >= self._config.atlan_batch_size:
                self._save_batch(batch)
                batch = []
        if batch:
            self._save_batch(batch)

    def dataset_qualified_name(self, record: DatasetRecord) -> str:
        return f"{self._qualified_name_prefix}/{record.identifier}"

    def _build_dataset(self, record: DatasetRecord) -> DataSet:
        qualified_name = self.dataset_qualified_name(record)
        attributes = DataSet.Attributes(
            qualified_name=qualified_name,
            name=record.name,
            description=record.classification.description,
            user_description=record.description,
            connection_name=self._config.atlan_connection_name,
            connection_qualified_name=self._connection_qualified_name,
            connector_name=self._connector_type.value,
            source_url=record.source_url,
        )
        dataset = DataSet(attributes=attributes)
        return dataset

    def _save_batch(self, batch: List[DataSet]) -> None:
        try:
            response = self._client.asset.save(batch)
            created = response.assets_created(DataSet)
            updated = response.assets_updated(DataSet)
            partial = response.assets_partially_updated(DataSet)
            mutated_count = len(created) + len(updated) + len(partial)
            if mutated_count == 0:
                if self._datasets_exist(batch):
                    LOGGER.info(
                        "No dataset changes detected in Atlan (request id: %s); assets already up to date.",
                        getattr(response, "request_id", "unknown"),
                    )
                    return
                message = (
                    "Atlan acknowledged the upsert request but did not mutate any "
                    "datasets. Verify the connection, connector name, and service "
                    "permissions."
                )
                LOGGER.error(message)
                raise AtlanUploadError(message)
            LOGGER.info(
                "Upserted %d dataset(s) into Atlan (request id: %s)",
                mutated_count,
                getattr(response, "request_id", "unknown"),
            )
        except AtlanPermissionError as exc:
            message = (
                "Atlan rejected the dataset upsert due to insufficient permissions. "
                "Verify that the provided API token can create DataSet assets for "
                f"connection '{self._config.atlan_connection_name}'."
            )
            LOGGER.error("%s (original error: %s)", message, exc)
            raise AtlanUploadError(message) from exc
        except AtlanError as exc:
            message = "Atlan rejected the dataset upsert request."
            LOGGER.error("%s (original error: %s)", message, exc)
            raise AtlanUploadError(message) from exc

    def _datasets_exist(self, batch: Iterable[DataSet]) -> bool:
        """Confirm each dataset in the batch exists in Atlan."""

        for dataset in batch:
            qualified_name = dataset.attributes.qualified_name
            try:
                self._client.asset.get_by_qualified_name(
                    qualified_name,
                    DataSet,
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

    def _build_dataset_prefix(self, connection_qn: str) -> str:
        base = (connection_qn or self._config.atlan_connection_qualified_name).rstrip("/")
        suffix = (self._config.atlan_dataset_path_prefix or "dxr").strip("/")
        if suffix:
            return f"{base}/{suffix}"
        return base

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


__all__ = ["AtlanUploadError", "AtlanUploader"]
