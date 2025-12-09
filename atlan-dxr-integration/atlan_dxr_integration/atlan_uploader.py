"""Utilities for writing DXR-derived tables and files into Atlan."""

from __future__ import annotations

import hashlib
import logging
import os
import re
from typing import Iterable, List, Optional, Sequence, Set

from pyatlan.client.atlan import AtlanClient
from pyatlan.errors import (
    AtlanError,
    NotFoundError,
    PermissionError as AtlanPermissionError,
)
from pyatlan.model.assets.core.asset import Asset
from pyatlan.model.assets.connection import Connection
from pyatlan.model.assets.core.database import Database
from pyatlan.model.assets.core.file import File
from pyatlan.model.assets.core.schema import Schema
from pyatlan.model.assets.core.table import Table
from pyatlan.model.core import AtlanTag, AtlanTagName
from pyatlan.model.enums import AtlanConnectorType, AtlanTagColor, FileType
from pyatlan.model.typedef import AtlanTagDef

from .config import Config
from .dataset_builder import DatasetFile, DatasetRecord

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
        self._ensured_tags: Set[str] = set()

    @property
    def client(self) -> AtlanClient:
        """Expose the underlying Atlan client for verification helpers."""

        return self._client

    def upsert(self, records: Iterable[DatasetRecord]) -> None:
        batch: List[Asset] = []
        batch_limit = max(self._config.atlan_batch_size, 1)
        for record in records:
            table = self._build_table(record)
            batch.append(table)
            if len(batch) >= batch_limit:
                self._save_batch(batch)
                batch = []
            for file_asset in self._build_file_assets(record):
                batch.append(file_asset)
                if len(batch) >= batch_limit:
                    self._save_batch(batch)
                    batch = []
        if batch:
            self._save_batch(batch)

    def table_qualified_name(self, record: DatasetRecord) -> str:
        return f"{self._schema_qualified_name}/{self._table_name(record)}"

    def file_qualified_name(
        self, record: DatasetRecord, dataset_file: DatasetFile
    ) -> str:
        """Qualified name for a dataset file represented as an Atlan file asset."""

        return f"{self._connection_qualified_name}/{self._file_name(record, dataset_file)}"

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

        table_tag_names = self._table_tag_names(record)
        tags = self._create_tags(table_tag_names)
        if tags:
            table.atlan_tags = tags

        return table

    def _save_batch(self, batch: Sequence[Asset]) -> None:
        try:
            response = self._client.asset.save(batch)
            mutated_tables = (
                len(response.assets_created(Table))
                + len(response.assets_updated(Table))
                + len(response.assets_partially_updated(Table))
            )
            mutated_objects = (
                len(response.assets_created(File))
                + len(response.assets_updated(File))
                + len(response.assets_partially_updated(File))
            )
            mutated_count = mutated_tables + mutated_objects
            if mutated_count == 0:
                if self._assets_exist(batch):
                    LOGGER.info(
                        "No DXR asset changes detected in Atlan (request id: %s); assets already up to date.",
                        getattr(response, "request_id", "unknown"),
                    )
                    return
                message = (
                    "Atlan acknowledged the upsert request but did not mutate any "
                    "assets. Verify the connection, connector name, and service "
                    "permissions."
                )
                LOGGER.error(message)
                raise AtlanUploadError(message)
            LOGGER.info(
                "Upserted %d DXR asset(s) into Atlan (tables=%d, objects=%d, request id: %s)",
                mutated_count,
                mutated_tables,
                mutated_objects,
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

    def _assets_exist(self, batch: Iterable[Asset]) -> bool:
        """Confirm each asset in the batch exists in Atlan."""

        for asset in batch:
            qualified_name = (
                asset.attributes.qualified_name if asset.attributes else None
            )
            if not qualified_name:
                return False
            try:
                self._client.asset.get_by_qualified_name(
                    qualified_name,
                    type(asset),
                    ignore_relationships=True,
                )
            except AtlanError:
                return False
        return True

    def _build_file_assets(self, record: DatasetRecord) -> List[File]:
        assets: List[File] = []
        for dataset_file in record.files:
            assets.append(self._create_file_asset(record, dataset_file))
        return assets

    def _create_file_asset(self, record: DatasetRecord, dataset_file: DatasetFile) -> File:
        name = self._file_name(record, dataset_file)
        file_type = self._resolve_file_type(dataset_file)
        file_asset = File.creator(
            name=name,
            connection_qualified_name=self._connection_qualified_name,
            file_type=file_type,
        )

        attrs = file_asset.attributes
        attrs.qualified_name = self.file_qualified_name(record, dataset_file)
        attrs.display_name = dataset_file.name or dataset_file.identifier or name
        attrs.description = dataset_file.identifier
        attrs.user_description = self._file_description(dataset_file)
        attrs.source_url = dataset_file.link
        attrs.file_path = dataset_file.path or dataset_file.link
        attrs.connection_name = self._config.atlan_connection_name
        attrs.connector_name = self._connector_type.value
        attrs.connection_qualified_name = self._connection_qualified_name

        file_tags = self._create_tags(dataset_file.labels)
        if file_tags:
            file_asset.atlan_tags = file_tags

        return file_asset

    @staticmethod
    def _file_description(dataset_file: DatasetFile) -> Optional[str]:
        parts: List[str] = []
        if dataset_file.path:
            parts.append(f"Path: {dataset_file.path}")
        if dataset_file.identifier and dataset_file.identifier not in (
            dataset_file.name,
            dataset_file.path,
        ):
            parts.append(f"DXR identifier: {dataset_file.identifier}")
        return "\n".join(parts) if parts else None

    def _file_name(self, record: DatasetRecord, dataset_file: DatasetFile) -> str:
        base = dataset_file.name or dataset_file.path or dataset_file.identifier or "file"
        sanitized = self._sanitize_fragment(base) if base else "file"
        unique_key = "|".join(
            value
            for value in (
                dataset_file.identifier,
                dataset_file.path,
                dataset_file.name,
                dataset_file.link,
                record.identifier,
            )
            if value
        )
        digest = hashlib.sha1(unique_key.encode("utf-8")).hexdigest()[:8]
        return f"{sanitized}-{digest}" if sanitized else digest

    def _resolve_file_type(self, dataset_file: DatasetFile) -> FileType:
        path = dataset_file.path or dataset_file.name
        if path:
            _, ext = os.path.splitext(path)
            ext = ext.lstrip(".").lower()
            mapping = {
                "pdf": FileType.PDF,
                "doc": FileType.DOC,
                "docx": FileType.DOC,
                "xls": FileType.XLS,
                "xlsx": FileType.XLS,
                "ppt": FileType.PPT,
                "pptx": FileType.PPT,
                "csv": FileType.CSV,
                "txt": FileType.TXT,
                "json": FileType.JSON,
                "xml": FileType.XML,
                "zip": FileType.ZIP,
                "yxdb": FileType.YXDB,
                "xlsm": FileType.XLSM,
                "hyper": FileType.HYPER,
            }
            if ext in mapping:
                return mapping[ext]
        return FileType.TXT

    @staticmethod
    def _sanitize_fragment(value: str) -> str:
        cleaned = value.strip()
        cleaned = cleaned.replace("\\", "/")
        cleaned = re.sub(r"[/\s]+", "_", cleaned)
        cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", cleaned)
        return cleaned.strip("_")

    def _table_tag_names(self, record: DatasetRecord) -> List[str]:
        names = {
            record.name.strip() if record.name else "",
            (record.classification_type or "").strip(),
            (record.classification_subtype or "").strip(),
        }
        return [name for name in sorted(names) if name]

    def _create_tags(self, tag_names: Iterable[str]) -> Optional[List[AtlanTag]]:
        cleaned = []
        seen = set()
        for raw in tag_names:
            tag_name = self._normalise_tag_name(raw)
            if not tag_name or tag_name in seen:
                continue
            seen.add(tag_name)
            self._ensure_tag_exists(tag_name)
            cleaned.append(AtlanTag(type_name=AtlanTagName(tag_name)))
        return cleaned or None

    @staticmethod
    def _normalise_tag_name(tag_name: str) -> str:
        return tag_name.strip()

    def _ensure_tag_exists(self, tag_name: str) -> None:
        if tag_name in self._ensured_tags:
            return

        tag_id = self._client.atlan_tag_cache.get_id_for_name(tag_name)
        if tag_id:
            self._ensured_tags.add(tag_name)
            return

        typedef = AtlanTagDef.create(name=tag_name, color=AtlanTagColor.GRAY)
        typedef.entity_types = ["Asset"]

        try:
            self._client.typedef.create(typedef)
        except AtlanError as exc:
            tag_id = self._client.atlan_tag_cache.get_id_for_name(tag_name)
            if not tag_id:
                raise AtlanUploadError(
                    f"Failed to create Atlan tag '{tag_name}': {exc}"
                ) from exc
        finally:
            tag_id = self._client.atlan_tag_cache.get_id_for_name(tag_name)
            if tag_id:
                self._ensured_tags.add(tag_name)

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
