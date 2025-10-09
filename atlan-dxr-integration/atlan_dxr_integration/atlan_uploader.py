"""Utilities for writing DXR-derived tables into Atlan."""

from __future__ import annotations

import logging
from typing import Any, Dict, Iterable, List

from app.atlan_service import AtlanRESTClient, AtlanRequestError

from .config import Config
from .connection_utils import ConnectionHandle, ConnectionProvisioner
from .dataset_builder import DatasetRecord

LOGGER = logging.getLogger(__name__)


class AtlanUploadError(RuntimeError):
    """Raised when Atlan rejects a table upsert request."""


class AtlanUploader:
    """Wrapper around Atlan's REST API for upserting DXR tables."""

    def __init__(self, config: Config) -> None:
        self._config = config
        self._client = AtlanRESTClient(
            base_url=config.atlan_base_url,
            api_key=config.atlan_api_token,
        )
        self._provisioner = ConnectionProvisioner(self._client)

        self._connection_handle = self._ensure_connection_exists()
        self._connector_name = self._connection_handle.connector_name
        self._connection_qualified_name = self._connection_handle.qualified_name
        self._connection_name = (
            self._connection_handle.connection.attributes.name
            or self._config.atlan_global_connection_name
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
    def rest_client(self) -> AtlanRESTClient:
        return self._client

    def upsert(self, records: Iterable[DatasetRecord]) -> None:
        batch: List[Dict[str, Any]] = []
        for record in records:
            entity = self._build_table_entity(record)
            batch.append(entity)
            if len(batch) >= self._config.atlan_batch_size:
                self._save_entities(batch, "Table", "tables", "table")
                batch = []
        if batch:
            self._save_entities(batch, "Table", "tables", "table")

    def table_qualified_name(self, record: DatasetRecord) -> str:
        return f"{self._schema_qualified_name}/{self._table_name(record)}"

    def upsert_files(self, assets: Iterable[Any]) -> None:
        batch: List[Dict[str, Any]] = []
        for asset in assets:
            entity = _asset_to_entity(asset)
            attrs = entity.setdefault("attributes", {})
            attrs.setdefault("connectionQualifiedName", self._connection_qualified_name)
            attrs.setdefault("connectionName", self._connection_name)
            attrs.setdefault("connectorName", self._connector_name)
            if not attrs.get("qualifiedName"):
                name = attrs.get("name") or attrs.get("displayName") or attrs.get("filePath")
                attrs["qualifiedName"] = f"{self._connection_qualified_name}/{name}"
            batch.append(entity)
            if len(batch) >= self._config.atlan_batch_size:
                self._save_entities(batch, "File", "file assets", "file asset")
                batch = []
        if batch:
            self._save_entities(batch, "File", "file assets", "file asset")

    def _build_table_entity(self, record: DatasetRecord) -> Dict[str, Any]:
        table_name = self._table_name(record)
        qualified_name = self.table_qualified_name(record)
        return {
            "typeName": "Table",
            "attributes": {
                "qualifiedName": qualified_name,
                "name": table_name,
                "displayName": record.name,
                "description": record.classification.description,
                "userDescription": record.description,
                "sourceURL": record.source_url,
                "connectorName": self._connector_name,
                "connectionName": self._connection_name,
                "connectionQualifiedName": self._connection_qualified_name,
                "schemaName": self._config.atlan_schema_name,
                "schemaQualifiedName": self._schema_qualified_name,
                "databaseName": self._config.atlan_database_name,
                "databaseQualifiedName": self._database_qualified_name,
            },
        }

    def _save_entities(
        self,
        batch: List[Dict[str, Any]],
        type_name: str,
        noun_plural: str,
        noun_singular: str,
    ) -> None:
        try:
            response = self._client.upsert_assets(batch)
            mutated = _extract_mutation_count(response, type_name)
            if mutated == 0:
                if self._entities_exist(batch, type_name):
                    LOGGER.info(
                        "No DXR %s were changed in Atlan; assets already up to date.",
                        noun_plural,
                    )
                    return
                raise AtlanUploadError(
                    f"Atlan acknowledged the {noun_singular} upsert but did not mutate any assets."
                )
            LOGGER.info("Upserted %d DXR %s into Atlan.", mutated, noun_plural)
        except AtlanRequestError as exc:
            message = (
                f"Atlan rejected the {noun_singular} upsert request (status {exc.status_code})."
            )
            LOGGER.error("%s Details: %s", message, exc.details)
            raise AtlanUploadError(message) from exc

    def _entities_exist(
        self,
        batch: Iterable[Dict[str, Any]],
        type_name: str,
    ) -> bool:
        for entity in batch:
            qualified_name = (entity.get("attributes") or {}).get("qualifiedName")
            if not qualified_name:
                return False
            found = self._client.get_asset(type_name, qualified_name)
            if not found:
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
        except Exception as exc:
            raise RuntimeError("Unable to verify Atlan connection") from exc

    def _ensure_database_exists(
        self,
        connection_handle: ConnectionHandle,
    ) -> str:
        database_qn = self._config.database_qualified_name
        try:
            return self._provisioner.ensure_database(
                name=self._config.atlan_database_name,
                qualified_name=database_qn,
                connection_handle=connection_handle,
            )
        except Exception as exc:
            raise RuntimeError(
                f"Unable to verify Atlan database '{database_qn}'"
            ) from exc

    def _ensure_schema_exists(
        self,
        database_qualified_name: str,
        connection_handle: ConnectionHandle,
    ) -> str:
        schema_qn = self._config.schema_qualified_name
        try:
            return self._provisioner.ensure_schema(
                name=self._config.atlan_schema_name,
                qualified_name=schema_qn,
                database_qualified_name=database_qualified_name,
                connection_handle=connection_handle,
            )
        except Exception as exc:
            raise RuntimeError(
                f"Unable to verify Atlan schema '{schema_qn}'"
            ) from exc

    def _table_name(self, record: DatasetRecord) -> str:
        return record.identifier


def _asset_to_entity(asset: Any) -> Dict[str, Any]:
    if isinstance(asset, dict):
        return asset
    attrs_obj = getattr(asset, "attributes", None)
    type_name = getattr(asset, "type_name", asset.__class__.__name__)
    if attrs_obj is None:
        raise TypeError("Unsupported asset payload; expected dict or object with 'attributes'.")
    attributes: Dict[str, Any] = {}
    for key, value in vars(attrs_obj).items():
        if key.startswith("_"):
            continue
        attributes[key] = value
    return {"typeName": type_name, "attributes": attributes}


def _extract_mutation_count(response: Any, type_name: str) -> int:
    if not isinstance(response, dict):
        return 0
    mutated = response.get("mutatedEntities") or {}
    count = 0
    for bucket in ("CREATE", "UPDATE", "PARTIAL_UPDATE"):
        entities = mutated.get(bucket) or []
        for entity in entities:
            if isinstance(entity, dict) and entity.get("typeName") == type_name:
                count += 1
    return count


__all__ = ["AtlanUploader", "AtlanUploadError"]
