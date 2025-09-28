"""Manage per-datasource connections and file asset ingestion."""

from __future__ import annotations

import logging
import re
import unicodedata
from dataclasses import dataclass, field
from typing import Dict, Mapping, Optional

from pyatlan.errors import AtlanError, PermissionError as AtlanPermissionError
from pyatlan.model.assets.core.file import File

from .atlan_uploader import AtlanUploadError
from .config import Config
from .connection_utils import (
    ConnectionHandle,
    ConnectionProvisioner,
    resolve_connector_type,
)
from .file_asset_builder import FileAssetFactory
from .tag_registry import TagHandle

LOGGER = logging.getLogger(__name__)


def _slugify(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
    lowered = ascii_text.lower()
    cleaned = re.sub(r"[^a-z0-9]+", "-", lowered).strip("-")
    return cleaned or "datasource"


@dataclass
class _DatasourceContext:
    handle: ConnectionHandle
    connection_name: str
    assets: list[File] = field(default_factory=list)


class DatasourceIngestionCoordinator:
    """Batch file assets per datasource and upsert them into Atlan."""

    def __init__(
        self,
        *,
        config: Config,
        client,
        provisioner: ConnectionProvisioner,
        factory: FileAssetFactory,
    ) -> None:
        self._config = config
        self._client = client
        self._provisioner = provisioner
        self._factory = factory
        self._batch_size = config.atlan_batch_size
        self._contexts: Dict[str, _DatasourceContext] = {}

    def consume(
        self,
        payload: Dict[str, object],
        *,
        classification_tags: Mapping[str, TagHandle],
    ) -> None:
        context = self._ensure_context(payload.get("datasource"))
        asset = self._factory.build(
            payload,
            connection_qualified_name=context.handle.qualified_name,
            connection_name=context.connection_name,
            classification_tags=classification_tags,
        )
        context.assets.append(asset)
        if len(context.assets) >= self._batch_size:
            self._flush_context(context)

    def flush(self) -> None:
        for context in self._contexts.values():
            if context.assets:
                self._flush_context(context)

    def _ensure_context(self, datasource: Optional[Dict[str, object]]) -> _DatasourceContext:
        datasource_id = None
        datasource_name = None
        connector_name = None

        if isinstance(datasource, dict):
            datasource_id = _coalesce_str(datasource.get("id"))
            datasource_name = _coalesce_str(datasource.get("name"))
            connector = datasource.get("connector")
            if isinstance(connector, dict):
                connector_name = _coalesce_str(connector.get("type"))

        key = datasource_id or datasource_name or "unknown"
        context = self._contexts.get(key)
        if context:
            return context

        connection_name = self._build_connection_name(key, datasource_name)
        qualified_name = self._build_connection_qualified_name(
            connection_name, connector_name
        )
        domain_name = self._build_domain_name(datasource_name or key)

        handle = self._provisioner.ensure_connection(
            qualified_name=qualified_name,
            connection_name=connection_name,
            connector_name=connector_name or self._config.atlan_global_connector_name,
            domain_name=domain_name,
        )

        context = _DatasourceContext(
            handle=handle,
            connection_name=handle.connection.attributes.name
            if handle.connection and handle.connection.attributes
            else connection_name,
        )
        self._contexts[key] = context
        return context

    def _flush_context(self, context: _DatasourceContext) -> None:
        if not context.assets:
            return
        try:
            response = self._client.asset.save(context.assets)
            created = response.assets_created(File)
            updated = response.assets_updated(File)
            partial = response.assets_partially_updated(File)
            mutated = len(created) + len(updated) + len(partial)
            LOGGER.info(
                "Upserted %d file asset(s) for connection '%s' (request id: %s)",
                mutated,
                context.connection_name,
                getattr(response, "request_id", "unknown"),
            )
        except AtlanPermissionError as exc:
            raise AtlanUploadError(
                f"Insufficient permissions to upsert files for connection '{context.connection_name}'."
            ) from exc
        except AtlanError as exc:
            raise AtlanUploadError(
                f"Failed to upsert files for connection '{context.connection_name}'."
            ) from exc
        finally:
            context.assets.clear()

    def _build_connection_name(self, key: str, display_name: Optional[str]) -> str:
        base = display_name or key
        suffix = _slugify(base)
        return f"{self._config.atlan_datasource_connection_prefix}-{suffix}".strip("-")

    def _build_connection_qualified_name(
        self, connection_name: str, connector_name: Optional[str]
    ) -> str:
        connector_type = resolve_connector_type(
            connector_name or self._config.atlan_global_connector_name
        )
        namespace = self._config.global_connection_namespace
        connector_segment = connector_type.value
        return f"{namespace}/{connector_segment}/{connection_name}"

    def _build_domain_name(self, datasource_name: str) -> Optional[str]:
        prefix = self._config.atlan_datasource_domain_prefix
        if not prefix:
            return None
        return f"{prefix} :: {datasource_name}"


def _coalesce_str(*values: object) -> Optional[str]:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


__all__ = ["DatasourceIngestionCoordinator"]
