"""Configuration handling for the DXR → Atlan integration."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional, Set

from dotenv import load_dotenv


@dataclass
class Config:
    """Runtime configuration loaded from environment variables."""

    dxr_base_url: str
    dxr_pat: str
    dxr_classification_types: Optional[Set[str]]
    dxr_sample_file_limit: int
    dxr_file_fetch_limit: int

    atlan_base_url: str
    atlan_api_token: str
    atlan_global_connection_qualified_name: str
    atlan_global_connection_name: str
    atlan_global_connector_name: str
    atlan_global_domain_name: Optional[str]
    atlan_datasource_connection_prefix: str
    atlan_datasource_domain_prefix: Optional[str]
    atlan_database_name: str
    atlan_schema_name: str
    atlan_dataset_path_prefix: str
    atlan_batch_size: int
    atlan_tag_namespace: str

    log_level: str

    @property
    def qualified_name_prefix(self) -> str:
        """Prefix used when generating dataset qualified names."""

        suffix = self.atlan_dataset_path_prefix.strip("/")
        base = self.global_schema_qualified_name.rstrip("/")
        if suffix:
            return f"{base}/{suffix}"
        return base

    @property
    def atlan_connection_qualified_name(self) -> str:
        """Backwards-compatible alias for the global connection qualified name."""

        return self.atlan_global_connection_qualified_name

    @property
    def atlan_connection_name(self) -> str:  # pragma: no cover - compatibility
        return self.atlan_global_connection_name

    @property
    def atlan_connector_name(self) -> str:  # pragma: no cover - compatibility
        return self.atlan_global_connector_name

    @property
    def global_connection_namespace(self) -> str:
        return self.atlan_global_connection_qualified_name.split("/")[0]

    @property
    def global_connection_connector_segment(self) -> str:
        parts = self.atlan_global_connection_qualified_name.split("/")
        return parts[1] if len(parts) > 1 else self.atlan_global_connector_name.lower()

    @property
    def database_qualified_name(self) -> str:
        """Qualified name for the Atlan database containing DXR assets."""

        base = self.atlan_global_connection_qualified_name.rstrip("/")
        return f"{base}/{self.atlan_database_name}"

    @property
    def schema_qualified_name(self) -> str:
        """Qualified name for the Atlan schema grouping DXR tables."""

        return f"{self.database_qualified_name}/{self.atlan_schema_name}"

    @property
    def global_schema_qualified_name(self) -> str:
        return self.schema_qualified_name

    @classmethod
    def from_env(cls) -> "Config":
        """Build configuration from environment variables."""

        load_dotenv()

        base_required = (
            "DXR_BASE_URL",
            "DXR_PAT",
            "ATLAN_BASE_URL",
            "ATLAN_API_TOKEN",
        )

        missing = [name for name in base_required if not os.getenv(name)]

        # Support legacy env names while transitioning to the new architecture.
        global_connection_qn = (
            os.getenv("ATLAN_GLOBAL_CONNECTION_QUALIFIED_NAME")
            or os.getenv("ATLAN_CONNECTION_QUALIFIED_NAME")
        )
        global_connection_name = (
            os.getenv("ATLAN_GLOBAL_CONNECTION_NAME")
            or os.getenv("ATLAN_CONNECTION_NAME")
        )
        global_connector_name = (
            os.getenv("ATLAN_GLOBAL_CONNECTOR_NAME")
            or os.getenv("ATLAN_CONNECTOR_NAME")
        )

        if not global_connection_qn:
            missing.append("ATLAN_GLOBAL_CONNECTION_QUALIFIED_NAME")
        if not global_connection_name:
            missing.append("ATLAN_GLOBAL_CONNECTION_NAME")
        if not global_connector_name:
            missing.append("ATLAN_GLOBAL_CONNECTOR_NAME")
        if missing:
            raise ValueError(
                "Missing required environment variables: " + ", ".join(missing)
            )

        classification_types = _parse_csv(os.getenv("DXR_CLASSIFICATION_TYPES"))
        batch_size = _parse_int(os.getenv("ATLAN_BATCH_SIZE"), default=20, minimum=1)
        sample_file_limit = _parse_int(
            os.getenv("DXR_SAMPLE_FILE_LIMIT"), default=5, minimum=0
        )
        file_fetch_limit = _parse_int(
            os.getenv("DXR_FILE_FETCH_LIMIT"), default=200, minimum=0
        )

        return cls(
            dxr_base_url=_strip_trailing_slash(os.environ["DXR_BASE_URL"]),
            dxr_pat=os.environ["DXR_PAT"],
            dxr_classification_types=classification_types,
            dxr_sample_file_limit=sample_file_limit,
            dxr_file_fetch_limit=file_fetch_limit,
            atlan_base_url=_strip_trailing_slash(os.environ["ATLAN_BASE_URL"]),
            atlan_api_token=os.environ["ATLAN_API_TOKEN"],
            atlan_global_connection_qualified_name=global_connection_qn,
            atlan_global_connection_name=global_connection_name,
            atlan_global_connector_name=global_connector_name,
            atlan_global_domain_name=os.getenv("ATLAN_GLOBAL_DOMAIN"),
            atlan_datasource_connection_prefix=os.getenv(
                "ATLAN_DATASOURCE_CONNECTION_PREFIX", "dxr-datasource"
            ),
            atlan_datasource_domain_prefix=os.getenv(
                "ATLAN_DATASOURCE_DOMAIN_PREFIX"
            ),
            atlan_database_name=os.getenv("ATLAN_DATABASE_NAME", "dxr"),
            atlan_schema_name=os.getenv("ATLAN_SCHEMA_NAME", "labels"),
            atlan_dataset_path_prefix=os.getenv("ATLAN_DATASET_PATH_PREFIX", "dxr"),
            atlan_batch_size=batch_size,
            atlan_tag_namespace=os.getenv("ATLAN_TAG_NAMESPACE", "DXR"),
            log_level=os.getenv("LOG_LEVEL", "INFO"),
        )


def _parse_csv(raw: Optional[str]) -> Optional[Set[str]]:
    if not raw:
        return None
    items = {value.strip().upper() for value in raw.split(",") if value.strip()}
    return items or None


def _parse_int(raw: Optional[str], *, default: int, minimum: int) -> int:
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError as exc:  # pragma: no cover - defensive
        raise ValueError(f"Invalid integer value: {raw!r}") from exc
    if value < minimum:
        raise ValueError(f"Value must be >= {minimum}: {value}")
    return value


def _strip_trailing_slash(value: str) -> str:
    return value.rstrip("/")


__all__ = ["Config"]
