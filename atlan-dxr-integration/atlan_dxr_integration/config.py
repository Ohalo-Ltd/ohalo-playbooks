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
    atlan_connection_qualified_name: str
    atlan_connection_name: str
    atlan_connector_name: str
    atlan_dataset_path_prefix: str
    atlan_batch_size: int

    log_level: str

    @property
    def qualified_name_prefix(self) -> str:
        """Prefix used when generating dataset qualified names."""

        suffix = self.atlan_dataset_path_prefix.strip("/")
        if suffix:
            return f"{self.atlan_connection_qualified_name.rstrip('/')}/{suffix}"
        return self.atlan_connection_qualified_name.rstrip("/")

    @classmethod
    def from_env(cls) -> "Config":
        """Build configuration from environment variables."""

        load_dotenv()

        missing = [
            name
            for name in (
                "DXR_BASE_URL",
                "DXR_PAT",
                "ATLAN_BASE_URL",
                "ATLAN_API_TOKEN",
                "ATLAN_CONNECTION_QUALIFIED_NAME",
                "ATLAN_CONNECTION_NAME",
                "ATLAN_CONNECTOR_NAME",
            )
            if not os.getenv(name)
        ]
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
            atlan_connection_qualified_name=os.environ[
                "ATLAN_CONNECTION_QUALIFIED_NAME"
            ],
            atlan_connection_name=os.environ["ATLAN_CONNECTION_NAME"],
            atlan_connector_name=os.environ["ATLAN_CONNECTOR_NAME"],
            atlan_dataset_path_prefix=os.getenv("ATLAN_DATASET_PATH_PREFIX", "dxr"),
            atlan_batch_size=batch_size,
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
