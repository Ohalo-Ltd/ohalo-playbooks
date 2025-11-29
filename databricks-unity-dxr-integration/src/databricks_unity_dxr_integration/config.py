from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional

try:  # pragma: no cover - optional for Databricks jobs
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    def load_dotenv(*_, **__):
        return False


@dataclass(frozen=True)
class VolumeConfig:
    catalog: str
    schema: str
    volume: str
    base_path: str = "/Volumes"
    prefix: Optional[str] = None

    @property
    def root_path(self) -> str:
        from pathlib import Path

        root = Path(self.base_path) / self.catalog / self.schema / self.volume
        if self.prefix:
            root = root / self.prefix.strip("/")
        return str(root)


@dataclass(frozen=True)
class MetadataTableConfig:
    catalog: str
    schema: str
    table: str

    @property
    def identifier(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.table}"


@dataclass(frozen=True)
class SecretConfig:
    scope: str
    key: str


@dataclass(frozen=True)
class DataXRayConfig:
    base_url: str
    datasource_id: str
    poll_interval_seconds: int = 10
    max_bytes_per_job: int = 30 * 1024 * 1024
    max_files_per_job: int = 10
    verify_ssl: bool = True
    ca_bundle_path: Optional[str] = None
    api_prefix: str = "/api"
    debug: bool = False


@dataclass(frozen=True)
class JobConfig:
    volume: VolumeConfig
    metadata_table: MetadataTableConfig
    dxr: DataXRayConfig
    secret: SecretConfig
    drop_metadata_table: bool = False


def load_config(env_file: Optional[str] = ".env") -> JobConfig:
    """Load configuration for the Databricks job from environment variables."""
    if env_file:
        load_dotenv(dotenv_path=env_file, override=False)

    volume = VolumeConfig(
        catalog=_require_env("VOLUME_CATALOG"),
        schema=_require_env("VOLUME_SCHEMA"),
        volume=_require_env("VOLUME_NAME"),
        base_path=os.environ.get("VOLUME_BASE_PATH", "/Volumes"),
        prefix=os.environ.get("VOLUME_PREFIX"),
    )
    metadata_table = _resolve_metadata_table(volume)
    secret = SecretConfig(
        scope=_require_env("DXR_SECRET_SCOPE"),
        key=_require_env("DXR_SECRET_KEY"),
    )
    dxr = DataXRayConfig(
        base_url=_require_env("DXR_BASE_URL").rstrip("/"),
        datasource_id=_require_env("DXR_DATASOURCE_ID"),
        poll_interval_seconds=int(os.environ.get("DXR_POLL_INTERVAL_SECONDS", "10")),
        max_bytes_per_job=int(os.environ.get("DXR_MAX_BYTES_PER_JOB", str(30 * 1024 * 1024))),
        max_files_per_job=max(1, int(os.environ.get("DXR_MAX_FILES_PER_JOB", "25"))),
        verify_ssl=_env_bool("DXR_VERIFY_SSL", default=True),
        ca_bundle_path=os.environ.get("DXR_CA_BUNDLE_PATH"),
        api_prefix=_normalize_api_prefix(os.environ.get("DXR_API_PREFIX", "/api")),
        debug=_env_bool("DXR_DEBUG", default=False),
    )
    drop_table = _env_bool("DXR_DROP_METADATA_TABLE", default=False)
    return JobConfig(
        volume=volume,
        metadata_table=metadata_table,
        dxr=dxr,
        secret=secret,
        drop_metadata_table=drop_table,
    )


def _require_env(key: str) -> str:
    value = os.environ.get(key)
    if not value:
        raise ValueError(f"Missing required environment variable: {key}")
    return value


def _env_bool(key: str, default: bool = True) -> bool:
    value = os.environ.get(key)
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"0", "false", "no", "off", ""}:
        return False
    if normalized in {"1", "true", "yes", "on"}:
        return True
    return default


def _normalize_api_prefix(raw: Optional[str]) -> str:
    if not raw:
        return ""
    prefix = raw.strip()
    if not prefix or prefix == "/":
        return ""
    if not prefix.startswith("/"):
        prefix = f"/{prefix}"
    return prefix.rstrip("/")


def _resolve_metadata_table(volume: VolumeConfig) -> MetadataTableConfig:
    catalog = _env_with_default("METADATA_CATALOG", volume.catalog)
    schema = _env_with_default("METADATA_SCHEMA", volume.schema)
    default_table = f"{volume.volume}_metadata"
    table = _env_with_default("METADATA_TABLE", default_table)
    return MetadataTableConfig(catalog=catalog, schema=schema, table=table)


def _env_with_default(key: str, default: str) -> str:
    value = os.environ.get(key)
    if value is None:
        return default
    stripped = value.strip()
    return stripped or default
