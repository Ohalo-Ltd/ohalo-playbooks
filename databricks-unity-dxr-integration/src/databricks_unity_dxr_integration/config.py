from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional

from dotenv import load_dotenv


@dataclass(frozen=True)
class DatabricksConfig:
    host: str
    token: str
    catalog: str
    schema: str
    source_volume: str
    label_volume_prefix: str = "dxr_label_"
    warehouse_id: Optional[str] = None
    checkpoint_table: Optional[str] = None
    job_ledger_table: Optional[str] = None
    metadata_table: Optional[str] = None


@dataclass(frozen=True)
class DataXRayConfig:
    base_url: str
    api_key: str
    datasource_id: str
    poll_interval_seconds: int = 10
    max_bytes_per_job: int = 30 * 1024 * 1024


@dataclass(frozen=True)
class IntegrationConfig:
    databricks: DatabricksConfig
    data_xray: DataXRayConfig


def load_config(env_file: Optional[str] = ".env") -> IntegrationConfig:
    """Load integration configuration from environment variables."""
    if env_file:
        load_dotenv(dotenv_path=env_file, override=False)

    databricks = DatabricksConfig(
        host=_require_env("DATABRICKS_HOST"),
        token=_require_env("DATABRICKS_TOKEN"),
        catalog=_require_env("DATABRICKS_CATALOG"),
        schema=_require_env("DATABRICKS_SCHEMA"),
        source_volume=_require_env("DATABRICKS_SOURCE_VOLUME"),
        label_volume_prefix=os.environ.get("DATABRICKS_LABEL_VOLUME_PREFIX", "dxr_label_"),
        warehouse_id=os.environ.get("DATABRICKS_WAREHOUSE_ID"),
        checkpoint_table=os.environ.get("DATABRICKS_CHECKPOINT_TABLE"),
        job_ledger_table=os.environ.get("DATABRICKS_JOB_LEDGER_TABLE"),
        metadata_table=os.environ.get("DATABRICKS_METADATA_TABLE"),
    )

    data_xray = DataXRayConfig(
        base_url=_require_env("DXR_BASE_URL").rstrip("/"),
        api_key=_require_env("DXR_API_KEY"),
        datasource_id=_require_env("DXR_DATASOURCE_ID"),
        poll_interval_seconds=int(os.environ.get("DXR_POLL_INTERVAL_SECONDS", "10")),
        max_bytes_per_job=int(os.environ.get("DXR_MAX_BYTES_PER_JOB", str(30 * 1024 * 1024))),
    )

    return IntegrationConfig(databricks=databricks, data_xray=data_xray)


def _require_env(key: str) -> str:
    value = os.environ.get(key)
    if not value:
        raise ValueError(f"Missing required environment variable: {key}")
    return value
