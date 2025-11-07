from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date
from typing import Optional

try:  # pragma: no cover - optional dependency on Databricks
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    def load_dotenv(*args, **kwargs):
        return False

DEFAULT_USER_AGENT = "query-data-xray-data-in-databricks/0.1.0"
DEFAULT_TIMEOUT = 120


def _env_bool(value: Optional[str], default: bool = True) -> bool:
    if value is None:
        return default
    return str(value).strip().lower() not in {"0", "false", "no"}


def _env_int(value: Optional[str], default: Optional[int] = None) -> Optional[int]:
    if value is None or value == "":
        return default
    try:
        return int(value)
    except ValueError:
        return default


@dataclass(slots=True)
class JobConfig:
    base_url: str
    bearer_token: str
    delta_path: str
    ingestion_date: str
    query: Optional[str]
    verify_ssl: bool
    http_timeout: int
    record_cap: Optional[int]
    delta_table: Optional[str]
    user_agent: str

class ConfigError(ValueError):
    """Raised when required configuration is missing."""


def load_config(args) -> JobConfig:
    load_dotenv()
    env = os.environ
    base_url = (args.base_url or env.get("DXR_BASE_URL") or "").strip().rstrip("/")
    if not base_url:
        raise ConfigError("DXR_BASE_URL (or --base-url) is required")

    bearer_token = (args.bearer_token or env.get("DXR_BEARER_TOKEN") or env.get("DXR_JWE_TOKEN") or "").strip()
    if not bearer_token:
        raise ConfigError("DXR_BEARER_TOKEN (or --bearer-token) is required")

    delta_path = (args.delta_path or env.get("DXR_DELTA_PATH") or "").strip()
    if not delta_path:
        raise ConfigError("DXR_DELTA_PATH (or --delta-path) is required")

    ingestion_date = (args.ingestion_date or env.get("DXR_INGESTION_DATE") or date.today().isoformat()).strip()
    query = args.query or env.get("DXR_QUERY") or None
    verify_ssl = _env_bool(args.verify_ssl if args.verify_ssl is not None else env.get("DXR_VERIFY_SSL"), True)
    http_timeout = args.http_timeout or _env_int(env.get("DXR_HTTP_TIMEOUT"), DEFAULT_TIMEOUT)
    record_cap = args.record_cap or _env_int(env.get("DXR_RECORD_CAP"))
    delta_table = args.delta_table or env.get("DXR_DELTA_TABLE") or None
    user_agent = (args.user_agent or env.get("DXR_USER_AGENT") or DEFAULT_USER_AGENT).strip()

    return JobConfig(
        base_url=base_url,
        bearer_token=bearer_token,
        delta_path=delta_path,
        ingestion_date=ingestion_date,
        query=query,
        verify_ssl=verify_ssl,
        http_timeout=http_timeout or DEFAULT_TIMEOUT,
        record_cap=record_cap,
        delta_table=delta_table,
        user_agent=user_agent,
    )
