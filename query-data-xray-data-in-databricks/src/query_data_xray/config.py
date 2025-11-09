from __future__ import annotations

import os
import sys
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
@dataclass(slots=True)
class SavedSQLQuery:
    query_id: str
    label: Optional[str] = None


@dataclass(slots=True)
class JobConfig:
    base_url: str
    bearer_token: str
    delta_path: str  # original path (may include scheme)
    delta_location: str  # UC-friendly path used in CREATE TABLE ... LOCATION
    ingestion_date: str
    query: Optional[str]
    verify_ssl: bool
    http_timeout: int
    record_cap: Optional[int]
    delta_table: Optional[str]
    user_agent: str
    sql_queries: tuple[SavedSQLQuery, ...]

class ConfigError(ValueError):
    """Raised when required configuration is missing."""


def _get_dbutils():
    """Best-effort access to Databricks dbutils."""
    for namespace in (globals(), vars(sys.modules.get("__main__")) if "__main__" in sys.modules else {}):
        dbutils = namespace.get("dbutils") if isinstance(namespace, dict) else None
        if dbutils:
            return dbutils
    try:  # pragma: no cover - requires Databricks runtime
        from pyspark.dbutils import DBUtils  # type: ignore
        from pyspark.sql import SparkSession  # type: ignore

        spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
        return DBUtils(spark)
    except Exception:
        return None


def _get_token_from_secrets(scope: str, key: str) -> str:
    dbutils = _get_dbutils()
    if not dbutils:
        raise ConfigError(
            "DXR_TOKEN_SCOPE/KEY provided but Databricks dbutils is unavailable; "
            "ensure this job runs inside Databricks or provide DXR_BEARER_TOKEN directly."
        )
    try:
        return dbutils.secrets.get(scope=scope, key=key)
    except Exception as exc:  # pragma: no cover - Databricks-specific
        raise ConfigError(f"Failed to fetch DXR bearer token from scope='{scope}', key='{key}': {exc}") from exc


def _normalize_delta_location(path: str) -> str:
    """Return the path used in the Delta LOCATION clause.

    Users may pass UC Volume paths (``/Volumes/...``). Keep those untouched so serverless
    clusters can register the location without tripping the ``dbfs:/`` restriction. For
    any other relative path, fall back to the value provided.
    """
    if path.startswith("dbfs:/") or path.startswith("/Volumes/"):
        return path
    return path


def _parse_sql_queries(cli_values, env_value: Optional[str]) -> tuple[SavedSQLQuery, ...]:
    tokens: list[str] = []
    if cli_values:
        tokens.extend(cli_values)
    if env_value:
        tokens.extend([chunk.strip() for chunk in env_value.split(",") if chunk.strip()])
    queries: list[SavedSQLQuery] = []
    for token in tokens:
        label: Optional[str] = None
        query_id = token
        if ":" in token:
            label, query_id = token.split(":", 1)
            label = label.strip() or None
        query_id = query_id.strip()
        if not query_id:
            continue
        queries.append(SavedSQLQuery(query_id=query_id, label=label))
    return tuple(queries)


def load_config(args) -> JobConfig:
    load_dotenv()
    env = os.environ
    base_url = (args.base_url or env.get("DXR_BASE_URL") or "").strip().rstrip("/")
    if not base_url:
        raise ConfigError("DXR_BASE_URL (or --base-url) is required")

    token_scope = (args.token_scope or env.get("DXR_TOKEN_SCOPE") or "").strip()
    token_key = (args.token_key or env.get("DXR_TOKEN_KEY") or "").strip()
    bearer_token = (args.bearer_token or env.get("DXR_BEARER_TOKEN") or env.get("DXR_JWE_TOKEN") or "").strip()
    if not bearer_token and token_scope and token_key:
        bearer_token = _get_token_from_secrets(token_scope, token_key).strip()

    if not bearer_token:
        raise ConfigError(
            "DXR_BEARER_TOKEN (or --bearer-token) is required; alternatively provide DXR_TOKEN_SCOPE and DXR_TOKEN_KEY."
        )
    if (token_scope and not token_key) or (token_key and not token_scope):
        raise ConfigError("DXR_TOKEN_SCOPE and DXR_TOKEN_KEY must be provided together.")

    delta_path = (args.delta_path or env.get("DXR_DELTA_PATH") or "").strip()
    if not delta_path:
        raise ConfigError("DXR_DELTA_PATH (or --delta-path) is required")
    delta_location = _normalize_delta_location(delta_path)

    ingestion_date = (args.ingestion_date or env.get("DXR_INGESTION_DATE") or date.today().isoformat()).strip()
    query = args.query or env.get("DXR_QUERY") or None
    verify_ssl = _env_bool(args.verify_ssl if args.verify_ssl is not None else env.get("DXR_VERIFY_SSL"), True)
    http_timeout = args.http_timeout or _env_int(env.get("DXR_HTTP_TIMEOUT"), DEFAULT_TIMEOUT)
    record_cap = args.record_cap or _env_int(env.get("DXR_RECORD_CAP"))
    delta_table = args.delta_table or env.get("DXR_DELTA_TABLE") or None
    user_agent = (args.user_agent or env.get("DXR_USER_AGENT") or DEFAULT_USER_AGENT).strip()
    sql_queries = _parse_sql_queries(args.sql_queries, env.get("DXR_SQL_QUERIES"))

    return JobConfig(
        base_url=base_url,
        bearer_token=bearer_token,
        delta_path=delta_path,
        delta_location=delta_location,
        ingestion_date=ingestion_date,
        query=query,
        verify_ssl=verify_ssl,
        http_timeout=http_timeout or DEFAULT_TIMEOUT,
        record_cap=record_cap,
        delta_table=delta_table,
        user_agent=user_agent,
        sql_queries=sql_queries,
    )
