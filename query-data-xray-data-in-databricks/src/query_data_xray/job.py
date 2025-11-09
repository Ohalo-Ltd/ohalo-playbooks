from __future__ import annotations

import argparse
import logging
import sys
import json
import os
import os
import re
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from uuid import uuid4

import requests
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from .config import ConfigError, JobConfig, SavedSQLQuery, _get_dbutils, load_config
from .dxr_client import DataXRayClient

logger = logging.getLogger(__name__)

_JSON_FIELDS = (
    "datasource",
    "labels",
    "extractedMetadata",
    "entitlements",
    "dlpLabels",
    "annotators",
    "owner",
    "createdBy",
    "modifiedBy",
)


def _stringify_complex(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        try:
            return json.dumps(value)
        except TypeError:
            return json.dumps(json.loads(json.dumps(value, default=str)))
    return value


def _project_record(record: Dict[str, Any]) -> Dict[str, Any]:
    row: Dict[str, Any] = {}
    row["file_id"] = record.get("fileId")
    row["file_name"] = record.get("fileName")
    row["path"] = record.get("path")
    row["size"] = record.get("size")
    row["mime_type"] = record.get("mimeType")
    row["created_at"] = record.get("createdAt")
    row["last_modified_at"] = record.get("lastModifiedAt")
    row["content_sha256"] = record.get("contentSha256")
    row["scan_depth"] = record.get("scanDepth")
    row["raw_json"] = json.dumps(record, default=str)
    for field in _JSON_FIELDS:
        row[f"{field}_json"] = _stringify_complex(record.get(field))
    return row


def _quote_table_identifier(name: str) -> str:
    parts = [part.strip().strip("`") for part in name.split(".") if part.strip().strip("`")]
    if len(parts) != 3:
        raise ConfigError("DXR_DELTA_TABLE is invalid; expected catalog.schema.table")
    return ".".join(f"`{part}`" for part in parts)


def _create_spark_session() -> SparkSession:
    """Build a Spark session that supports local Delta runs when outside Databricks."""
    builder = SparkSession.builder.appName("dxr-file-metadata-daily")
    on_databricks = bool(os.getenv("DATABRICKS_RUNTIME_VERSION"))
    if not on_databricks:
        builder = builder.master(os.getenv("DXR_SPARK_MASTER", "local[*]"))

        delta_pref = os.getenv("DXR_USE_DELTA_PIP", "auto").strip().lower()
        enable_delta = delta_pref in {"1", "true", "yes", "on", "auto"}
        if enable_delta:
            try:
                from delta import configure_spark_with_delta_pip  # type: ignore
            except ImportError:
                if delta_pref != "auto":
                    logger.warning(
                        "DXR_USE_DELTA_PIP is set but delta-spark is not installed. "
                        "Install delta-spark or unset DXR_USE_DELTA_PIP to skip Delta configuration."
                    )
            else:
                builder = configure_spark_with_delta_pip(builder)

    return builder.getOrCreate()

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Snapshot Data X-Ray file metadata into Delta Lake")
    parser.add_argument(
        "--base-url",
        "--DXR_BASE_URL",
        dest="base_url",
        help="Override DXR_BASE_URL",
    )
    parser.add_argument(
        "--bearer-token",
        "--DXR_BEARER_TOKEN",
        dest="bearer_token",
        help="Override DXR_BEARER_TOKEN",
    )
    parser.add_argument(
        "--token-scope",
        "--DXR_TOKEN_SCOPE",
        dest="token_scope",
        help="Databricks secret scope to fetch DXR bearer token from",
    )
    parser.add_argument(
        "--token-key",
        "--DXR_TOKEN_KEY",
        dest="token_key",
        help="Databricks secret key to fetch DXR bearer token from",
    )
    parser.add_argument(
        "--delta-path",
        "--DXR_DELTA_PATH",
        dest="delta_path",
        help="Override DXR_DELTA_PATH",
    )
    parser.add_argument(
        "--delta-table",
        "--DXR_DELTA_TABLE",
        dest="delta_table",
        help="Unity Catalog table to refresh",
    )
    parser.add_argument(
        "--ingestion-date",
        "--DXR_INGESTION_DATE",
        dest="ingestion_date",
        help="YYYY-MM-DD partition value to use",
    )
    parser.add_argument(
        "--query",
        "--DXR_QUERY",
        dest="query",
        help="KQL filter applied to /api/v1/files",
    )
    parser.add_argument(
        "--verify-ssl",
        "--DXR_VERIFY_SSL",
        dest="verify_ssl",
        help="Treat as boolean to control TLS verification",
    )
    parser.add_argument(
        "--http-timeout",
        "--DXR_HTTP_TIMEOUT",
        dest="http_timeout",
        type=int,
        help="HTTP timeout in seconds",
    )
    parser.add_argument(
        "--record-cap",
        "--DXR_RECORD_CAP",
        dest="record_cap",
        type=int,
        help="Stop after N JSONL rows (testing)",
    )
    parser.add_argument(
        "--user-agent",
        "--DXR_USER_AGENT",
        dest="user_agent",
        help="Custom user-agent header",
    )
    parser.add_argument(
        "--sql-query",
        "--DXR_SQL_QUERY",
        dest="sql_queries",
        action="append",
        help=(
            "Saved Databricks SQL query to export as CSV (format label:query_id or query_id). "
            "Repeat flag to export multiple queries."
        ),
    )
    return parser


def _normalize_args(argv: List[str] | None) -> List[str]:
    """Allow Databricks job parameters like DXR_BASE_URL without leading dashes."""
    raw_args = list(argv) if argv is not None else sys.argv[1:]
    normalized: List[str] = []
    for token in raw_args:
        if token.startswith("-") or not token:
            normalized.append(token)
            continue
        if token.upper().startswith("DXR_"):
            normalized.append(f"--{token.upper()}")
        else:
            normalized.append(token)
    return normalized


def _init_logging():
    if logging.getLogger().handlers:
        return
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


def _get_workspace_api_details():
    dbutils = _get_dbutils()
    if not dbutils:
        raise ConfigError(
            "Saved SQL queries require Databricks dbutils; run this job inside Databricks when DXR_SQL_QUERY is set."
        )
    try:
        context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        api_url = context.apiUrl().get()
        api_token = context.apiToken().get()
    except Exception as exc:  # pragma: no cover - Databricks only
        raise ConfigError(f"Failed to access Databricks workspace context: {exc}") from exc
    if not api_url or not api_token:
        raise ConfigError("Databricks context did not provide apiUrl/apiToken; saved SQL queries cannot run.")
    return api_url.rstrip("/"), api_token


def _slugify(value: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", value).strip("-").lower()
    return slug or "query"


def _build_child_path(base: str, *parts: str) -> str:
    path = base.rstrip("/")
    for part in parts:
        clean = part.strip("/")
        if clean:
            path = f"{path}/{clean}"
    return path


def _ensure_dbfs_uri(path: str) -> str:
    if path.startswith("dbfs:/"):
        return path
    if path.startswith("/"):
        return f"dbfs:{path}"
    return f"dbfs:/{path}"


def _dbfs_uri_to_local_path(uri: str) -> str:
    normalized = _ensure_dbfs_uri(uri)
    return normalized.replace("dbfs:/", "/dbfs/")


class DatabricksSQLClient:
    def __init__(self, api_url: str, api_token: str, user_agent: str):
        self._base_url = api_url.rstrip("/")
        self._session = requests.Session()
        self._session.headers.update(
            {
                "Authorization": f"Bearer {api_token}",
                "Content-Type": "application/json",
                "User-Agent": user_agent,
            }
        )

    def fetch_query(self, query_id: str) -> Tuple[str, Optional[str]]:
        resp = self._session.get(f"{self._base_url}/api/2.0/sql/queries/{query_id}")
        if resp.status_code != 200:
            raise ConfigError(
                f"Failed to fetch saved SQL query '{query_id}': HTTP {resp.status_code} {resp.text}"
            )
        payload = resp.json()
        query = (payload.get("query") or "").strip()
        if not query:
            raise ConfigError(f"Saved SQL query '{query_id}' has no SQL text")
        name = payload.get("name")
        return query, name


def _write_csv_artifact(spark_df, artifact_dir: str, dbutils, csv_name: str = "results") -> str:
    dbfs_dir = _ensure_dbfs_uri(artifact_dir)
    tmp_dir = _build_child_path(dbfs_dir, f"_tmp_csv_{uuid4().hex}")
    logger.info("Writing CSV artifact to %s (temporary: %s)", dbfs_dir, tmp_dir)
    (
        spark_df.coalesce(1)
        .write.mode("overwrite")
        .option("header", True)
        .csv(tmp_dir)
    )
    csv_files = [entry.path for entry in dbutils.fs.ls(tmp_dir) if entry.path.endswith(".csv")]
    if not csv_files:
        raise ConfigError(f"No CSV output produced for query artifacts near {tmp_dir}")
    target_csv = _build_child_path(dbfs_dir, f"{csv_name}.csv")
    dbutils.fs.mkdirs(dbfs_dir)
    dbutils.fs.mv(csv_files[0], target_csv, True)
    dbutils.fs.rm(tmp_dir, True)
    return target_csv


def _render_powershell_script(csv_hint: str, label: str) -> str:
    stub_message = (
        "This file has been removed due to records and retention policy, "
        "please contact your file server administrator if you need access to this file."
    )
    script = f"""<#
Generated by query-data-xray-data-in-databricks
Saved query label : {label}
Source CSV (download first): {csv_hint}
CSV must include columns 'path' and 'content_sha256'.
#>
param(
    [Parameter(Mandatory = $true)]
    [string]$CsvPath,

    [Parameter(Mandatory = $true)]
    [string]$ArchiveRoot,

    [string]$StubMessage = "{stub_message}"
)

if (-not (Test-Path -LiteralPath $CsvPath)) {{
    Write-Error "CSV file not found: $CsvPath"
    exit 1
}}

$rows = Import-Csv -LiteralPath $CsvPath
foreach ($row in $rows) {{
    $source = $row.path
    if ([string]::IsNullOrWhiteSpace($source)) {{
        continue
    }}
    if (-not (Test-Path -LiteralPath $source)) {{
        Write-Warning "Source path not found: $source"
        continue
    }}

    $hash = if ([string]::IsNullOrWhiteSpace($row.content_sha256)) {{
        "unhashed"
    }} else {{
        $row.content_sha256
    }}

    $targetFolder = Join-Path -Path $ArchiveRoot -ChildPath $hash
    New-Item -ItemType Directory -Force -Path $targetFolder | Out-Null

    $fileName = Split-Path -Path $source -Leaf
    $archivePath = Join-Path -Path $targetFolder -ChildPath $fileName

    Move-Item -LiteralPath $source -Destination $archivePath -Force
    Set-Content -LiteralPath $source -Value $StubMessage -Force -Encoding UTF8
}}
"""
    return script


def _write_powershell_artifact(script_text: str, artifact_dir: str, script_name: str) -> str:
    dbfs_dir = _ensure_dbfs_uri(artifact_dir)
    script_dbfs = _build_child_path(dbfs_dir, script_name)
    local_path = Path(_dbfs_uri_to_local_path(script_dbfs))
    local_path.parent.mkdir(parents=True, exist_ok=True)
    local_path.write_text(script_text, encoding="utf-8")
    return script_dbfs


def _materialize_saved_queries(spark: SparkSession, config: JobConfig):
    if not config.sql_queries:
        return
    dbutils = _get_dbutils()
    if not dbutils:
        raise ConfigError("Databricks dbutils unavailable; cannot export saved SQL queries.")
    api_url, api_token = _get_workspace_api_details()
    client = DatabricksSQLClient(api_url, api_token, config.user_agent)
    artifact_root = _build_child_path(config.delta_location, f"ingestion_date={config.ingestion_date}", "artifacts")
    for query_cfg in config.sql_queries:
        sql_text, remote_name = client.fetch_query(query_cfg.query_id)
        label = query_cfg.label or remote_name or query_cfg.query_id
        slug = _slugify(label)
        logger.info("Executing saved query %s (%s)", label, query_cfg.query_id)
        df = spark.sql(sql_text.rstrip(";\n "))
        artifact_dir = _build_child_path(artifact_root, slug)
        logger.info("Materializing artifacts for %s under %s", label, artifact_dir)
        csv_dbfs = _write_csv_artifact(df, artifact_dir, dbutils, "results")
        script_text = _render_powershell_script(csv_dbfs, label)
        script_dbfs = _write_powershell_artifact(script_text, artifact_dir, "remediate_duplicates.ps1")
        logger.info("Saved query artifacts written: CSV=%s PowerShell=%s", csv_dbfs, script_dbfs)


def run_job(config: JobConfig) -> None:
    client = DataXRayClient(
        base_url=config.base_url,
        bearer_token=config.bearer_token,
        http_timeout=config.http_timeout,
        verify_ssl=config.verify_ssl,
        user_agent=config.user_agent,
    )
    logger.info(
        "Fetching Data X-Ray files: base_url=%s path=/api/v1/files query=%s",
        config.base_url,
        config.query or "<none>",
    )
    records = list(client.stream_file_metadata(query=config.query, record_cap=config.record_cap))
    if not records:
        logger.info("/api/v1/files returned no rows; skipping Delta write")
        return

    normalized_rows = [_project_record(rec) for rec in records]

    spark = _create_spark_session()
    df = spark.createDataFrame(normalized_rows)
    df = df.withColumn("ingestion_date", F.lit(config.ingestion_date))
    df = df.withColumn("ingested_at", F.current_timestamp())

    logger.info("Writing %s rows to %s", len(records), config.delta_location)
    (
        df.write.format("delta")
        .mode("overwrite")
        .partitionBy("ingestion_date")
        .option("replaceWhere", f"ingestion_date = '{config.ingestion_date}'")
        .save(config.delta_location)
    )

    if config.delta_table:
        quoted_table = _quote_table_identifier(config.delta_table)
        logger.info("Registering Delta path %s as table %s", config.delta_location, quoted_table)
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {quoted_table} USING DELTA LOCATION '{config.delta_location}'"
        )
        spark.sql(f"REFRESH TABLE {quoted_table}")
        logger.info("Table %s refreshed", quoted_table)

    if config.sql_queries:
        _materialize_saved_queries(spark, config)

    logger.info("Job completed for %s", config.ingestion_date)
    spark.stop()


def main(argv: List[str] | None = None) -> None:
    _init_logging()
    parser = _build_parser()
    try:
        normalized_args = _normalize_args(argv)
        config = load_config(parser.parse_args(normalized_args))
    except ConfigError as exc:
        parser.error(str(exc))
    run_job(config)


if __name__ == "__main__":
    main()
