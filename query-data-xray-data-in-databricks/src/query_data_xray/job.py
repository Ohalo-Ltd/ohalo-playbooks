from __future__ import annotations

import argparse
import logging
import sys
import json
from typing import List, Dict, Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from .config import ConfigError, JobConfig, load_config
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

    spark = SparkSession.builder.appName("dxr-file-metadata-daily").getOrCreate()
    df = spark.createDataFrame(normalized_rows)
    df = df.withColumn("ingestion_date", F.lit(config.ingestion_date))
    df = df.withColumn("ingested_at", F.current_timestamp())

    logger.info("Writing %s rows to %s", len(records), config.delta_path)
    (
        df.write.format("delta")
        .mode("overwrite")
        .partitionBy("ingestion_date")
        .option("replaceWhere", f"ingestion_date = '{config.ingestion_date}'")
        .save(config.delta_path)
    )

    if config.delta_table:
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {config.delta_table} USING DELTA LOCATION '{config.delta_path}'"
        )
        spark.sql(f"REFRESH TABLE {config.delta_table}")
        logger.info("Table %s refreshed", config.delta_table)

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
