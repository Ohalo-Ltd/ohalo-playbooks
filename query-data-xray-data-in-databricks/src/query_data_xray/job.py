from __future__ import annotations

import argparse
import logging
from typing import List

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from .config import ConfigError, JobConfig, load_config
from .dxr_client import DataXRayClient

logger = logging.getLogger(__name__)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Snapshot Data X-Ray file metadata into Delta Lake")
    parser.add_argument("--base-url", help="Override DXR_BASE_URL")
    parser.add_argument("--bearer-token", help="Override DXR_BEARER_TOKEN")
    parser.add_argument("--delta-path", help="Override DXR_DELTA_PATH")
    parser.add_argument("--delta-table", help="Unity Catalog table to refresh")
    parser.add_argument("--ingestion-date", help="YYYY-MM-DD partition value to use")
    parser.add_argument("--query", help="KQL filter applied to /api/v1/files")
    parser.add_argument("--verify-ssl", help="Treat as boolean to control TLS verification")
    parser.add_argument("--http-timeout", type=int, help="HTTP timeout in seconds")
    parser.add_argument("--record-cap", type=int, help="Stop after N JSONL rows (testing)")
    parser.add_argument("--user-agent", help="Custom user-agent header")
    return parser


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

    spark = SparkSession.builder.appName("dxr-file-metadata-daily").getOrCreate()
    df = spark.createDataFrame(records)
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
        config = load_config(parser.parse_args(argv))
    except ConfigError as exc:
        parser.error(str(exc))
    run_job(config)


if __name__ == "__main__":
    main()
