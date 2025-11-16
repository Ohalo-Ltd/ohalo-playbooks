from __future__ import annotations

import uuid
from typing import Iterable, List

try:  # pragma: no cover - imported at runtime on Databricks
    from pyspark.sql import DataFrame, SparkSession
except ImportError:  # pragma: no cover
    DataFrame = object  # type: ignore
    SparkSession = object  # type: ignore

from .config import MetadataTableConfig
from .metadata_records import MetadataRecord


class MetadataStore:
    """Persists DXR metadata into a managed Unity Catalog Delta table."""

    def __init__(self, spark: SparkSession, config: MetadataTableConfig):
        self._spark = spark
        self._config = config

    def ensure_table(self) -> None:
        table = self._config.identifier
        self._spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {table} (
                file_path STRING,
                relative_path STRING,
                catalog_name STRING,
                schema_name STRING,
                volume_name STRING,
                file_size BIGINT,
                modification_time BIGINT,
                datasource_id STRING,
                datasource_scan_id BIGINT,
                job_id STRING,
                labels ARRAY<STRING>,
                tags ARRAY<STRING>,
                categories ARRAY<STRING>,
                raw_metadata STRING,
                collected_at TIMESTAMP
            )
            USING DELTA
            """
        )

    def upsert_records(self, records: Iterable[MetadataRecord]) -> None:
        rows = [record.as_row() for record in records]
        if not rows:
            return

        df = self._spark.createDataFrame(rows)
        temp_view = f"dxr_metadata_{uuid.uuid4().hex}"
        df.createOrReplaceTempView(temp_view)

        table = self._config.identifier
        self._spark.sql(
            f"""
            MERGE INTO {table} AS target
            USING {temp_view} AS source
            ON target.file_path = source.file_path
            WHEN MATCHED THEN UPDATE SET
                relative_path = source.relative_path,
                catalog_name = source.catalog_name,
                schema_name = source.schema_name,
                volume_name = source.volume_name,
                file_size = source.file_size,
                modification_time = source.modification_time,
                datasource_id = source.datasource_id,
                datasource_scan_id = source.datasource_scan_id,
                job_id = source.job_id,
                labels = source.labels,
                tags = source.tags,
                categories = source.categories,
                raw_metadata = source.raw_metadata,
                collected_at = source.collected_at
            WHEN NOT MATCHED THEN INSERT *
            """
        )
        self._spark.catalog.dropTempView(temp_view)
