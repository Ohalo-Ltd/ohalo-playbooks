from __future__ import annotations

import uuid
from typing import Iterable, List

try:  # pragma: no cover - imported at runtime on Databricks
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.types import (
        ArrayType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )
except ImportError:  # pragma: no cover
    DataFrame = object  # type: ignore
    SparkSession = object  # type: ignore
    ArrayType = LongType = StringType = StructField = StructType = TimestampType = None  # type: ignore

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
        _validate_table_schema(self._spark, table)

    def upsert_records(self, records: Iterable[MetadataRecord]) -> None:
        rows = [record.as_row() for record in records]
        if not rows:
            return

        schema = _build_schema()
        if schema is not None:
            df = self._spark.createDataFrame(rows, schema=schema)
        else:  # pragma: no cover - fallback for local unit tests without PySpark
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


def _build_schema():
    if StructType is None:
        return None
    return StructType(
        [
            StructField("file_path", StringType(), nullable=False),
            StructField("relative_path", StringType(), nullable=False),
            StructField("catalog_name", StringType(), nullable=False),
            StructField("schema_name", StringType(), nullable=False),
            StructField("volume_name", StringType(), nullable=False),
            StructField("file_size", LongType(), nullable=False),
            StructField("modification_time", LongType(), nullable=False),
            StructField("datasource_id", StringType(), nullable=False),
            StructField("datasource_scan_id", LongType(), nullable=True),
            StructField("job_id", StringType(), nullable=False),
            StructField("labels", ArrayType(StringType(), containsNull=False), nullable=False),
            StructField("tags", ArrayType(StringType(), containsNull=False), nullable=False),
            StructField("categories", ArrayType(StringType(), containsNull=False), nullable=False),
            StructField("raw_metadata", StringType(), nullable=False),
            StructField("collected_at", TimestampType(), nullable=False),
        ]
    )


def _validate_table_schema(spark: SparkSession, table: str) -> None:
    schema = _build_schema()
    if schema is None:
        return

    try:
        actual_schema = spark.table(table).schema
    except Exception as exc:  # pragma: no cover - requires Spark runtime
        raise RuntimeError(f"Unable to read schema for {table}: {exc}") from exc

    actual_map = {field.name: field for field in actual_schema}
    missing = [field.name for field in schema if field.name not in actual_map]
    mismatched = []
    for expected in schema:
        actual = actual_map.get(expected.name)
        if not actual:
            continue
        if actual.dataType.simpleString() != expected.dataType.simpleString():
            mismatched.append(
                f"{expected.name} (expected {expected.dataType.simpleString()}, found {actual.dataType.simpleString()})"
            )

    if missing or mismatched:
        problems = []
        if missing:
            problems.append(f"Missing columns: {', '.join(missing)}")
        if mismatched:
            problems.append(f"Type mismatches: {', '.join(mismatched)}")
        message = "; ".join(problems)
        raise ValueError(
            f"Metadata table {table} schema does not match expected DXR schema. {message}. "
            "Point METADATA_* variables at an empty table or drop/recreate the existing table."
        )
