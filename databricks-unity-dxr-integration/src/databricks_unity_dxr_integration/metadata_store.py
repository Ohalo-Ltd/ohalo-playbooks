from __future__ import annotations

import uuid
from typing import Iterable, List

try:  # pragma: no cover - imported at runtime on Databricks
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.types import (
        ArrayType,
        BooleanType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )
except ImportError:  # pragma: no cover
    DataFrame = object  # type: ignore
    SparkSession = object  # type: ignore
    ArrayType = BooleanType = LongType = StringType = StructField = StructType = TimestampType = None  # type: ignore

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
                file_name STRING,
                object_id STRING,
                parent_paths ARRAY<STRING>,
                mime_type STRING,
                indexed_at STRING,
                sha256 STRING,
                sha256_file_meta STRING,
                doc_language STRING,
                composite_type STRING,
                is_processed BOOLEAN,
                document_status STRING,
                text_extraction_status STRING,
                metadata_extraction_status STRING,
                dxr_tags ARRAY<STRING>,
                removed_tags ARRAY<STRING>,
                ocr_used BOOLEAN,
                categories ARRAY<STRING>,
                annotations STRING,
                folder_id STRING,
                modified_at STRING,
                binary_hash STRING,
                annotation_stats_json STRING,
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
                file_name = source.file_name,
                object_id = source.object_id,
                parent_paths = source.parent_paths,
                mime_type = source.mime_type,
                indexed_at = source.indexed_at,
                sha256 = source.sha256,
                sha256_file_meta = source.sha256_file_meta,
                doc_language = source.doc_language,
                composite_type = source.composite_type,
                is_processed = source.is_processed,
                document_status = source.document_status,
                text_extraction_status = source.text_extraction_status,
                metadata_extraction_status = source.metadata_extraction_status,
                dxr_tags = source.dxr_tags,
                removed_tags = source.removed_tags,
                ocr_used = source.ocr_used,
                categories = source.categories,
                annotations = source.annotations,
                folder_id = source.folder_id,
                modified_at = source.modified_at,
                binary_hash = source.binary_hash,
                annotation_stats_json = source.annotation_stats_json,
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
            StructField("file_name", StringType(), nullable=True),
            StructField("object_id", StringType(), nullable=True),
            StructField("parent_paths", ArrayType(StringType(), containsNull=False), nullable=False),
            StructField("mime_type", StringType(), nullable=True),
            StructField("indexed_at", StringType(), nullable=True),
            StructField("sha256", StringType(), nullable=True),
            StructField("sha256_file_meta", StringType(), nullable=True),
            StructField("doc_language", StringType(), nullable=True),
            StructField("composite_type", StringType(), nullable=True),
            StructField("is_processed", BooleanType(), nullable=True),
            StructField("document_status", StringType(), nullable=True),
            StructField("text_extraction_status", StringType(), nullable=True),
            StructField("metadata_extraction_status", StringType(), nullable=True),
            StructField("dxr_tags", ArrayType(StringType(), containsNull=False), nullable=False),
            StructField("removed_tags", ArrayType(StringType(), containsNull=False), nullable=False),
            StructField("ocr_used", BooleanType(), nullable=True),
            StructField("categories", ArrayType(StringType(), containsNull=False), nullable=False),
            StructField("annotations", StringType(), nullable=True),
            StructField("folder_id", StringType(), nullable=True),
            StructField("modified_at", StringType(), nullable=True),
            StructField("binary_hash", StringType(), nullable=True),
            StructField("annotation_stats_json", StringType(), nullable=True),
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
