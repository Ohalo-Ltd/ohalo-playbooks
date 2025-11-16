from datetime import datetime, timezone

from databricks_unity_dxr_integration.config import MetadataTableConfig
from databricks_unity_dxr_integration.metadata_records import MetadataRecord
from databricks_unity_dxr_integration.metadata_store import MetadataStore


class StubSpark:
    def __init__(self):
        self.sql_calls = []
        self.temp_views = set()

    def sql(self, statement: str):
        self.sql_calls.append(statement)

    def createDataFrame(self, rows):
        return StubDataFrame(rows, self)

    @property
    def catalog(self):
        return self

    def dropTempView(self, name: str):
        self.temp_views.discard(name)


class StubDataFrame:
    def __init__(self, rows, spark: StubSpark):
        self.rows = rows
        self.spark = spark

    def createOrReplaceTempView(self, name: str):
        self.spark.temp_views.add(name)
        self.temp_view_name = name


def test_metadata_store_writes_rows():
    spark = StubSpark()
    table = MetadataTableConfig(catalog="cat", schema="sch", table="tbl")
    store = MetadataStore(spark, table)
    store.ensure_table()

    record = MetadataRecord(
        file_path="/Volumes/cat/sch/vol/file.txt",
        relative_path="file.txt",
        catalog_name="cat",
        schema_name="sch",
        volume_name="vol",
        file_size=10,
        modification_time=1,
        datasource_id="42",
        datasource_scan_id=99,
        job_id="job-1",
        labels=["Finance"],
        tags=["Confidential"],
        categories=["Category"],
        raw_metadata={"dxr#labels": ["Finance"]},
        collected_at=datetime.now(timezone.utc),
    )

    store.upsert_records([record])

    assert spark.sql_calls, "Expected at least one SQL statement to run."
