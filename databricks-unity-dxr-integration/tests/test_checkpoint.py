from databricks_unity_dxr_integration.checkpoint import (
    DeltaCheckpointStore,
    FileCheckpoint,
    InMemoryCheckpointStore,
)
from databricks_unity_dxr_integration.metadata import MetadataRecord


def test_in_memory_checkpoint_round_trip():
    store = InMemoryCheckpointStore()
    record = FileCheckpoint(
        volume_path="/Volumes/cat/sch/vol",
        file_path="/Volumes/cat/sch/vol/file.txt",
        file_size=10,
        modification_time=5,
        checksum="abc",
        datasource_scan_id=123,
    )
    store.upsert(record)

    loaded = store.load()
    assert loaded[record.file_path] == record


def test_delta_checkpoint_load_maps_rows_to_records():
    backend = StubBackend(
        rows=[
            {
                "volume_path": "/Volumes/cat/sch/vol",
                "file_path": "/Volumes/cat/sch/vol/file.txt",
                "file_size": 20,
                "modification_time": 10,
                "checksum": "xyz",
                "datasource_scan_id": 200,
            }
        ]
    )
    store = DeltaCheckpointStore(backend, "dxr_metadata.file_checkpoints")

    records = store.load()

    assert records["/Volumes/cat/sch/vol/file.txt"].file_size == 20
    assert backend.last_fetch_statement.startswith("SELECT")


def test_delta_checkpoint_upsert_emits_merge_statement():
    backend = StubBackend()
    store = DeltaCheckpointStore(backend, "dxr_metadata.file_checkpoints")
    record = FileCheckpoint(
        volume_path="/Volumes/cat/sch/vol",
        file_path="/Volumes/cat/sch/vol/file with 'quote'.txt",
        file_size=20,
        modification_time=10,
        checksum=None,
        datasource_scan_id=None,
    )

    store.upsert(record)

    assert backend.executed_statements, "Expected merge statement to be executed."
    statement = backend.executed_statements[0]
    assert "MERGE INTO dxr_metadata.file_checkpoints" in statement
    assert "file with ''quote''" in statement  # escaped quote
    assert "NULL AS checksum" in statement or "checksum = NULL" in statement


def test_delta_checkpoint_record_job_and_metadata():
    backend = StubBackend()
    store = DeltaCheckpointStore(
        backend,
        table_name="dxr_metadata.file_checkpoints",
        ledger_table="dxr_metadata.job_ledger",
        metadata_table="dxr_metadata.file_metadata",
    )

    store.record_job("job-1", "FINISHED", 200)
    store.persist_metadata(
        [
            MetadataRecord(
                file_id="file-1",
                volume_path="/Volumes/cat/sch/vol",
                file_path="/Volumes/cat/sch/vol/file1.txt",
                datasource_id=1,
                datasource_scan_id=200,
                dxr_labels=["Finance"],
                dxr_tags=["Confidential"],
                dxr_categories=["Category"],
                metadata_json={"foo": "bar"},
            )
        ]
    )

    assert any("job_ledger" in stmt for stmt in backend.executed_statements)
    assert any("file_metadata" in stmt for stmt in backend.executed_statements)


class StubBackend:
    def __init__(self, rows=None):
        self._rows = rows or []
        self.executed_statements = []
        self.last_fetch_statement = None

    def fetch_all(self, statement: str):
        self.last_fetch_statement = statement
        return list(self._rows)

    def execute(self, statement: str):
        self.executed_statements.append(statement)

    def execute_many(self, statements):
        for stmt in statements:
            self.execute(stmt)
