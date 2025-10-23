from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Dict, Iterable, List, Optional, Protocol

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import ResultManifest, StatementResponse

if TYPE_CHECKING:
    from .metadata import MetadataRecord

CHECKPOINT_TABLE_NAME = "dxr_metadata.file_checkpoints"
"""
Default Delta table used to store Databricks volume classification checkpoints.
The recommended schema:

CREATE TABLE dxr_metadata.file_checkpoints (
    volume_path STRING NOT NULL,
    file_path STRING NOT NULL,
    file_size BIGINT,
    modification_time BIGINT,
    checksum STRING,
    last_classified_at TIMESTAMP,
    datasource_scan_id BIGINT,
    PRIMARY KEY (volume_path, file_path)
);
GRANT SELECT ON TABLE dxr_metadata.file_checkpoints TO `data-governance`;

Optional job ledger table for tracking pipeline runs:

CREATE TABLE dxr_metadata.job_ledger (
    job_id STRING NOT NULL,
    state STRING,
    datasource_scan_id BIGINT,
    updated_at TIMESTAMP DEFAULT current_timestamp(),
    PRIMARY KEY (job_id)
);

Optional metadata table storing JSON payloads per file:

CREATE TABLE dxr_metadata.file_metadata (
    file_id STRING NOT NULL,
    metadata_json STRING,
    updated_at TIMESTAMP DEFAULT current_timestamp(),
    PRIMARY KEY (file_id)
);
"""


@dataclass(frozen=True)
class FileCheckpoint:
    """Represents the latest processed state of a file in a Unity Catalog volume."""

    volume_path: str
    file_path: str
    file_size: int
    modification_time: int
    checksum: Optional[str] = None
    datasource_scan_id: Optional[int] = None


class CheckpointStore:
    """Abstract persistence interface for retrieving & updating file checkpoints."""

    def load(self) -> Dict[str, FileCheckpoint]:
        raise NotImplementedError

    def upsert(self, record: FileCheckpoint) -> None:
        raise NotImplementedError

    def record_job(self, job_id: str, state: str, datasource_scan_id: Optional[int]) -> None:
        """Persist job status information (optional)."""

    def persist_metadata(self, records: List["MetadataRecord"]) -> None:
        """Persist metadata rows associated with files (optional)."""


class InMemoryCheckpointStore(CheckpointStore):
    """Simple checkpoint store used for local runs and unit tests."""

    def __init__(self, initial: Optional[Dict[str, FileCheckpoint]] = None):
        self._records: Dict[str, FileCheckpoint] = dict(initial) if initial else {}

    def load(self) -> Dict[str, FileCheckpoint]:
        return dict(self._records)

    def upsert(self, record: FileCheckpoint) -> None:
        self._records[record.file_path] = record

    def record_job(self, job_id: str, state: str, datasource_scan_id: Optional[int]) -> None:
        _ = (job_id, state, datasource_scan_id)

    def persist_metadata(self, records: List["MetadataRecord"]) -> None:
        _ = records


class SqlBackend(Protocol):
    """Minimal SQL execution interface used by DeltaCheckpointStore."""

    def fetch_all(self, statement: str) -> List[Dict[str, object]]:
        ...

    def execute(self, statement: str) -> None:
        ...

    def execute_many(self, statements: Iterable[str]) -> None:
        ...

class DatabricksSQLBackend(SqlBackend):
    """Thin wrapper around Databricks SQL warehouses for checkpoint persistence."""

    def __init__(self, workspace: WorkspaceClient, warehouse_id: str, wait_timeout: int = 60):
        self._workspace = workspace
        self._warehouse_id = warehouse_id
        self._wait_timeout = wait_timeout

    def fetch_all(self, statement: str) -> List[Dict[str, object]]:
        response = self._workspace.sql.statements.execute(
            statement=statement,
            warehouse_id=self._warehouse_id,
            wait_timeout=self._wait_timeout,
        )
        return _rows_from_statement(response)

    def execute(self, statement: str) -> None:
        self._workspace.sql.statements.execute(
            statement=statement,
            warehouse_id=self._warehouse_id,
            wait_timeout=self._wait_timeout,
        )

    def execute_many(self, statements: Iterable[str]) -> None:
        for statement in statements:
            self.execute(statement)


class DeltaCheckpointStore(CheckpointStore):
    """Checkpoint store backed by a Delta table in Databricks SQL."""

    def __init__(
        self,
        sql_backend: SqlBackend,
        table_name: str,
        ledger_table: Optional[str] = None,
        metadata_table: Optional[str] = None,
    ):
        self._backend = sql_backend
        self._table_name = table_name
        self._ledger_table = ledger_table or table_name
        self._metadata_table = metadata_table

    def load(self) -> Dict[str, FileCheckpoint]:
        statement = (
            f"SELECT volume_path, file_path, file_size, modification_time, checksum, datasource_scan_id "
            f"FROM {self._table_name}"
        )
        rows = self._backend.fetch_all(statement)
        checkpoints: Dict[str, FileCheckpoint] = {}
        for row in rows:
            checkpoint = FileCheckpoint(
                volume_path=str(row.get("volume_path", "")),
                file_path=str(row.get("file_path", "")),
                file_size=int(row.get("file_size", 0) or 0),
                modification_time=int(row.get("modification_time", 0) or 0),
                checksum=row.get("checksum"),
                datasource_scan_id=_maybe_int(row.get("datasource_scan_id")),
            )
            checkpoints[checkpoint.file_path] = checkpoint
        return checkpoints

    def upsert(self, record: FileCheckpoint) -> None:
        statement = _build_merge_statement(self._table_name, record)
        self._backend.execute(statement)

    def record_job(self, job_id: str, state: str, datasource_scan_id: Optional[int]) -> None:
        if not self._ledger_table:
            return
        statement = _build_ledger_statement(self._ledger_table, job_id, state, datasource_scan_id)
        self._backend.execute(statement)

    def persist_metadata(self, records: List["MetadataRecord"]) -> None:
        if not self._metadata_table or not records:
            return

        statements = [
            _build_metadata_statement(
                table_name=self._metadata_table,
                record=record,
            )
            for record in records
        ]
        self._backend.execute_many(statements)


def _rows_from_statement(response: StatementResponse) -> List[Dict[str, object]]:
    result = response.result
    manifest: Optional[ResultManifest] = response.manifest
    if result is None or manifest is None or manifest.schema is None:
        return []

    columns = [column.name for column in manifest.schema.columns or []]
    data_array = result.data_array or []

    return [dict(zip(columns, row)) for row in data_array]


def _maybe_int(value: object) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _build_merge_statement(table_name: str, record: FileCheckpoint) -> str:
    literal = _sql_literal
    select_clause = (
        "SELECT "
        f"{literal(record.volume_path)} AS volume_path, "
        f"{literal(record.file_path)} AS file_path, "
        f"{record.file_size} AS file_size, "
        f"{record.modification_time} AS modification_time, "
        f"{literal(record.checksum)} AS checksum, "
        f"{literal(record.datasource_scan_id)} AS datasource_scan_id"
    )
    return (
        f"MERGE INTO {table_name} AS target "
        f"USING ({select_clause}) AS source "
        "ON target.volume_path = source.volume_path AND target.file_path = source.file_path "
        "WHEN MATCHED THEN UPDATE SET "
        "target.file_size = source.file_size, "
        "target.modification_time = source.modification_time, "
        "target.checksum = source.checksum, "
        "target.datasource_scan_id = source.datasource_scan_id "
        "WHEN NOT MATCHED THEN INSERT (volume_path, file_path, file_size, modification_time, checksum, datasource_scan_id) "
        "VALUES (source.volume_path, source.file_path, source.file_size, source.modification_time, source.checksum, source.datasource_scan_id)"
    )


def _sql_literal(value: Optional[object]) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    text = str(value).replace("'", "''")
    return f"'{text}'"


def _build_ledger_statement(table_name: str, job_id: str, state: str, datasource_scan_id: Optional[int]) -> str:
    literal = _sql_literal
    return (
        f"MERGE INTO {table_name} AS target "
        f"USING (SELECT {literal(job_id)} AS job_id, {literal(state)} AS state, {literal(datasource_scan_id)} AS datasource_scan_id) AS source "
        "ON target.job_id = source.job_id "
        "WHEN MATCHED THEN UPDATE SET target.state = source.state, target.datasource_scan_id = source.datasource_scan_id, target.updated_at = current_timestamp() "
        "WHEN NOT MATCHED THEN INSERT (job_id, state, datasource_scan_id, updated_at) VALUES (source.job_id, source.state, source.datasource_scan_id, current_timestamp())"
    )


def _build_metadata_statement(table_name: str, record: "MetadataRecord") -> str:
    import json

    literal = _sql_literal
    metadata_json = json.dumps(record.metadata_json, separators=(",", ":"))
    labels_json = json.dumps(record.dxr_labels, separators=(",", ":"))
    tags_json = json.dumps(record.dxr_tags, separators=(",", ":"))
    categories_json = json.dumps(record.dxr_categories, separators=(",", ":"))

    return (
        f"MERGE INTO {table_name} AS target "
        "USING (SELECT "
        f"{literal(record.file_id)} AS file_id, "
        f"{literal(record.volume_path)} AS volume_path, "
        f"{literal(record.file_path)} AS file_path, "
        f"{literal(record.datasource_id)} AS datasource_id, "
        f"{literal(record.datasource_scan_id)} AS datasource_scan_id, "
        f"{literal(labels_json)} AS labels_json, "
        f"{literal(tags_json)} AS tags_json, "
        f"{literal(categories_json)} AS categories_json, "
        f"{literal(metadata_json)} AS metadata_json) AS source "
        "ON target.file_id = source.file_id "
        "WHEN MATCHED THEN UPDATE SET "
        "target.volume_path = source.volume_path, "
        "target.file_path = source.file_path, "
        "target.datasource_id = source.datasource_id, "
        "target.datasource_scan_id = source.datasource_scan_id, "
        "target.labels_json = source.labels_json, "
        "target.tags_json = source.tags_json, "
        "target.categories_json = source.categories_json, "
        "target.metadata_json = source.metadata_json, "
        "target.updated_at = current_timestamp() "
        "WHEN NOT MATCHED THEN INSERT (file_id, volume_path, file_path, datasource_id, datasource_scan_id, labels_json, tags_json, categories_json, metadata_json, updated_at) "
        "VALUES (source.file_id, source.volume_path, source.file_path, source.datasource_id, source.datasource_scan_id, source.labels_json, source.tags_json, source.categories_json, source.metadata_json, current_timestamp())"
    )
