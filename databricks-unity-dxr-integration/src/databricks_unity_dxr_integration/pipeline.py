from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from typing import Iterable, List, Optional, Sequence

from .config import IntegrationConfig, load_config
from .databricks_client import DatabricksVolumeClient, VolumeFile
from .dxr_client import DataXRayClient, FileUpload, SubmittedJob
from .checkpoint import (
    CheckpointStore,
    DatabricksSQLBackend,
    DeltaCheckpointStore,
    FileCheckpoint,
    InMemoryCheckpointStore,
)
from .metadata import extract_metadata_records


@dataclass
class BatchSubmission:
    job: Optional[SubmittedJob]
    files: List[VolumeFile]


@dataclass
class CollectResult:
    job_id: str
    state: Optional[str]
    datasource_scan_id: Optional[int]
    metadata: List[dict]


class DXRDatabricksPipeline:
    """Coordinates the end-to-end flow between Databricks and Data X-Ray."""

    def __init__(
        self,
        config: IntegrationConfig,
        databricks_client: Optional[DatabricksVolumeClient] = None,
        dxr_client: Optional[DataXRayClient] = None,
        checkpoint_store: Optional[CheckpointStore] = None,
    ):
        self._config = config
        self._databricks = databricks_client or DatabricksVolumeClient(config.databricks)
        self._dxr = dxr_client or DataXRayClient(config.data_xray)
        if checkpoint_store is not None:
            self._checkpoint_store = checkpoint_store
        else:
            self._checkpoint_store = self._build_default_checkpoint_store()

    def ingest(self, prefix: Optional[str] = None, dry_run: bool = False) -> List[BatchSubmission]:
        """List Databricks files, batch them, and submit to Data X-Ray."""
        checkpoints = self._checkpoint_store.load()
        files: List[VolumeFile]
        try:
            files = list(self._databricks.list_files(prefix=prefix))
        except Exception as exc:
            if dry_run:
                print(
                    f"[dry-run] Unable to list files from Databricks ({exc}). "
                    "Continuing with an empty file set."
                )
                files = []
            else:
                raise
        candidates = [file for file in files if self._should_process(file, checkpoints)]
        batches = plan_batches(candidates, self._config.data_xray.max_bytes_per_job)

        submissions: List[BatchSubmission] = []
        for batch in batches:
            uploads = [self._build_upload(file) for file in batch]
            job: Optional[SubmittedJob] = None
            if dry_run:
                job = None
            else:
                job = self._dxr.submit_job(uploads)

            submissions.append(BatchSubmission(job=job, files=batch))
            if job:
                for file in batch:
                    self._checkpoint_store.upsert(
                        FileCheckpoint(
                            volume_path=self._databricks.source_volume_uri,
                            file_path=file.path,
                            file_size=file.size,
                            modification_time=file.modification_time,
                            checksum=None,
                            datasource_scan_id=job.datasource_scan_id,
                        )
                    )
                self._checkpoint_store.record_job(
                    job_id=job.job_id,
                    state="SUBMITTED",
                    datasource_scan_id=job.datasource_scan_id,
                )
        return submissions

    def collect(self, job_ids: Sequence[str]) -> List["CollectResult"]:
        """Poll job statuses and fetch metadata."""
        if not job_ids:
            return []

        results: List[CollectResult] = []
        for job_id in job_ids:
            job = self._dxr.get_job(job_id)
            scan_id = job.get("datasourceScanId")
            metadata_hits = self._dxr.search_by_scan_id(scan_id) if scan_id is not None else []
            self._checkpoint_store.record_job(
                job_id=job_id,
                state=str(job.get("state", "UNKNOWN")),
                datasource_scan_id=scan_id,
            )
            metadata_records = extract_metadata_records(job_id, metadata_hits)
            if metadata_records:
                self._checkpoint_store.persist_metadata(metadata_records)

            results.append(
                CollectResult(
                    job_id=job_id,
                    state=job.get("state"),
                    datasource_scan_id=scan_id,
                    metadata=metadata_hits,
                )
            )
        return results

    def run_once(self) -> None:
        """Execute a single synchronization pass."""
        submissions = self.ingest()
        job_ids = [submission.job.job_id for submission in submissions if submission.job]
        self.collect(job_ids)

    def _should_process(self, file: VolumeFile, checkpoints: dict[str, FileCheckpoint]) -> bool:
        record = checkpoints.get(file.path)
        if not record:
            return True

        return record.file_size != file.size or record.modification_time != file.modification_time

    def _build_upload(self, file: VolumeFile) -> FileUpload:
        data = self._databricks.read_file(file.path)
        filename = os.path.basename(file.path)
        return FileUpload(filename=filename or file.path, data=data)

    def _build_default_checkpoint_store(self) -> CheckpointStore:
        cfg = self._config.databricks
        if cfg.checkpoint_table and cfg.warehouse_id:
            backend = DatabricksSQLBackend(self._databricks.workspace, cfg.warehouse_id)
            return DeltaCheckpointStore(
                sql_backend=backend,
                table_name=cfg.checkpoint_table,
                ledger_table=cfg.job_ledger_table,
                metadata_table=cfg.metadata_table,
            )
        return InMemoryCheckpointStore()


def plan_batches(files: Iterable[VolumeFile], max_bytes: int) -> List[List[VolumeFile]]:
    """Plan file batches while respecting max bytes per job."""
    batches: List[List[VolumeFile]] = []
    current: List[VolumeFile] = []
    current_size = 0

    for file in files:
        if file.size > max_bytes:
            # TODO: Support chunked upload of large files.
            raise ValueError(f"File {file.path} exceeds max_bytes_per_job limit.")

        if current_size + file.size > max_bytes and current:
            batches.append(current)
            current = []
            current_size = 0

        current.append(file)
        current_size += file.size

    if current:
        batches.append(current)

    return batches


def main() -> None:
    """Entry point with default configuration loading."""
    parser = argparse.ArgumentParser(description="Run Databricks ↔ Data X-Ray integration pipeline.")
    parser.add_argument("--mode", choices=["ingest", "collect", "run-once"], default="ingest")
    parser.add_argument("--prefix", default=None, help="Optional prefix within the source volume.")
    parser.add_argument("--dry-run", action="store_true", help="Run without submitting jobs to Data X-Ray.")
    parser.add_argument(
        "--job-id",
        dest="job_ids",
        action="append",
        help="Job ID to collect. Provide multiple times to process several jobs.",
    )

    args = parser.parse_args()

    if args.dry_run:
        os.environ.setdefault("DATABRICKS_HOST", "https://example.com")
        os.environ.setdefault("DATABRICKS_TOKEN", "dry-run-token")
        os.environ.setdefault("DATABRICKS_CATALOG", "dry_run_catalog")
        os.environ.setdefault("DATABRICKS_SCHEMA", "dry_run_schema")
        os.environ.setdefault("DATABRICKS_SOURCE_VOLUME", "dry_run_volume")
        os.environ.setdefault("DXR_BASE_URL", "https://dxr.example/api")
        os.environ.setdefault("DXR_API_KEY", "dry-run-dxr-token")
        os.environ.setdefault("DXR_DATASOURCE_ID", "0")

    config = load_config()
    pipeline = DXRDatabricksPipeline(config)
    if args.mode == "ingest":
        pipeline.ingest(prefix=args.prefix, dry_run=args.dry_run)
    elif args.mode == "collect":
        pipeline.collect(args.job_ids or [])
    else:
        pipeline.run_once()


if __name__ == "__main__":
    main()
