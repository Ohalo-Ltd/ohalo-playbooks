from __future__ import annotations

import os
from contextlib import ExitStack
from typing import Dict, Iterable, List

try:  # pragma: no cover - available on Databricks
    from pyspark.dbutils import DBUtils
except ImportError:  # pragma: no cover
    DBUtils = object  # type: ignore

try:  # pragma: no cover
    from pyspark.sql import SparkSession
except ImportError:  # pragma: no cover
    SparkSession = object  # type: ignore

from .config import JobConfig, load_config
from .dxr_client import DataXRayClient, FileUpload, SubmittedJob
from .metadata_records import MetadataRecord, build_metadata_records
from .metadata_store import MetadataStore
from .volume import UnityVolumeScanner, VolumeFile


class UnityDXRJob:
    """Coordinates reading files from a Unity Catalog volume and persisting DXR metadata."""

    def __init__(self, config: JobConfig, spark: SparkSession, dbutils: DBUtils, dxr_client: DataXRayClient):
        self._config = config
        self._spark = spark
        self._dbutils = dbutils
        self._dxr = dxr_client
        self._scanner = UnityVolumeScanner(config.volume)
        self._metadata_store = MetadataStore(spark, config.metadata_table)
        self._metadata_store.ensure_table()

    def run(self) -> None:
        files = self._scanner.list_files()
        if not files:
            print("No files discovered in the configured volume.")
            return

        batches = plan_batches(
            files,
            max_bytes=self._config.dxr.max_bytes_per_job,
            max_files=self._config.dxr.max_files_per_job,
        )
        submissions: List[tuple[SubmittedJob, List[VolumeFile]]] = []
        for batch in batches:
            job = self._submit_batch(batch)
            submissions.append((job, batch))

        for job, batch in submissions:
            print(f"Polling job {job.job_id}...")
            result = self._dxr.wait_for_completion(job.job_id, self._config.dxr.poll_interval_seconds)
            if result.get("state") != "FINISHED":
                print(f"Job {job.job_id} ended in state {result.get('state')}, skipping metadata collection.")
                continue

            scan_id = result.get("datasourceScanId")
            if scan_id is None:
                print(f"Job {job.job_id} did not include a datasource scan id.")
                continue

            metadata = self._dxr.search_by_scan_id(scan_id)
            files_by_name = {file.upload_name: file for file in batch}
            records = build_metadata_records(
                volume_config=self._config.volume,
                job_id=job.job_id,
                datasource_id=self._config.dxr.datasource_id,
                hits=metadata,
                known_files=files_by_name,
            )
            self._metadata_store.upsert_records(records)
            print(f"Wrote {len(records)} metadata rows for job {job.job_id}.")

    def _submit_batch(self, batch: Iterable[VolumeFile]) -> SubmittedJob:
        uploads: List[FileUpload] = []
        stack = ExitStack()
        try:
            for file in batch:
                handle = stack.enter_context(open(file.absolute_path, "rb"))
                uploads.append(FileUpload(filename=file.upload_name, file_handle=handle))
            job = self._dxr.submit_job(uploads)
            print(f"Submitted {len(uploads)} files to job {job.job_id}.")
            return job
        finally:
            stack.close()


def plan_batches(files: Iterable[VolumeFile], max_bytes: int, max_files: int) -> List[List[VolumeFile]]:
    """Plan file batches while respecting byte and file-count limits per job."""
    if max_bytes <= 0:
        raise ValueError("max_bytes must be positive.")
    if max_files <= 0:
        raise ValueError("max_files must be positive.")

    batches: List[List[VolumeFile]] = []
    current: List[VolumeFile] = []
    current_size = 0

    for file in files:
        if file.size_bytes > max_bytes:
            raise ValueError(f"File {file.absolute_path} exceeds max_bytes_per_job limit.")
        if (current_size + file.size_bytes > max_bytes or len(current) >= max_files) and current:
            batches.append(current)
            current = []
            current_size = 0
        current.append(file)
        current_size += file.size_bytes

    if current:
        batches.append(current)

    return batches


def run_job() -> None:
    """Entry point for Databricks jobs."""
    config = load_config()
    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark)
    api_key = dbutils.secrets.get(scope=config.secret.scope, key=config.secret.key)
    dxr_client = DataXRayClient(config.dxr, api_key=api_key)

    job = UnityDXRJob(config=config, spark=spark, dbutils=dbutils, dxr_client=dxr_client)
    job.run()
