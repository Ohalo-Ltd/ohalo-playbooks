from __future__ import annotations

import logging
import os
import time
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

logger = logging.getLogger(__name__)


class UnityDXRJob:
    """Coordinates reading files from a Unity Catalog volume and persisting DXR metadata."""

    def __init__(self, config: JobConfig, spark: SparkSession, dbutils: DBUtils, dxr_client: DataXRayClient):
        self._config = config
        self._spark = spark
        self._dbutils = dbutils
        self._dxr = dxr_client
        self._scanner = UnityVolumeScanner(config.volume)
        self._metadata_store = MetadataStore(spark, config.metadata_table)
        self._metadata_store.ensure_table(drop_existing=config.drop_metadata_table)

    def run(self) -> None:
        files = self._scanner.list_files()
        if not files:
            logger.info("No files discovered in the configured volume.")
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
            try:
                logger.info(f"Polling job {job.job_id}...")
                result = self._dxr.wait_for_completion(job.job_id, self._config.dxr.poll_interval_seconds)
                if result.get("state") != "FINISHED":
                    logger.warning(f"Job {job.job_id} ended in state {result.get('state')}, attempting metadata collection anyway.")

                # Wait for metadata extraction child jobs to complete and index in Elasticsearch
                delay = self._config.dxr.metadata_extraction_delay_seconds
                if delay > 0:
                    logger.info(f"Waiting {delay} seconds for metadata extraction to complete and index...")
                    time.sleep(delay)

                scan_id = result.get("datasourceScanId")
                if scan_id is None:
                    logger.warning(f"Job {job.job_id} did not include a datasource scan id.")
                    continue

                metadata = self._dxr.search_by_scan_id(scan_id)

                # Enrich metadata with file details to get extractedMetadata if not present
                metadata = self._enrich_with_file_details(metadata)

                files_by_name = {file.upload_name: file for file in batch}
                records = build_metadata_records(
                    volume_config=self._config.volume,
                    job_id=job.job_id,
                    datasource_id=self._config.dxr.datasource_id,
                    hits=metadata,
                    known_files=files_by_name,
                )
                self._metadata_store.upsert_records(records)
                logger.info(f"Wrote {len(records)} metadata rows for job {job.job_id}.")
            except Exception as e:
                logger.error(f"Error processing job {job.job_id}: {e}")
                if self._config.dxr.debug:
                    logger.exception(f"Full traceback for job {job.job_id}:")
                continue

    def _enrich_with_file_details(self, hits: List[Dict]) -> List[Dict]:
        """Enrich search results with file details from the files API if extractedMetadata is missing."""
        enriched = []
        for hit in hits:
            source = hit.get("_source", {}) if isinstance(hit, dict) else {}
            if not isinstance(source, dict):
                enriched.append(hit)
                continue

            # Check if extractedMetadata is already present
            has_extracted_metadata = (
                "extractedMetadata" in source
                or "dxr#extractedMetadata" in source
                or "extractedMetadata" in hit
            )

            if has_extracted_metadata:
                enriched.append(hit)
                continue

            # Try to get file_id to fetch details
            file_id = source.get("dxr#file_id") or hit.get("_id")
            if not file_id:
                enriched.append(hit)
                continue

            # Fetch file details from files API
            try:
                file_details = self._dxr.get_file_metadata(file_id)
                # Merge additional fields from file details into source
                fields_to_merge = [
                    "extractedMetadata",
                    "scanDepth",
                    "contentSha256",
                    "createdAt",
                    "labels",
                    "dlpLabels",
                    "owner",
                    "createdBy",
                    "modifiedBy",
                    "datasource",
                    "annotators",
                ]
                for field in fields_to_merge:
                    if field in file_details:
                        source[field] = file_details[field]
                enriched.append(hit)
            except Exception as e:
                logger.warning(f"Failed to enrich file {file_id} with file details: {e}")
                enriched.append(hit)

        return enriched

    def _submit_batch(self, batch: Iterable[VolumeFile]) -> SubmittedJob:
        uploads: List[FileUpload] = []
        stack = ExitStack()
        try:
            for file in batch:
                handle = stack.enter_context(open(file.absolute_path, "rb"))
                uploads.append(FileUpload(filename=file.upload_name, file_handle=handle))
            job = self._dxr.submit_job(uploads)
            logger.info(f"Submitted {len(uploads)} files to job {job.job_id}.")
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
