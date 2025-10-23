from typing import Dict, List, Optional, Tuple

from databricks_unity_dxr_integration.checkpoint import FileCheckpoint, InMemoryCheckpointStore
from databricks_unity_dxr_integration.metadata import MetadataRecord
from databricks_unity_dxr_integration.config import DataXRayConfig, DatabricksConfig, IntegrationConfig
from databricks_unity_dxr_integration.databricks_client import VolumeFile
from databricks_unity_dxr_integration.dxr_client import SubmittedJob, FileUpload
from databricks_unity_dxr_integration.pipeline import (
    BatchSubmission,
    CollectResult,
    DXRDatabricksPipeline,
    plan_batches,
)


def test_plan_batches_respects_max_bytes():
    files = [
        VolumeFile(path="a", size=5, modification_time=1),
        VolumeFile(path="b", size=4, modification_time=1),
        VolumeFile(path="c", size=6, modification_time=1),
    ]

    batches = plan_batches(files, max_bytes=10)

    assert len(batches) == 2
    assert [f.path for f in batches[0]] == ["a", "b"]
    assert [f.path for f in batches[1]] == ["c"]


def test_plan_batches_raises_when_single_file_exceeds_limit():
    files = [
        VolumeFile(path="a", size=15, modification_time=1),
    ]

    try:
        plan_batches(files, max_bytes=10)
    except ValueError as exc:
        assert "exceeds" in str(exc)
    else:
        raise AssertionError("Expected ValueError for oversized file")


def test_ingest_submits_new_files_and_updates_checkpoint():
    files = [
        VolumeFile(path="/Volumes/cat/sch/vol/file1.txt", size=5, modification_time=1),
        VolumeFile(path="/Volumes/cat/sch/vol/file2.txt", size=6, modification_time=2),
    ]
    databricks = StubDatabricksClient(files=files, contents={f.path: b"data" for f in files})
    dxr = StubDXRClient()
    store = InMemoryCheckpointStore()
    pipeline = build_pipeline(databricks, dxr, store)

    submissions = pipeline.ingest()

    assert len(submissions) == 1
    submission = submissions[0]
    assert isinstance(submission, BatchSubmission)
    assert submission.job is not None
    assert dxr.submissions == 1
    assert [upload.filename for upload in dxr.last_uploads] == ["file1.txt", "file2.txt"]
    assert set(store.load().keys()) == {files[0].path, files[1].path}


def test_ingest_skips_unmodified_files():
    file = VolumeFile(path="/Volumes/cat/sch/vol/file.txt", size=5, modification_time=1)
    databricks = StubDatabricksClient(files=[file], contents={file.path: b"data"})
    dxr = StubDXRClient()
    store = InMemoryCheckpointStore(
        initial={
            file.path: FileCheckpoint(
                volume_path="/Volumes/cat/sch/vol",
                file_path=file.path,
                file_size=file.size,
                modification_time=file.modification_time,
                checksum=None,
                datasource_scan_id=101,
            )
        }
    )
    pipeline = build_pipeline(databricks, dxr, store)

    submissions = pipeline.ingest()

    assert submissions == []
    assert dxr.submissions == 0


def test_collect_returns_metadata_for_finished_jobs():
    files = [
        VolumeFile(path="/Volumes/cat/sch/vol/file1.txt", size=5, modification_time=1),
    ]
    databricks = StubDatabricksClient(files=files, contents={files[0].path: b"data"})
    dxr = StubDXRClient()
    store = InMemoryCheckpointStore()
    pipeline = build_pipeline(databricks, dxr, store)

    submissions = pipeline.ingest()
    job_ids = [submission.job.job_id for submission in submissions if submission.job]

    for submission in submissions:
        dxr.set_job_state(
            submission.job.job_id,
            state="FINISHED",
            metadata=[{"_source": {"id": submission.job.job_id}}],
        )

    results = pipeline.collect(job_ids)

    assert len(results) == len(job_ids)
    assert all(result.state == "FINISHED" for result in results)
    assert results[0].metadata[0]["_source"]["id"] == submissions[0].job.job_id


def test_collect_returns_empty_when_no_jobs():
    pipeline = build_pipeline(
        databricks=StubDatabricksClient(files=[], contents={}),
        dxr=StubDXRClient(),
        store=InMemoryCheckpointStore(),
    )

    assert pipeline.collect([]) == []


def test_collect_records_job_and_metadata_with_checkpoint_store():
    files = [
        VolumeFile(path="/Volumes/cat/sch/vol/file1.txt", size=5, modification_time=1),
    ]
    databricks = StubDatabricksClient(files=files, contents={files[0].path: b"data"})
    dxr = StubDXRClient()
    store = RecordingCheckpointStore()
    pipeline = build_pipeline(databricks, dxr, store)

    submissions = pipeline.ingest()
    job_id = submissions[0].job.job_id

    dxr.set_job_state(
        job_id,
        state="FINISHED",
        metadata=[{"_id": "file-123", "_source": {"dxr#file_id": "file-123", "ds#file_name": "file1"}}],
    )

    pipeline.collect([job_id])

    assert any(entry[0] == job_id and entry[1] == "SUBMITTED" for entry in store.recorded_jobs)
    assert any(entry[0] == job_id and entry[1] == "FINISHED" for entry in store.recorded_jobs)
    assert store.metadata_records
    assert store.metadata_records[0].file_id == "file-123"


def build_pipeline(databricks, dxr, store) -> DXRDatabricksPipeline:
    config = IntegrationConfig(
        databricks=DatabricksConfig(
            host="https://workspace",
            token="token",
            catalog="cat",
            schema="sch",
            source_volume="vol",
            label_volume_prefix="prefix_",
        ),
        data_xray=DataXRayConfig(
            base_url="https://dxr.example/api",
            api_key="token",
            datasource_id="123",
            poll_interval_seconds=5,
            max_bytes_per_job=20,
        ),
    )
    return DXRDatabricksPipeline(
        config,
        databricks_client=databricks,
        dxr_client=dxr,
        checkpoint_store=store,
    )


class StubDatabricksClient:
    def __init__(self, files: List[VolumeFile], contents: Dict[str, bytes]):
        self._files = files
        self._contents = contents
        self.source_volume_uri = "/Volumes/cat/sch/vol"

    def list_files(self, prefix=None):
        return list(self._files)

    def read_file(self, path: str) -> bytes:
        return self._contents[path]


class StubDXRClient:
    def __init__(self):
        self.submissions = 0
        self.last_uploads: List[FileUpload] = []

        self.jobs: Dict[str, dict] = {}
        self.metadata: Dict[int, List[dict]] = {}

    def submit_job(self, uploads):
        self.submissions += 1
        self.last_uploads = list(uploads)
        job_id = f"job-{self.submissions}"
        scan_id = 100 + self.submissions
        self.jobs[job_id] = {"id": job_id, "state": "UPLOADING_FILES", "datasourceScanId": scan_id}
        self.metadata[scan_id] = []
        return SubmittedJob(job_id=job_id, datasource_scan_id=scan_id)

    def set_job_state(self, job_id: str, *, state: str, metadata: List[dict]):
        job = self.jobs[job_id]
        job["state"] = state
        scan_id = job["datasourceScanId"]
        self.metadata[scan_id] = metadata

    def get_job(self, job_id: str) -> dict:
        return dict(self.jobs[job_id])

    def search_by_scan_id(self, scan_id: int) -> List[dict]:
        return list(self.metadata.get(scan_id, []))


class RecordingCheckpointStore(InMemoryCheckpointStore):
    def __init__(self):
        super().__init__()
        self.recorded_jobs: List[Tuple[str, str, Optional[int]]] = []
        self.metadata_records: List[MetadataRecord] = []

    def record_job(self, job_id: str, state: str, datasource_scan_id: Optional[int]) -> None:
        self.recorded_jobs.append((job_id, state, datasource_scan_id))

    def persist_metadata(self, records: List[MetadataRecord]) -> None:
        self.metadata_records.extend(records)
