from datetime import datetime, timezone

from databricks_unity_dxr_integration.config import VolumeConfig
from databricks_unity_dxr_integration.metadata_records import MetadataRecord, build_metadata_records
from databricks_unity_dxr_integration.volume import VolumeFile


def test_build_metadata_records_matches_files_by_relative_path():
    volume_config = VolumeConfig(catalog="cat", schema="sch", volume="vol")
    file = VolumeFile(
        absolute_path="/Volumes/cat/sch/vol/file1.txt",
        relative_path="file1.txt",
        size_bytes=100,
        modification_time=10,
    )
    hits = [
        {
            "_id": "abc",
            "_source": {
                "dxr#datasource_scan_id": 99,
                "dxr#tags": ["Confidential"],
                "dxr#manually_removed_tags": ["Public"],
                "ds#parent_folder_paths": ["folder"],
                "dxr#is_processed": True,
                "dxr#ocr_used": False,
                "metadata#MODIFIED_DATE": "2024-01-01T00:00:00Z",
                "ds#file_name": "file1.txt",
                "folder_id": "folder",
                "annotations": "[]",
            },
        }
    ]

    records = build_metadata_records(
        volume_config=volume_config,
        job_id="job-1",
        datasource_id="42",
        hits=hits,
        known_files={file.upload_name: file},
    )

    assert len(records) == 1
    record = records[0]
    assert isinstance(record, MetadataRecord)
    assert record.catalog_name == "cat"
    assert record.dxr_tags == ["Confidential"]
    assert record.removed_tags == ["Public"]
    assert record.parent_paths == ["folder"]
    assert record.is_processed is True
    assert record.ocr_used is False
    assert record.annotations == "[]"
    assert record.datasource_scan_id == 99


def test_build_metadata_records_skips_unknown_files():
    volume_config = VolumeConfig(catalog="cat", schema="sch", volume="vol")
    hits = [{"_source": {"ds#file_name": "missing.txt"}}]

    records = build_metadata_records(
        volume_config=volume_config,
        job_id="job-1",
        datasource_id="42",
        hits=hits,
        known_files={},
    )

    assert records == []
