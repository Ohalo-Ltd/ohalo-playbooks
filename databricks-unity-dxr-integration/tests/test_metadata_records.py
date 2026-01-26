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
                "annotation.21": ["Organization", "Person"],
                "annotation_stats#count.21": 5,
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
    assert record.datasource_scan_id == 99
    assert record.annotations_json is not None
    assert "annotation.21" in record.annotations_json
    assert record.annotation_stats_json is not None
    assert "annotation_stats#count.21" in record.annotation_stats_json


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




def test_extract_extracted_metadata_fields():
    from databricks_unity_dxr_integration.metadata_records import _extract_extracted_metadata_fields

    source = {
        "dxr#file_id": "abc123",
        "ds#file_name": "test.pdf",
        "extracted_metadata#1": "LEAP-1B-72-00-0369-01A-930A-D",
        "extracted_metadata#2": "ENGINE - GENERAL",
        "extracted_metadata#3": "Issue 001-00 - 2022-08-22",
        "other_field": "not metadata",
    }

    # Test without metadata definitions (should use raw field names)
    json_string, metadata_map = _extract_extracted_metadata_fields(source)

    assert json_string is not None
    assert "extracted_metadata#1" in json_string
    assert metadata_map["extracted_metadata#1"] == "LEAP-1B-72-00-0369-01A-930A-D"
    assert metadata_map["extracted_metadata#2"] == "ENGINE - GENERAL"
    assert metadata_map["extracted_metadata#3"] == "Issue 001-00 - 2022-08-22"
    assert len(metadata_map) == 3
    assert "other_field" not in metadata_map

    # Test with metadata definitions (should use display names)
    metadata_defs = {
        "1": "Service Bulletin (SB) Number Identification",
        "2": "Service Bulletin Title",
        "3": "Service Bulletin Issue Number & Effective Date",
    }
    json_string, metadata_map = _extract_extracted_metadata_fields(source, metadata_defs)

    assert json_string is not None
    assert "Service Bulletin (SB) Number Identification" in metadata_map
    assert metadata_map["Service Bulletin (SB) Number Identification"] == "LEAP-1B-72-00-0369-01A-930A-D"
    assert metadata_map["Service Bulletin Title"] == "ENGINE - GENERAL"
    assert metadata_map["Service Bulletin Issue Number & Effective Date"] == "Issue 001-00 - 2022-08-22"
    assert len(metadata_map) == 3


def test_extract_extracted_metadata_fields_handles_empty():
    from databricks_unity_dxr_integration.metadata_records import _extract_extracted_metadata_fields

    json_string, metadata_map = _extract_extracted_metadata_fields({})

    assert json_string is None
    assert metadata_map == {}


def test_extract_metadata_fields():
    from databricks_unity_dxr_integration.metadata_records import _extract_metadata_fields

    source = {
        "metadata#CREATED_BY": 12345,
        "metadata#MODIFIED_DATE": "2026-01-26T19:19:26Z",
        "metadata#binary_hash": "75f70238d7b45b9a67f8bbed7778fd146c52334663aa1e60c9966208264df109",
        "computed.metadata#OWNER": 54321,
        "dxr#file_id": "abc123",
        "other_field": "not metadata",
    }

    json_string, metadata_map = _extract_metadata_fields(source)

    assert json_string is not None
    # JSON should preserve original types
    assert '"metadata#CREATED_BY":12345' in json_string

    # Map should have string values (for Spark compatibility)
    assert "metadata#CREATED_BY" in metadata_map
    assert metadata_map["metadata#CREATED_BY"] == "12345"
    assert metadata_map["metadata#MODIFIED_DATE"] == "2026-01-26T19:19:26Z"
    assert metadata_map["metadata#binary_hash"] == "75f70238d7b45b9a67f8bbed7778fd146c52334663aa1e60c9966208264df109"
    assert metadata_map["computed.metadata#OWNER"] == "54321"
    assert "other_field" not in metadata_map
    assert "dxr#file_id" not in metadata_map


def test_build_metadata_records_with_full_metadata():
    volume_config = VolumeConfig(catalog="cat", schema="sch", volume="vol")
    file = VolumeFile(
        absolute_path="/Volumes/cat/sch/vol/file1.pdf",
        relative_path="file1.pdf",
        size_bytes=100,
        modification_time=10,
    )

    # Test with metadata definitions for human-readable field names
    metadata_defs = {
        "1": "Service Bulletin (SB) Number Identification",
        "2": "Service Bulletin Title",
    }

    hits = [
        {
            "_id": "abc",
            "_source": {
                "dxr#datasource_scan_id": 99,
                "dxr#file_id": "abc",
                "ds#file_name": "file1.pdf",
                "ds#file_size": 477537,
                "ds#parent_folder_paths": ["folder/subfolder"],
                "dxr#mime_type": "application/pdf",
                "dxr#indexed_date": "2026-01-26T19:19:38.885244433Z",
                "dxr#sha_256_hash": "3165e6d93eb95cf14766616bae73b8a5ed03a8320e08da6e1ee8cf01e14a9433",
                "dxr#sha_256_hash_file_meta": "205507f872c2f9274a79cab19b9ce2358432389b2b49e3aa1b903982a2e933f9",
                "dxr#doc_lang": "en",
                "dxr#composite_type": "SIMPLE",
                "dxr#is_processed": True,
                "dxr#document_status": "INDEXED",
                "dxr#text_extraction_status": "SUCCESS",
                "dxr#metadata_extraction_status": "SUCCESS",
                "dxr#tags": ["Important"],
                "dxr#manually_removed_tags": [],
                "dxr#ocr_used": False,
                "folder_id": "folder-123",
                "metadata#MODIFIED_DATE": "2026-01-26T19:19:26Z",
                "metadata#binary_hash": "75f70238d7b45b9a67f8bbed7778fd146c52334663aa1e60c9966208264df109",
                "metadata#CREATED_BY": 12345,
                "computed.metadata#OWNER": 54321,
                "ai#category": "Engineering Document",
                "ai#category_last_updated": "2026-01-26T19:20:00Z",
                "extracted_metadata#1": "LEAP-1B-72-00-0369-01A-930A-D",
                "extracted_metadata#2": "ENGINE - GENERAL (72-00-00)",
                "annotation.21": ["Organization", "Person"],
                "annotation.3": ["Phone Number"],
                "annotation_stats#count.21": 5,
                "annotation_stats#count.3": 2,
                "annotation_stats#unique_phrase_count.21": 3,
                "annotation_stats#unique_phrase_count.3": 1,
                "object_id": "93c6c82f-07b2-4be8-84af-8b5bc3e9f9b1/file1.pdf",
            },
        }
    ]

    records = build_metadata_records(
        volume_config=volume_config,
        job_id="job-1",
        datasource_id="42",
        hits=hits,
        known_files={file.upload_name: file},
        metadata_defs=metadata_defs,
    )

    assert len(records) == 1
    record = records[0]

    # Check basic fields
    assert record.catalog_name == "cat"
    assert record.schema_name == "sch"
    assert record.volume_name == "vol"
    assert record.datasource_scan_id == 99
    assert record.file_name == "file1.pdf"
    assert record.object_id == "93c6c82f-07b2-4be8-84af-8b5bc3e9f9b1/file1.pdf"

    # Check extracted metadata with display names
    assert "Service Bulletin (SB) Number Identification" in record.extracted_metadata_map
    assert record.extracted_metadata_map["Service Bulletin (SB) Number Identification"] == "LEAP-1B-72-00-0369-01A-930A-D"
    assert record.extracted_metadata_map["Service Bulletin Title"] == "ENGINE - GENERAL (72-00-00)"

    # Check DXR fields
    assert record.mime_type == "application/pdf"
    assert record.doc_language == "en"
    assert record.composite_type == "SIMPLE"
    assert record.is_processed is True
    assert record.document_status == "INDEXED"
    assert record.text_extraction_status == "SUCCESS"
    assert record.metadata_extraction_status == "SUCCESS"
    assert record.dxr_tags == ["Important"]
    assert record.removed_tags == []
    assert record.ocr_used is False

    # Check AI category fields
    assert record.ai_category == "Engineering Document"
    assert record.ai_category_last_updated == "2026-01-26T19:20:00Z"

    # Check annotation fields
    assert record.annotations_json is not None
    assert "annotation.21" in record.annotations_json
    assert "annotation.3" in record.annotations_json
    assert record.annotation_stats_json is not None
    assert "annotation_stats#count.21" in record.annotation_stats_json

    # Check metadata# and computed.metadata# fields
    assert record.metadata_fields_json is not None
    assert "metadata#MODIFIED_DATE" in record.metadata_fields_map
    assert record.metadata_fields_map["metadata#MODIFIED_DATE"] == "2026-01-26T19:19:26Z"
    assert record.metadata_fields_map["metadata#binary_hash"] == "75f70238d7b45b9a67f8bbed7778fd146c52334663aa1e60c9966208264df109"
    # Map values are strings for Spark compatibility
    assert record.metadata_fields_map["metadata#CREATED_BY"] == "12345"
    assert record.metadata_fields_map["computed.metadata#OWNER"] == "54321"
    # JSON should preserve original types
    assert '"metadata#CREATED_BY":12345' in record.metadata_fields_json

    # Check folder_id
    assert record.folder_id == "folder-123"
