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


def test_build_metadata_records_extracts_owner_info():
    from databricks_unity_dxr_integration.metadata_records import _extract_user_info

    user_obj = {
        "id": "user-123",
        "name": "John Doe",
        "email": "john@example.com",
        "accountType": "USER",
    }

    name, email, user_id = _extract_user_info(user_obj)

    assert name == "John Doe"
    assert email == "john@example.com"
    assert user_id == "user-123"


def test_extract_user_info_handles_none():
    from databricks_unity_dxr_integration.metadata_records import _extract_user_info

    name, email, user_id = _extract_user_info(None)

    assert name is None
    assert email is None
    assert user_id is None


def test_build_metadata_records_extracts_metadata_fields():
    from databricks_unity_dxr_integration.metadata_records import _extract_metadata_fields

    extracted_metadata = [
        {"id": "1", "name": "Title", "value": "Test Document", "type": "TEXT"},
        {"id": "2", "name": "Author", "value": "John Doe", "type": "TEXT"},
    ]

    json_string, metadata_map = _extract_metadata_fields(extracted_metadata)

    assert json_string is not None
    assert "Title" in json_string
    assert metadata_map["Title"] == "Test Document"
    assert metadata_map["Author"] == "John Doe"


def test_extract_metadata_fields_handles_empty():
    from databricks_unity_dxr_integration.metadata_records import _extract_metadata_fields

    json_string, metadata_map = _extract_metadata_fields([])

    assert json_string is None
    assert metadata_map == {}


def test_extract_metadata_fields_from_source():
    from databricks_unity_dxr_integration.metadata_records import _extract_metadata_fields_from_source

    source = {
        "dxr#file_id": "abc123",
        "ds#file_name": "test.pdf",
        "extracted_metadata#1": "LEAP-1B-72-00-0369-01A-930A-D",
        "extracted_metadata#2": "ENGINE - GENERAL",
        "extracted_metadata#3": "Issue 001-00 - 2022-08-22",
        "other_field": "not metadata",
    }

    # Test without metadata definitions (should use raw field names)
    json_string, metadata_map = _extract_metadata_fields_from_source(source)

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
    json_string, metadata_map = _extract_metadata_fields_from_source(source, metadata_defs)

    assert json_string is not None
    assert "Service Bulletin (SB) Number Identification" in metadata_map
    assert metadata_map["Service Bulletin (SB) Number Identification"] == "LEAP-1B-72-00-0369-01A-930A-D"
    assert metadata_map["Service Bulletin Title"] == "ENGINE - GENERAL"
    assert metadata_map["Service Bulletin Issue Number & Effective Date"] == "Issue 001-00 - 2022-08-22"
    assert len(metadata_map) == 3


def test_extract_metadata_fields_from_source_handles_empty():
    from databricks_unity_dxr_integration.metadata_records import _extract_metadata_fields_from_source

    json_string, metadata_map = _extract_metadata_fields_from_source({})

    assert json_string is None
    assert metadata_map == {}


def test_build_metadata_records_extracts_annotators():
    from databricks_unity_dxr_integration.metadata_records import _extract_annotators_info

    annotators = [
        {
            "id": "ann-1",
            "name": "Organization",
            "uniquePhrases": 5,
            "domain": {"name": "Non-sensitive"},
        },
        {
            "id": "ann-2",
            "name": "Person",
            "uniquePhrases": 3,
            "domain": {"name": "Personal Data"},
        },
    ]

    json_string, summary = _extract_annotators_info(annotators)

    assert json_string is not None
    assert "Organization" in json_string
    assert summary["Organization"] == 5
    assert summary["Person"] == 3


def test_build_metadata_records_with_full_metadata():
    volume_config = VolumeConfig(catalog="cat", schema="sch", volume="vol")
    file = VolumeFile(
        absolute_path="/Volumes/cat/sch/vol/file1.pdf",
        relative_path="file1.pdf",
        size_bytes=100,
        modification_time=10,
    )
    hits = [
        {
            "_id": "abc",
            "_source": {
                "dxr#datasource_scan_id": 99,
                "dxr#file_id": "abc",
                "ds#file_name": "file1.pdf",
                "extracted_metadata#1": "LEAP-1B-72-00-0369-01A-930A-D",
                "extracted_metadata#2": "ENGINE - GENERAL (72-00-00)",
                "scanDepth": "DISCOVERY_AND_CLASSIFICATION",
                "contentSha256": "abc123",
                "createdAt": "2024-01-01T00:00:00Z",
                "labels": ["Important"],
                "dlpLabels": ["Confidential"],
                "owner": {
                    "name": "Alice Smith",
                    "email": "alice@example.com",
                    "id": "user-1",
                },
                "createdBy": {
                    "name": "Bob Jones",
                    "email": "bob@example.com",
                },
                "modifiedBy": {
                    "name": "Carol White",
                    "email": "carol@example.com",
                },
                "datasource": {
                    "name": "SharePoint",
                    "connector": {
                        "type": "SHAREPOINT_ONLINE_GRAPH_API",
                        "siteUrl": "https://company.sharepoint.com/sites/docs",
                    },
                },
                "annotators": [
                    {"name": "Organization", "uniquePhrases": 3}
                ],
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
    assert record.extracted_metadata_map["extracted_metadata#1"] == "LEAP-1B-72-00-0369-01A-930A-D"
    assert record.extracted_metadata_map["extracted_metadata#2"] == "ENGINE - GENERAL (72-00-00)"
    assert record.scan_depth == "DISCOVERY_AND_CLASSIFICATION"
    assert record.content_sha256 == "abc123"
    assert record.created_at == "2024-01-01T00:00:00Z"
    assert record.labels == ["Important"]
    assert record.dlp_labels == ["Confidential"]
    assert record.owner_name == "Alice Smith"
    assert record.owner_email == "alice@example.com"
    assert record.created_by_name == "Bob Jones"
    assert record.modified_by_email == "carol@example.com"
    assert record.datasource_name == "SharePoint"
    assert record.connector_type == "SHAREPOINT_ONLINE_GRAPH_API"
    assert record.connector_site_url == "https://company.sharepoint.com/sites/docs"
    assert record.annotators_summary["Organization"] == 3
