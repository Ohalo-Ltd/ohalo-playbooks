from databricks_unity_dxr_integration.metadata import MetadataRecord, extract_metadata_records


def test_extract_metadata_records_maps_fields():
    hits = [
        {
            "_id": "hit-1",
            "_source": {
                "dxr#file_id": "file-123",
                "dxr#datasource_id": 42,
                "dxr#datasource_scan_id": 99,
                "dxr#labels": ["Finance"],
                "dxr#tags": ["Confidential"],
                "ai#category": ["Financial"],
                "ds#file_name": "/Volumes/cat/sch/vol/file.txt",
            },
        }
    ]

    records = extract_metadata_records("job-1", hits)

    assert len(records) == 1
    record = records[0]
    assert isinstance(record, MetadataRecord)
    assert record.file_id == "file-123"
    assert record.datasource_id == 42
    assert record.datasource_scan_id == 99
    assert record.dxr_labels == ["Finance"]
    assert record.dxr_tags == ["Confidential"]
    assert record.dxr_categories == ["Financial"]
    assert record.metadata_json["dxr#file_id"] == "file-123"


def test_extract_metadata_records_handles_missing_fields():
    hits = [{}]

    records = extract_metadata_records("job-2", hits)

    assert len(records) == 1
    record = records[0]
    assert record.file_id == "job-2-0"
    assert record.dxr_labels == []
    assert record.metadata_json == {}
