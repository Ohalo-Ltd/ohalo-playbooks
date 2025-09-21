import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from atlan_dxr_integration.dataset_builder import DatasetBuilder, SampleFile
from atlan_dxr_integration.dxr_client import Classification


def _classification(**overrides):
    data = {
        "id": "123",
        "name": "Heart Failure",
        "type": "ANNOTATOR",
        "subtype": "REGEX",
        "description": "Files classified under the disease Heart Failure",
        "link": "/resource/123",
        "searchLink": "/resource-search/123",
    }
    data.update(overrides)
    return Classification.from_dict(data)


def test_dataset_builder_aggregates_files_and_samples():
    classification = _classification()
    builder = DatasetBuilder([classification], dxr_base_url="https://atlan.dataxray.io/api")

    file_payload = {
        "id": "file-1",
        "name": "patient_data.csv",
        "path": "/clinical/patient_data.csv",
        "link": "/resource/file-1",
        "labels": [{"id": "123"}],
    }
    builder.consume_file(file_payload)

    records = builder.build()
    assert len(records) == 1
    record = records[0]

    assert record.file_count == 1
    assert record.source_url == "https://atlan.dataxray.io/resource-search/123"
    assert record.description is not None
    assert "1 file" in record.description
    assert record.sample_files[0].render() == "patient_data.csv – /clinical/patient_data.csv"


def test_dataset_builder_filters_types():
    classification = _classification(type="ANNOTATOR")
    builder = DatasetBuilder([classification], allowed_types=["LABEL"])
    records = builder.build()
    assert records == []


def test_dataset_builder_handles_multiple_labels():
    classification = _classification(id="321", name="Cardiology")
    builder = DatasetBuilder([classification], sample_file_limit=1)
    file_payload = {
        "id": "file-2",
        "filename": "report.pdf",
        "filepath": "/reports/report.pdf",
        "classifications": ["321", {"classificationId": "321"}],
    }
    builder.consume_file(file_payload)
    records = builder.build()
    record = records[0]
    assert record.file_count == 2
    assert len(record.sample_files) == 1
    assert isinstance(record.sample_files[0], SampleFile)
