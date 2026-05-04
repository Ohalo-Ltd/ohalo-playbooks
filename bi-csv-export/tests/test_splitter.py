"""Unit tests for dxr_to_csv.splitter.

All tests are self-contained (no network, no files on disk).
Run with: pytest -m unit
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

import sys
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from dxr_to_csv.splitter import Splitter, _infer_type, _pick_person_id

FIXTURES_PATH = Path(__file__).parent / "fixtures" / "sample_files.jsonl"


def _load_fixtures() -> list[dict]:
    records = []
    with open(FIXTURES_PATH) as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestHelpers:
    def test_infer_type_bool(self):
        assert _infer_type(True) == "BOOLEAN"
        assert _infer_type(False) == "BOOLEAN"

    def test_infer_type_number(self):
        assert _infer_type(42) == "NUMBER"
        assert _infer_type(3.14) == "NUMBER"

    def test_infer_type_text(self):
        assert _infer_type("hello") == "TEXT"
        assert _infer_type(None) == "TEXT"

    def test_pick_person_id_email(self):
        assert _pick_person_id({"email": "a@b.com", "displayName": "A"}) == "a@b.com"

    def test_pick_person_id_upn_fallback(self):
        assert _pick_person_id({"userPrincipalName": "a@domain.com"}) == "a@domain.com"

    def test_pick_person_id_display_name_fallback(self):
        assert _pick_person_id({"displayName": "Alice"}) == "Alice"

    def test_pick_person_id_empty(self):
        assert _pick_person_id({}) is None


# ---------------------------------------------------------------------------
# Splitter — basic structure
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestSplitterStructure:
    def test_tables_keys_always_present(self):
        splitter = Splitter()
        tables = splitter.tables()
        assert set(tables.keys()) == {
            "files", "labels", "annotations", "extracted_metadata", "dlp_labels"
        }

    def test_empty_splitter_returns_empty_tables(self):
        splitter = Splitter()
        tables = splitter.tables()
        for rows in tables.values():
            assert rows == []

    def test_file_count(self):
        splitter = Splitter()
        records = _load_fixtures()
        for r in records:
            splitter.ingest(r)
        assert splitter.file_count == len(records)


# ---------------------------------------------------------------------------
# Splitter — files table
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestFilesTable:
    def setup_method(self):
        self.splitter = Splitter()
        for r in _load_fixtures():
            self.splitter.ingest(r)
        self.rows = {r["file_id"]: r for r in self.splitter.tables()["files"]}

    def test_one_row_per_file(self):
        assert len(self.rows) == 4

    def test_scalar_fields_present(self):
        f001 = self.rows["f001"]
        assert f001["file_name"] == "employee_records_2024.xlsx"
        assert f001["size_bytes"] == 248320
        assert f001["mime_type"] == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        assert f001["datasource_id"] == 101
        assert f001["datasource_name"] == "SharePoint Production"

    def test_label_count(self):
        assert self.rows["f001"]["label_count"] == 2
        assert self.rows["f003"]["label_count"] == 0

    def test_annotation_count(self):
        # f001 has PERSON(2) + EMAIL(2) + PHONE(1) = 5
        assert self.rows["f001"]["annotation_count"] == 5

    def test_owner_email(self):
        assert self.rows["f001"]["owner_email"] == "hr-admin@acme.com"


# ---------------------------------------------------------------------------
# Splitter — labels table
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestLabelsTable:
    def setup_method(self):
        splitter = Splitter()
        for r in _load_fixtures():
            splitter.ingest(r)
        self.rows = splitter.tables()["labels"]

    def test_row_count(self):
        # f001:2, f002:1, f003:0, f004:1 = 4
        assert len(self.rows) == 4

    def test_pii_label_on_f001(self):
        pii_rows = [r for r in self.rows if r["file_id"] == "f001" and r["label_name"] == "PII"]
        assert len(pii_rows) == 1
        assert pii_rows[0]["label_hex_color"] == "E74C3C"
        assert pii_rows[0]["label_type"] == "SMART"

    def test_file_with_no_labels_absent(self):
        f003_labels = [r for r in self.rows if r["file_id"] == "f003"]
        assert f003_labels == []


# ---------------------------------------------------------------------------
# Splitter — annotations table
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestAnnotationsTable:
    def setup_method(self):
        splitter = Splitter()
        for r in _load_fixtures():
            splitter.ingest(r)
        self.rows = splitter.tables()["annotations"]

    def test_row_count(self):
        # f001:5, f002:2, f003:4, f004:1 = 12
        assert len(self.rows) == 12

    def test_annotation_fields(self):
        email_rows = [r for r in self.rows if r["value"] == "alice.johnson@acme.com"]
        assert len(email_rows) == 1
        row = email_rows[0]
        assert row["file_id"] == "f001"
        assert row["annotator_id"] == 42
        assert row["annotator_name"] == "EMAIL_ADDRESS"
        assert row["start"] == 60
        assert row["end"] == 82

    def test_person_names_present(self):
        person_rows = [r for r in self.rows if r["annotator_name"] == "PERSON"]
        values = {r["value"] for r in person_rows}
        assert "Alice Johnson" in values
        assert "Carlos Mendez" in values


# ---------------------------------------------------------------------------
# Splitter — extracted_metadata table
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestExtractedMetadataTable:
    def setup_method(self):
        splitter = Splitter()
        for r in _load_fixtures():
            splitter.ingest(r)
        self.rows = splitter.tables()["extracted_metadata"]

    def test_row_count(self):
        # f001:3, f002:2, f003:3, f004:1 = 9
        assert len(self.rows) == 9

    def test_boolean_value_and_type(self):
        rows = [r for r in self.rows if r["file_id"] == "f001" and r["field_name"] == "document_contains_salary"]
        assert len(rows) == 1
        assert rows[0]["value"] == "True"
        assert rows[0]["data_type"] == "BOOLEAN"

    def test_string_field(self):
        rows = [r for r in self.rows if r["field_name"] == "contract_type"]
        assert len(rows) == 1
        assert rows[0]["value"] == "NDA"
        assert rows[0]["data_type"] == "TEXT"


# ---------------------------------------------------------------------------
# Splitter — dlp_labels table
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestDlpLabelsTable:
    def setup_method(self):
        splitter = Splitter()
        for r in _load_fixtures():
            splitter.ingest(r)
        self.rows = splitter.tables()["dlp_labels"]

    def test_row_count(self):
        # f001:0, f002:2, f003:0, f004:0 = 2
        assert len(self.rows) == 2

    def test_dlp_label_fields(self):
        fin_rows = [r for r in self.rows if r["dlp_label"] == "FINANCIAL_DATA"]
        assert len(fin_rows) == 1
        assert fin_rows[0]["file_id"] == "f002"
        assert fin_rows[0]["score"] == pytest.approx(0.92)
        assert fin_rows[0]["category"] == "Financial"


# ---------------------------------------------------------------------------
# Splitter — edge cases
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestEdgeCases:
    def test_missing_optional_fields(self):
        """Records with absent nested arrays should not raise."""
        sparse = {"fileId": "x", "fileName": "sparse.txt", "path": "/sparse.txt"}
        splitter = Splitter()
        splitter.ingest(sparse)
        tables = splitter.tables()
        assert len(tables["files"]) == 1
        assert len(tables["labels"]) == 0
        assert len(tables["annotations"]) == 0
        assert len(tables["extracted_metadata"]) == 0
        assert len(tables["dlp_labels"]) == 0

    def test_id_field_fallback(self):
        """Records using 'id' instead of 'fileId' are handled."""
        record = {"id": "alt-id", "fileName": "alt.txt", "path": "/alt.txt"}
        splitter = Splitter()
        splitter.ingest(record)
        assert splitter.tables()["files"][0]["file_id"] == "alt-id"

    def test_multiple_ingests_accumulate(self):
        splitter = Splitter()
        for r in _load_fixtures():
            splitter.ingest(r)
        # Ingest the same fixture twice
        for r in _load_fixtures():
            splitter.ingest(r)
        assert splitter.file_count == 8
