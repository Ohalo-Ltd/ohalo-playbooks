"""Split a flat stream of /api/v1/files JSONL records into normalised row tables.

Each file record from DXR contains nested collections (labels, annotations,
extracted metadata, DLP labels).  Loading these nested arrays directly into a
BI tool produces one opaque cell per file.  This module *explodes* them into
separate relational tables so every BI query is a simple flat filter.

Tables produced
---------------
``files``
    One row per file.  Contains the scalar file properties: IDs, path, size,
    MIME type, timestamps, datasource, scan depth, owner info.

``labels``
    One row per (file, label) pair.  Contains label ID, name, colour, and type.
    A file with three labels appears three times here.

``annotations``
    One row per (file, annotator, annotation-value) triple.  Contains the
    annotator ID, entity type (if known), the detected value, and its character
    offsets within the document.

``extracted_metadata``
    One row per (file, extractor field) pair.  Contains the extractor field
    name and its string/boolean/numeric value.  Only present when LLM metadata
    extraction is configured on the datasource.

``dlp_labels``
    One row per (file, DLP label) pair.  Contains the DLP label name and any
    associated score or metadata.

Usage::

    from dxr_to_csv.splitter import Splitter

    splitter = Splitter()
    for record in stream:
        splitter.ingest(record)

    tables = splitter.tables()   # dict[str, list[dict]]
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class Splitter:
    """Accumulates /api/v1/files JSONL records and splits them into 5 tables.

    Call :meth:`ingest` for each record, then :meth:`tables` to retrieve the
    result.  The splitter is intentionally stateful so it can be used in a
    streaming fashion without loading the entire response into memory first.
    """

    _files: List[Dict[str, Any]] = field(default_factory=list)
    _labels: List[Dict[str, Any]] = field(default_factory=list)
    _annotations: List[Dict[str, Any]] = field(default_factory=list)
    _extracted_metadata: List[Dict[str, Any]] = field(default_factory=list)
    _dlp_labels: List[Dict[str, Any]] = field(default_factory=list)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def ingest(self, record: Dict[str, Any]) -> None:
        """Process a single /api/v1/files record."""
        file_id = record.get("fileId") or record.get("id")

        self._files.append(_project_file_row(record, file_id))
        self._labels.extend(_explode_labels(record, file_id))
        self._annotations.extend(_explode_annotations(record, file_id))
        self._extracted_metadata.extend(_explode_extracted_metadata(record, file_id))
        self._dlp_labels.extend(_explode_dlp_labels(record, file_id))

    def tables(self) -> Dict[str, List[Dict[str, Any]]]:
        """Return all five tables as a name → rows dict.

        Tables with zero rows are still included so callers always receive
        a consistent set of keys.
        """
        return {
            "files": list(self._files),
            "labels": list(self._labels),
            "annotations": list(self._annotations),
            "extracted_metadata": list(self._extracted_metadata),
            "dlp_labels": list(self._dlp_labels),
        }

    @property
    def file_count(self) -> int:
        return len(self._files)


# ------------------------------------------------------------------
# Row-projection helpers
# ------------------------------------------------------------------


def _project_file_row(record: Dict[str, Any], file_id: Any) -> Dict[str, Any]:
    """Extract scalar fields from a file record into a flat dict."""
    datasource = record.get("datasource") or {}
    owner = record.get("owner") or {}
    created_by = record.get("createdBy") or {}
    modified_by = record.get("modifiedBy") or {}

    return {
        "file_id": file_id,
        "file_name": record.get("fileName") or record.get("name"),
        "path": record.get("path"),
        "size_bytes": record.get("size"),
        "mime_type": record.get("mimeType"),
        "created_at": record.get("createdAt"),
        "last_modified_at": record.get("lastModifiedAt"),
        "content_sha256": record.get("contentSha256"),
        "scan_depth": record.get("scanDepth"),
        "datasource_id": datasource.get("id"),
        "datasource_name": datasource.get("name"),
        "datasource_connector_type_id": datasource.get("connectorTypeId"),
        "owner_email": _pick_person_id(owner),
        "created_by_email": _pick_person_id(created_by),
        "modified_by_email": _pick_person_id(modified_by),
        "label_count": len(record.get("labels") or []),
        "annotation_count": sum(
            len(a.get("annotations") or [])
            for a in (record.get("annotators") or [])
        ),
    }


def _explode_labels(record: Dict[str, Any], file_id: Any) -> List[Dict[str, Any]]:
    rows = []
    for label in record.get("labels") or []:
        if not isinstance(label, dict):
            continue
        rows.append(
            {
                "file_id": file_id,
                "label_id": label.get("id"),
                "label_name": label.get("name"),
                "label_hex_color": label.get("hexColor"),
                "label_type": label.get("type"),
                "label_description": label.get("description"),
            }
        )
    return rows


def _explode_annotations(record: Dict[str, Any], file_id: Any) -> List[Dict[str, Any]]:
    rows = []
    for annotator in record.get("annotators") or []:
        if not isinstance(annotator, dict):
            continue
        annotator_id = annotator.get("id")
        annotator_name = annotator.get("name") or annotator.get("entityType")
        for annotation in annotator.get("annotations") or []:
            if not isinstance(annotation, dict):
                continue
            rows.append(
                {
                    "file_id": file_id,
                    "annotator_id": annotator_id,
                    "annotator_name": annotator_name,
                    "value": annotation.get("value"),
                    "start": annotation.get("start"),
                    "end": annotation.get("end"),
                    "confidence": annotation.get("confidence"),
                }
            )
    return rows


def _explode_extracted_metadata(
    record: Dict[str, Any], file_id: Any
) -> List[Dict[str, Any]]:
    """Normalise extractedMetadata (dict of field → value) into rows."""
    rows = []
    extracted = record.get("extractedMetadata")
    if not extracted:
        return rows

    if isinstance(extracted, dict):
        items = extracted.items()
    elif isinstance(extracted, list):
        # Some API versions return a list of {name, value} objects
        items = ((e.get("name", ""), e.get("value")) for e in extracted if isinstance(e, dict))
    else:
        return rows

    for field_name, value in items:
        rows.append(
            {
                "file_id": file_id,
                "field_name": field_name,
                "value": str(value) if value is not None else None,
                "data_type": _infer_type(value),
            }
        )
    return rows


def _explode_dlp_labels(record: Dict[str, Any], file_id: Any) -> List[Dict[str, Any]]:
    rows = []
    for dlp in record.get("dlpLabels") or []:
        if not isinstance(dlp, dict):
            # Bare string label names
            rows.append({"file_id": file_id, "dlp_label": str(dlp), "score": None})
            continue
        rows.append(
            {
                "file_id": file_id,
                "dlp_label": dlp.get("name") or dlp.get("label"),
                "score": dlp.get("score") or dlp.get("confidence"),
                "category": dlp.get("category"),
            }
        )
    return rows


# ------------------------------------------------------------------
# Utility helpers
# ------------------------------------------------------------------


def _pick_person_id(person: Dict[str, Any]) -> Optional[str]:
    """Return the best available identifier from a person/owner object."""
    return (
        person.get("email")
        or person.get("userPrincipalName")
        or person.get("displayName")
        or person.get("id")
    )


def _infer_type(value: Any) -> str:
    if isinstance(value, bool):
        return "BOOLEAN"
    if isinstance(value, (int, float)):
        return "NUMBER"
    return "TEXT"
