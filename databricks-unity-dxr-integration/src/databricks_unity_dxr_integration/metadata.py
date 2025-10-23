from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional


@dataclass(frozen=True)
class MetadataRecord:
    """Structured representation of a Data X-Ray metadata hit."""

    file_id: str
    volume_path: Optional[str]
    file_path: Optional[str]
    datasource_id: Optional[int]
    datasource_scan_id: Optional[int]
    dxr_labels: List[str]
    dxr_tags: List[str]
    dxr_categories: List[str]
    metadata_json: Dict[str, Any]


def extract_metadata_records(job_id: str, hits: Iterable[Dict[str, Any]]) -> List[MetadataRecord]:
    """Map DXR JSON search hits into structured metadata records."""
    records: List[MetadataRecord] = []
    for index, hit in enumerate(hits):
        source = hit.get("_source", {}) if isinstance(hit, dict) else {}
        if not isinstance(source, dict):
            source = {}

        file_id = _get_first_non_empty(
            [
                source.get("dxr#file_id"),
                hit.get("_id") if isinstance(hit, dict) else None,
                source.get("ds#file_name"),
                f"{job_id}-{index}",
            ]
        )

        volume_path = source.get("dxr#volume_path") or source.get("ds#volume_path")
        file_path = source.get("ds#file_path") or source.get("ds#file_name")
        datasource_id = _maybe_int(source.get("dxr#datasource_id"))
        datasource_scan_id = _maybe_int(source.get("dxr#datasource_scan_id"))

        dxr_labels = _coerce_str_list(source.get("dxr#labels"))
        dxr_tags = _coerce_str_list(source.get("dxr#tags"))
        categories = []
        category_field = source.get("ai#category")
        if isinstance(category_field, list):
            categories = [str(item) for item in category_field]
        elif isinstance(category_field, str):
            categories = [category_field]

        records.append(
            MetadataRecord(
                file_id=str(file_id),
                volume_path=_coerce_optional_str(volume_path),
                file_path=_coerce_optional_str(file_path),
                datasource_id=datasource_id,
                datasource_scan_id=datasource_scan_id,
                dxr_labels=dxr_labels,
                dxr_tags=dxr_tags,
                dxr_categories=categories,
                metadata_json=source,
            )
        )
    return records


def _get_first_non_empty(candidates: List[Optional[str]]) -> str:
    for candidate in candidates:
        if candidate:
            return str(candidate)
    return ""


def _maybe_int(value: Any) -> Optional[int]:
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isdigit():
        return int(value)
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _coerce_str_list(value: Any) -> List[str]:
    if isinstance(value, list):
        return [str(item) for item in value]
    if isinstance(value, str):
        return [value]
    return []


def _coerce_optional_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value)
    return text if text else None
