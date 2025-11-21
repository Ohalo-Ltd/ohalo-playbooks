from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

from .config import VolumeConfig
from .volume import VolumeFile


@dataclass(frozen=True)
class MetadataRecord:
    file_path: str
    relative_path: str
    catalog_name: str
    schema_name: str
    volume_name: str
    file_size: int
    modification_time: int
    datasource_id: str
    datasource_scan_id: Optional[int]
    job_id: str
    file_name: Optional[str]
    object_id: Optional[str]
    parent_paths: List[str]
    mime_type: Optional[str]
    indexed_at: Optional[str]
    sha256: Optional[str]
    sha256_file_meta: Optional[str]
    doc_language: Optional[str]
    composite_type: Optional[str]
    is_processed: Optional[bool]
    document_status: Optional[str]
    text_extraction_status: Optional[str]
    metadata_extraction_status: Optional[str]
    dxr_tags: List[str]
    removed_tags: List[str]
    ocr_used: Optional[bool]
    categories: List[str]
    annotations: Optional[str]
    folder_id: Optional[str]
    modified_at: Optional[str]
    binary_hash: Optional[str]
    annotation_stats_json: Optional[str]
    raw_metadata: Dict[str, Any]
    collected_at: datetime

    def as_row(self) -> Dict[str, Any]:
        return {
            "file_path": self.file_path,
            "relative_path": self.relative_path,
            "catalog_name": self.catalog_name,
            "schema_name": self.schema_name,
            "volume_name": self.volume_name,
            "file_size": self.file_size,
            "modification_time": self.modification_time,
            "datasource_id": self.datasource_id,
            "datasource_scan_id": self.datasource_scan_id,
            "job_id": self.job_id,
            "file_name": self.file_name,
            "object_id": self.object_id,
            "parent_paths": self.parent_paths,
            "mime_type": self.mime_type,
            "indexed_at": self.indexed_at,
            "sha256": self.sha256,
            "sha256_file_meta": self.sha256_file_meta,
            "doc_language": self.doc_language,
            "composite_type": self.composite_type,
            "is_processed": self.is_processed,
            "document_status": self.document_status,
            "text_extraction_status": self.text_extraction_status,
            "metadata_extraction_status": self.metadata_extraction_status,
            "dxr_tags": self.dxr_tags,
            "removed_tags": self.removed_tags,
            "ocr_used": self.ocr_used,
            "categories": self.categories,
            "annotations": self.annotations,
            "folder_id": self.folder_id,
            "modified_at": self.modified_at,
            "binary_hash": self.binary_hash,
            "annotation_stats_json": self.annotation_stats_json,
            "raw_metadata": json.dumps(self.raw_metadata, separators=(",", ":")),
            "collected_at": self.collected_at,
        }


def build_metadata_records(
    volume_config: VolumeConfig,
    job_id: str,
    datasource_id: str,
    hits: Iterable[Dict[str, Any]],
    known_files: Dict[str, VolumeFile],
) -> List[MetadataRecord]:
    """Match DXR metadata hits back to the originating Unity Catalog files."""
    records: List[MetadataRecord] = []
    for index, hit in enumerate(hits):
        source = hit.get("_source", {}) if isinstance(hit, dict) else {}
        if not isinstance(source, dict):
            source = {}

        file_key = _first_value(
            [
                source.get("ds#file_path"),
                source.get("ds#file_name"),
                source.get("dxr#file_id"),
                hit.get("_id") if isinstance(hit, dict) else None,
            ]
        )
        file = known_files.get(file_key)
        if file is None:
            # Try to locate the file using only the basename.
            basename = file_key.split("/")[-1]
            file = _match_by_basename(basename, known_files.values())
        if file is None:
            continue

        annotation_stats_json = _extract_annotation_stats(source)

        records.append(
            MetadataRecord(
                file_path=file.absolute_path,
                relative_path=file.relative_path,
                catalog_name=volume_config.catalog,
                schema_name=volume_config.schema,
                volume_name=volume_config.volume,
                file_size=file.size_bytes,
                modification_time=file.modification_time,
                datasource_id=datasource_id,
                datasource_scan_id=_maybe_int(source.get("dxr#datasource_scan_id")),
                job_id=job_id,
                file_name=_maybe_str(source.get("ds#file_name")),
                object_id=_maybe_str(source.get("object_id") or source.get("dxr#file_id")),
                parent_paths=_coerce_str_list(source.get("ds#parent_folder_paths")),
                mime_type=_maybe_str(source.get("dxr#mime_type")),
                indexed_at=_maybe_str(source.get("dxr#indexed_date")),
                sha256=_maybe_str(source.get("dxr#sha_256_hash")),
                sha256_file_meta=_maybe_str(source.get("dxr#sha_256_hash_file_meta")),
                doc_language=_maybe_str(source.get("dxr#doc_lang")),
                composite_type=_maybe_str(source.get("dxr#composite_type")),
                is_processed=_maybe_bool(source.get("dxr#is_processed")),
                document_status=_maybe_str(source.get("dxr#document_status")),
                text_extraction_status=_maybe_str(source.get("dxr#text_extraction_status")),
                metadata_extraction_status=_maybe_str(source.get("dxr#metadata_extraction_status")),
                dxr_tags=_coerce_str_list(source.get("dxr#tags")),
                removed_tags=_coerce_str_list(source.get("dxr#manually_removed_tags")),
                ocr_used=_maybe_bool(source.get("dxr#ocr_used")),
                categories=_coerce_str_list(source.get("ai#category")),
                annotations=_maybe_str(source.get("annotations")),
                folder_id=_maybe_str(source.get("folder_id")),
                modified_at=_maybe_str(source.get("metadata#MODIFIED_DATE")),
                binary_hash=_maybe_str(source.get("metadata#binary_hash")),
                annotation_stats_json=annotation_stats_json,
                raw_metadata=source,
                collected_at=datetime.now(timezone.utc),
            )
        )
    return records


def _first_value(candidates: List[Optional[str]]) -> str:
    for value in candidates:
        if value:
            return str(value)
    return ""


def _match_by_basename(basename: str, files: Iterable[VolumeFile]) -> Optional[VolumeFile]:
    if not basename:
        return None
    for file in files:
        if file.relative_path.split("/")[-1] == basename:
            return file
    return None


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


def _maybe_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        return stripped or None
    return str(value)


def _maybe_bool(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes"}:
            return True
        if normalized in {"false", "0", "no"}:
            return False
    return None


def _extract_annotation_stats(source: Dict[str, Any]) -> Optional[str]:
    payload = {
        key: value
        for key, value in source.items()
        if isinstance(key, str) and (key.startswith("annotation_stats#") or key.startswith("annotation."))
    }
    if not payload:
        return None
    return json.dumps(payload, separators=(",", ":"))
