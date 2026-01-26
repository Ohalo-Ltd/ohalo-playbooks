from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

from .config import VolumeConfig
from .volume import VolumeFile


@dataclass(frozen=True)
class MetadataRecord:
    # Core Unity Catalog fields
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

    # File identification fields (ds#* prefix)
    file_name: Optional[str]
    object_id: Optional[str]
    parent_paths: List[str]
    folder_id: Optional[str]

    # DXR processing fields (dxr#* prefix)
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

    # AI-generated fields (ai#* prefix)
    ai_category: Optional[str]
    ai_category_last_updated: Optional[str]

    # Annotation fields (annotation.* and annotation_stats#* prefixes)
    annotation_stats_json: Optional[str]
    annotations_json: Optional[str]

    # Metadata fields (metadata#* and computed.metadata#* prefixes) - stored as JSON for flexibility
    metadata_fields_json: Optional[str]
    metadata_fields_map: Dict[str, str]

    # Extracted metadata fields (extracted_metadata#* prefix) - mapped to display names if available
    extracted_metadata_json: Optional[str]
    extracted_metadata_map: Dict[str, str]

    # Raw metadata for complete fidelity
    raw_metadata: Dict[str, Any]

    # Timestamp
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
            "folder_id": self.folder_id,
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
            "ai_category": self.ai_category,
            "ai_category_last_updated": self.ai_category_last_updated,
            "annotation_stats_json": self.annotation_stats_json,
            "annotations_json": self.annotations_json,
            "metadata_fields_json": self.metadata_fields_json,
            "metadata_fields_map": self.metadata_fields_map,
            "extracted_metadata_json": self.extracted_metadata_json,
            "extracted_metadata_map": self.extracted_metadata_map,
            "raw_metadata": json.dumps(self.raw_metadata, separators=(",", ":")),
            "collected_at": self.collected_at,
        }


def build_metadata_records(
    volume_config: VolumeConfig,
    job_id: str,
    datasource_id: str,
    hits: Iterable[Dict[str, Any]],
    known_files: Dict[str, VolumeFile],
    metadata_defs: Optional[Dict[str, str]] = None,
) -> List[MetadataRecord]:
    """Match DXR metadata hits back to the originating Unity Catalog files.

    Args:
        volume_config: Unity Catalog volume configuration
        job_id: ODC job ID
        datasource_id: Data X-Ray datasource ID
        hits: Search results from indexed-files/search
        known_files: Dictionary mapping file keys to VolumeFile objects
        metadata_defs: Optional mapping from field IDs to display names for extracted metadata
    """
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

        # Extract annotation fields
        annotation_stats_json = _extract_annotation_stats(source)
        annotations_json = _extract_annotations(source)

        # Extract metadata# and computed.metadata# fields
        metadata_fields_json, metadata_fields_map = _extract_metadata_fields(source)

        # Extract extracted_metadata# fields and map to display names if available
        extracted_metadata_json, extracted_metadata_map = _extract_extracted_metadata_fields(source, metadata_defs)

        # Extract AI category fields
        ai_category = _maybe_str(source.get("ai#category"))
        ai_category_last_updated = _maybe_str(source.get("ai#category_last_updated"))

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
                folder_id=_maybe_str(source.get("folder_id")),
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
                ai_category=ai_category,
                ai_category_last_updated=ai_category_last_updated,
                annotation_stats_json=annotation_stats_json,
                annotations_json=annotations_json,
                metadata_fields_json=metadata_fields_json,
                metadata_fields_map=metadata_fields_map,
                extracted_metadata_json=extracted_metadata_json,
                extracted_metadata_map=extracted_metadata_map,
                raw_metadata=source,
                collected_at=datetime.now(timezone.utc),
            )
        )
    return records


def _first_value(candidates: list[str | None]) -> str:
    return next((v for v in candidates if v), "")


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
        if isinstance(key, str) and key.startswith("annotation_stats#")
    }
    if not payload:
        return None
    return json.dumps(payload, separators=(",", ":"))


def _extract_annotations(source: Dict[str, Any]) -> Optional[str]:
    payload = {
        key: value
        for key, value in source.items()
        if isinstance(key, str) and key.startswith("annotation.")
    }
    if not payload:
        return None
    return json.dumps(payload, separators=(",", ":"))




def _extract_metadata_fields(source: Dict[str, Any]) -> tuple[Optional[str], Dict[str, str]]:
    """
    Extract metadata# and computed.metadata# fields from indexed-files/search _source object.

    These fields come from the data source connectors and contain file system or application metadata:
    - metadata#CREATED_BY: User ID who created the file
    - metadata#MODIFIED_DATE: When the file was last modified
    - computed.metadata#OWNER: Computed owner information
    - etc.

    Args:
        source: The _source object from indexed-files/search response

    Returns:
        Tuple of (json_string, map_dict) where:
        - json_string is a JSON representation of all metadata fields (with original types preserved)
        - map_dict is a dictionary of field name -> string value (for Spark compatibility)
    """
    if not isinstance(source, dict):
        return None, {}

    # Extract all metadata# and computed.metadata# fields
    metadata_fields_original = {}
    metadata_fields_str = {}

    for key, value in source.items():
        if isinstance(key, str) and (key.startswith("metadata#") or key.startswith("computed.metadata#")):
            if value is not None:
                # Store original value for JSON (preserves types)
                metadata_fields_original[key] = value
                # Convert to string for map (Spark requires string values)
                metadata_fields_str[key] = str(value)

    if not metadata_fields_original:
        return None, {}

    # Store as JSON with original types for complete fidelity
    json_string = json.dumps(metadata_fields_original, separators=(",", ":"), sort_keys=True)

    return json_string, metadata_fields_str


def _extract_extracted_metadata_fields(
    source: Dict[str, Any],
    metadata_defs: Optional[Dict[str, str]] = None
) -> tuple[Optional[str], Dict[str, str]]:
    """
    Extract extracted_metadata# fields from indexed-files/search _source object.

    The indexed-files/search endpoint returns extracted metadata as numbered fields:
    - extracted_metadata#1: "value1"
    - extracted_metadata#2: "value2"
    - etc.

    Args:
        source: The _source object from indexed-files/search response
        metadata_defs: Optional mapping from field IDs to display names
                      (e.g., {"1": "Service Bulletin (SB) Number Identification"})

    Returns:
        Tuple of (json_string, map_dict) where:
        - json_string is a JSON representation of the extracted metadata
        - map_dict is a dictionary mapping display names (if available) to their values
    """
    if not isinstance(source, dict):
        return None, {}

    # Extract all extracted_metadata# fields
    metadata_fields = {}
    for key, value in source.items():
        if isinstance(key, str) and key.startswith("extracted_metadata#"):
            if value is not None:
                # Convert value to string and truncate if too long
                str_value = str(value)
                if len(str_value) > 10000:
                    str_value = str_value[:10000] + "...[truncated]"

                # Try to map to display name if metadata definitions provided
                field_id = key.replace("extracted_metadata#", "")
                if metadata_defs and field_id in metadata_defs:
                    display_name = metadata_defs[field_id]
                    metadata_fields[display_name] = str_value
                else:
                    # Fall back to raw field key if no mapping available
                    metadata_fields[key] = str_value

    if not metadata_fields:
        return None, {}

    # Store as JSON for complete fidelity
    json_string = json.dumps(metadata_fields, separators=(",", ":"), sort_keys=True)

    return json_string, metadata_fields


