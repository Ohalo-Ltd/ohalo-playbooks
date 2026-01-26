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
    extracted_metadata_json: Optional[str]
    extracted_metadata_map: Dict[str, str]
    # Priority 1 fields
    scan_depth: Optional[str]
    content_sha256: Optional[str]
    created_at: Optional[str]
    labels: List[str]
    dlp_labels: List[str]
    owner_name: Optional[str]
    owner_email: Optional[str]
    owner_id: Optional[str]
    created_by_name: Optional[str]
    created_by_email: Optional[str]
    modified_by_name: Optional[str]
    modified_by_email: Optional[str]
    # Priority 2 fields
    datasource_name: Optional[str]
    connector_type: Optional[str]
    connector_site_url: Optional[str]
    annotators_json: Optional[str]
    annotators_summary: Dict[str, int]
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
            "extracted_metadata_json": self.extracted_metadata_json,
            "extracted_metadata_map": self.extracted_metadata_map,
            "scan_depth": self.scan_depth,
            "content_sha256": self.content_sha256,
            "created_at": self.created_at,
            "labels": self.labels,
            "dlp_labels": self.dlp_labels,
            "owner_name": self.owner_name,
            "owner_email": self.owner_email,
            "owner_id": self.owner_id,
            "created_by_name": self.created_by_name,
            "created_by_email": self.created_by_email,
            "modified_by_name": self.modified_by_name,
            "modified_by_email": self.modified_by_email,
            "datasource_name": self.datasource_name,
            "connector_type": self.connector_type,
            "connector_site_url": self.connector_site_url,
            "annotators_json": self.annotators_json,
            "annotators_summary": self.annotators_summary,
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

        annotation_stats_json = _extract_annotation_stats(source)

        # Extract metadata fields directly from _source (indexed-files/search format)
        # These appear as extracted_metadata#1, extracted_metadata#2, etc.
        # Map to human-readable names if metadata definitions are provided
        extracted_metadata_json, extracted_metadata_map = _extract_metadata_fields_from_source(source, metadata_defs)

        # Extract owner, creator, modifier info
        owner_name, owner_email, owner_id = _extract_user_info(source.get("owner"))
        created_by_name, created_by_email, _ = _extract_user_info(source.get("createdBy"))
        modified_by_name, modified_by_email, _ = _extract_user_info(source.get("modifiedBy"))

        # Extract annotators info
        annotators = source.get("annotators") or []
        annotators_json, annotators_summary = _extract_annotators_info(annotators)

        # Extract datasource and connector info
        datasource = source.get("datasource") or {}
        datasource_name = _maybe_str(datasource.get("name")) if isinstance(datasource, dict) else None
        connector = datasource.get("connector") if isinstance(datasource, dict) else {}
        connector_type = _maybe_str(connector.get("type")) if isinstance(connector, dict) else None
        connector_site_url = _maybe_str(connector.get("siteUrl")) if isinstance(connector, dict) else None

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
                extracted_metadata_json=extracted_metadata_json,
                extracted_metadata_map=extracted_metadata_map,
                scan_depth=_maybe_str(source.get("scanDepth")),
                content_sha256=_maybe_str(source.get("contentSha256")),
                created_at=_maybe_str(source.get("createdAt")),
                labels=_coerce_str_list(source.get("labels")),
                dlp_labels=_coerce_str_list(source.get("dlpLabels")),
                owner_name=owner_name,
                owner_email=owner_email,
                owner_id=owner_id,
                created_by_name=created_by_name,
                created_by_email=created_by_email,
                modified_by_name=modified_by_name,
                modified_by_email=modified_by_email,
                datasource_name=datasource_name,
                connector_type=connector_type,
                connector_site_url=connector_site_url,
                annotators_json=annotators_json,
                annotators_summary=annotators_summary,
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
        if isinstance(key, str) and (key.startswith("annotation_stats#") or key.startswith("annotation."))
    }
    if not payload:
        return None
    return json.dumps(payload, separators=(",", ":"))


def _extract_metadata_fields(extracted_metadata: List[Dict[str, Any]]) -> tuple[Optional[str], Dict[str, str]]:
    """
    Parse extractedMetadata array into JSON string and a map.

    Returns:
        Tuple of (json_string, map_dict) where:
        - json_string is the full extractedMetadata array as JSON
        - map_dict is a dictionary mapping extractor names to their values
    """
    if not extracted_metadata or not isinstance(extracted_metadata, list):
        return None, {}

    # Store full JSON for complete fidelity
    json_string = json.dumps(extracted_metadata, separators=(",", ":"))

    # Build map of extractor name -> value
    metadata_map = {}
    for item in extracted_metadata:
        if not isinstance(item, dict):
            continue
        name = item.get("name")
        value = item.get("value")
        if name and value is not None:
            # Convert value to string and truncate if too long to avoid issues
            str_value = str(value)
            if len(str_value) > 10000:  # Reasonable limit for map values
                str_value = str_value[:10000] + "...[truncated]"
            metadata_map[name] = str_value

    return json_string, metadata_map


def _extract_metadata_fields_from_source(
    source: Dict[str, Any],
    metadata_defs: Optional[Dict[str, str]] = None
) -> tuple[Optional[str], Dict[str, str]]:
    """
    Extract metadata fields directly from indexed-files/search _source object.

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


def _extract_user_info(user_obj: Optional[Dict[str, Any]]) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Extract name, email, and id from a user object (owner, createdBy, modifiedBy).

    Returns:
        Tuple of (name, email, id)
    """
    if not user_obj or not isinstance(user_obj, dict):
        return None, None, None

    name = _maybe_str(user_obj.get("name"))
    email = _maybe_str(user_obj.get("email"))
    user_id = _maybe_str(user_obj.get("id"))

    return name, email, user_id


def _extract_annotators_info(annotators: List[Dict[str, Any]]) -> tuple[Optional[str], Dict[str, int]]:
    """
    Parse annotators array into JSON string and a summary map.

    Returns:
        Tuple of (json_string, summary_dict) where:
        - json_string is the full annotators array as JSON
        - summary_dict is a map of annotator_name -> unique_phrase_count
    """
    if not annotators or not isinstance(annotators, list):
        return None, {}

    # Store full JSON for complete fidelity
    json_string = json.dumps(annotators, separators=(",", ":"))

    # Build summary map of annotator name -> unique phrase count
    summary = {}
    for annotator in annotators:
        if not isinstance(annotator, dict):
            continue
        name = annotator.get("name")
        unique_phrases = annotator.get("uniquePhrases", 0)
        if name:
            # Convert to int safely
            try:
                count = int(unique_phrases) if unique_phrases is not None else 0
                summary[name] = count
            except (ValueError, TypeError):
                summary[name] = 0

    return json_string, summary
