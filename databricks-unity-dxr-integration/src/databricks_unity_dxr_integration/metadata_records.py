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
    labels: List[str]
    tags: List[str]
    categories: List[str]
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
            "labels": self.labels,
            "tags": self.tags,
            "categories": self.categories,
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
                labels=_coerce_str_list(source.get("dxr#labels")),
                tags=_coerce_str_list(source.get("dxr#tags")),
                categories=_coerce_str_list(source.get("ai#category")),
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
