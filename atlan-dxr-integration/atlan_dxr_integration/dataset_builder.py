"""Transformation utilities for turning DXR metadata into Atlan-ready records."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional
from urllib.parse import urljoin

from .dxr_client import Classification

LOGGER = logging.getLogger(__name__)


@dataclass
class SampleFile:
    """Simplified representation of a DXR file used in dataset summaries."""

    identifier: Optional[str]
    name: Optional[str]
    path: Optional[str]
    link: Optional[str]

    def render(self) -> str:
        parts: List[str] = []
        if self.name:
            parts.append(self.name)
        elif self.identifier:
            parts.append(self.identifier)
        if self.path and self.path not in parts:
            parts.append(self.path)
        return " – ".join(parts) if parts else (self.identifier or "Unknown file")


@dataclass
class DatasetRecord:
    """Aggregated dataset representation ready for ingestion into Atlan."""

    classification: Classification
    file_count: int
    sample_files: List[SampleFile] = field(default_factory=list)
    description: Optional[str] = None
    source_url: Optional[str] = None

    @property
    def identifier(self) -> str:
        return self.classification.identifier

    @property
    def name(self) -> str:
        return self.classification.name

    @property
    def classification_type(self) -> Optional[str]:
        return self.classification.type

    @property
    def classification_subtype(self) -> Optional[str]:
        return self.classification.subtype


class DatasetBuilder:
    """Build dataset records from DXR classifications and files."""

    def __init__(
        self,
        classifications: Iterable[Classification],
        *,
        allowed_types: Optional[Iterable[str]] = None,
        sample_file_limit: int = 5,
        dxr_base_url: Optional[str] = None,
        file_fetch_limit: Optional[int] = None,
    ) -> None:
        self._records: Dict[str, DatasetRecord] = {}
        self._allowed_types = {t.upper() for t in allowed_types} if allowed_types else None
        self._sample_file_limit = sample_file_limit
        self._dxr_base_url = _normalise_base_url(dxr_base_url) if dxr_base_url else None
        self._file_fetch_limit = (
            file_fetch_limit if file_fetch_limit and file_fetch_limit > 0 else None
        )

        for classification in classifications:
            if self._allowed_types and (
                (classification.type or "").upper() not in self._allowed_types
            ):
                LOGGER.debug(
                    "Skipping classification %s due to filtered type %s",
                    classification.identifier,
                    classification.type,
                )
                continue
            self._records[classification.identifier] = DatasetRecord(
                classification=classification,
                file_count=0,
                source_url=self._build_source_url(classification),
            )

    def consume_file(self, payload: Dict[str, object]) -> None:
        """Add a DXR file payload into the aggregation."""

        labels = _extract_labels(payload)
        if not labels:
            return

        for label in labels:
            record = self._records.get(label)
            if not record:
                continue
            if self._file_fetch_limit and record.file_count >= self._file_fetch_limit:
                continue
            record.file_count += 1
            if len(record.sample_files) < self._sample_file_limit:
                record.sample_files.append(
                    SampleFile(
                        identifier=_safe_str(
                            payload.get("id")
                            or payload.get("fileId")
                            or payload.get("file_id")
                        ),
                        name=_safe_str(
                            payload.get("name")
                            or payload.get("filename")
                            or payload.get("fileName")
                        ),
                        path=_safe_str(
                            payload.get("path")
                            or payload.get("filepath")
                            or payload.get("filePath")
                        ),
                        link=_safe_str(payload.get("link")),
                    )
                )

    def build(self) -> List[DatasetRecord]:
        """Generate final dataset records with descriptions."""

        for record in self._records.values():
            record.description = self._build_description(record)
        return list(self._records.values())

    def _build_source_url(self, classification: Classification) -> Optional[str]:
        path = classification.search_link or classification.link
        if not path or not self._dxr_base_url:
            return path
        return urljoin(self._dxr_base_url + "/", path.lstrip("/"))

    def _build_description(self, record: DatasetRecord) -> Optional[str]:
        description_parts: List[str] = []
        if record.classification.description:
            description_parts.append(record.classification.description)
        description_parts.append(f"DXR dataset contains {record.file_count} file(s).")
        if record.sample_files:
            bullet_list = "\n".join(f"- {sample.render()}" for sample in record.sample_files)
            description_parts.append("Sample files:\n" + bullet_list)
        return "\n\n".join(description_parts)


def _extract_labels(payload: Dict[str, object]) -> List[str]:
    raw = payload.get("labels") or payload.get("classifications")
    if not raw:
        return []
    if isinstance(raw, list):
        identifiers = []
        for item in raw:
            if isinstance(item, dict):
                identifier = item.get("id") or item.get("classificationId")
                if identifier:
                    identifiers.append(str(identifier))
            elif isinstance(item, str):
                identifiers.append(item)
        return identifiers
    if isinstance(raw, dict):
        identifiers = []
        for key in ("id", "classificationId"):
            if key in raw:
                identifiers.append(str(raw[key]))
        return identifiers
    return []


def _safe_str(value: object) -> Optional[str]:
    if value is None:
        return None
    text = str(value)
    return text if text else None


def _normalise_base_url(base_url: str) -> str:
    cleaned = base_url.rstrip("/")
    if cleaned.endswith("/api"):
        cleaned = cleaned[: -len("/api")]
    return cleaned


__all__ = ["DatasetBuilder", "DatasetRecord", "SampleFile"]
