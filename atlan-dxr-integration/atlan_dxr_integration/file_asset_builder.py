"""Helpers for transforming DXR file payloads into Atlan File assets."""

from __future__ import annotations

import logging
import math
import re
import unicodedata
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set
from urllib.parse import urljoin

from pyatlan.model.assets.core.file import File
from pyatlan.model.enums import FileType

LOGGER = logging.getLogger(__name__)


class FileAssetBuilder:
    """Accumulates DXR file payloads and converts them into Atlan File assets."""

    def __init__(
        self,
        *,
        connection_qualified_name: str,
        connection_name: str,
        dxr_base_url: str | None = None,
    ) -> None:
        self._connection_qualified_name = connection_qualified_name.rstrip("/")
        self._connection_name = connection_name
        self._dxr_base_url = _normalise_base_url(dxr_base_url)
        self._files: Dict[str, Dict[str, object]] = {}

    def consume(self, payload: Dict[str, object]) -> None:
        """Queue a DXR file payload for transformation."""

        identifier = _file_identifier(payload)
        if not identifier:
            LOGGER.debug("Ignoring DXR file payload without identifier: %s", payload)
            return
        if identifier not in self._files:
            self._files[identifier] = payload

    def build(self) -> List[File]:
        """Build Atlan File assets for all queued payloads."""

        assets: List[File] = []
        for identifier, payload in self._files.items():
            asset = self._to_file_asset(identifier, payload)
            assets.append(asset)
        return assets

    def _to_file_asset(self, identifier: str, payload: Dict[str, object]) -> File:
        name = _safe_str(
            payload.get("fileName")
            or payload.get("name")
            or payload.get("filename")
            or identifier
        ) or identifier

        asset = File.creator(
            name=name,
            connection_qualified_name=self._connection_qualified_name,
            file_type=_resolve_file_type(payload, name=name),
        )

        attrs = asset.attributes
        attrs.qualified_name = f"{self._connection_qualified_name}/{identifier}"
        attrs.connection_name = self._connection_name
        attrs.display_name = name

        description = _build_description(payload)
        if description:
            attrs.description = description
            attrs.user_description = description

        path = _safe_str(
            payload.get("filePath")
            or payload.get("path")
            or payload.get("filepath")
        )
        if path:
            attrs.file_path = path

        source_url = _derive_source_url(identifier, path, self._dxr_base_url)
        if source_url:
            attrs.source_url = source_url

        owner = _extract_owner(payload)
        if owner:
            attrs.owner_users = {owner}

        tags = _collect_tags(payload)
        if tags:
            existing = set(attrs.asset_tags or [])
            attrs.asset_tags = existing.union(tags)

        return asset


def _file_identifier(payload: Dict[str, object]) -> Optional[str]:
    for key in ("fileId", "id", "contentSha256", "path", "filePath"):
        value = payload.get(key)
        text = _safe_str(value)
        if text:
            return _normalise_identifier(text)
    return None


def _normalise_identifier(value: str) -> str:
    return value.replace(" ", "_")


def _resolve_file_type(payload: Dict[str, object], *, name: str) -> FileType:
    mime = _safe_str(payload.get("mimeType"))
    if mime:
        file_type = _MIME_TYPE_MAP.get(mime.lower())
        if file_type:
            return file_type

    extension = Path(name).suffix.lower()
    if not extension:
        path = _safe_str(payload.get("filePath") or payload.get("path") or payload.get("filepath"))
        if path:
            extension = Path(path).suffix.lower()

    if extension:
        file_type = _EXTENSION_MAP.get(extension)
        if file_type:
            return file_type

    LOGGER.debug(
        "Falling back to TXT file type for payload with unknown extension/mime: %s",
        payload,
    )
    return FileType.TXT


_MIME_TYPE_MAP: Dict[str, FileType] = {
    "application/pdf": FileType.PDF,
    "application/msword": FileType.DOC,
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": FileType.DOC,
    "application/vnd.ms-excel": FileType.XLS,
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": FileType.XLS,
    "application/vnd.ms-powerpoint": FileType.PPT,
    "application/vnd.openxmlformats-officedocument.presentationml.presentation": FileType.PPT,
    "text/csv": FileType.CSV,
    "text/plain": FileType.TXT,
    "application/json": FileType.JSON,
    "application/xml": FileType.XML,
    "application/zip": FileType.ZIP,
}


_EXTENSION_MAP: Dict[str, FileType] = {
    ".pdf": FileType.PDF,
    ".doc": FileType.DOC,
    ".docx": FileType.DOC,
    ".xls": FileType.XLS,
    ".xlsx": FileType.XLS,
    ".xlsm": FileType.XLSM,
    ".ppt": FileType.PPT,
    ".pptx": FileType.PPT,
    ".csv": FileType.CSV,
    ".txt": FileType.TXT,
    ".json": FileType.JSON,
    ".xml": FileType.XML,
    ".zip": FileType.ZIP,
    ".yxdb": FileType.YXDB,
    ".hyper": FileType.HYPER,
}


def _collect_tags(payload: Dict[str, object]) -> Set[str]:
    tags: Set[str] = set()

    for label in _extract_label_names(payload):
        tags.add(_format_tag("dxr-label", label))

    tags.update(_extract_dlp_labels(payload))
    tags.update(_extract_annotator_tags(payload))
    tags.update(_extract_entitlement_tags(payload))
    tags.update(_extract_metadata_tags(payload))
    tags.update(_extract_category_tags(payload))

    return {tag for tag in tags if tag}


def _build_description(payload: Dict[str, object]) -> Optional[str]:
    lines: List[str] = []

    datasource = payload.get("datasource")
    if isinstance(datasource, dict):
        datasource_name = _safe_str(datasource.get("name"))
        datasource_type = _safe_str(
            (datasource.get("connector") or {}).get("type")
            if isinstance(datasource.get("connector"), dict)
            else None
        )
        if datasource_name:
            if datasource_type:
                lines.append(f"Data source: {datasource_name} ({datasource_type})")
            else:
                lines.append(f"Data source: {datasource_name}")

    path = _safe_str(
        payload.get("filePath")
        or payload.get("path")
        or payload.get("filepath")
    )
    if path:
        lines.append(f"Path: {path}")

    size = payload.get("size")
    if isinstance(size, int) and size >= 0:
        lines.append(f"Size: {_format_bytes(size)}")

    labels = _extract_label_names(payload)
    if labels:
        lines.append("Labels: " + ", ".join(labels))

    scan_depth = _safe_str(payload.get("scanDepth"))
    if scan_depth:
        lines.append(f"Scan depth: {scan_depth}")

    return "\n".join(lines) if lines else None


def _extract_label_names(payload: Dict[str, object]) -> List[str]:
    raw = payload.get("labels")
    names: List[str] = []
    if isinstance(raw, list):
        for item in raw:
            if isinstance(item, dict):
                name = _safe_str(item.get("name"))
                if name:
                    names.append(name)
    return names


def _extract_dlp_labels(payload: Dict[str, object]) -> Set[str]:
    tags: Set[str] = set()
    raw = payload.get("dlpLabels")
    if isinstance(raw, list):
        for item in raw:
            if not isinstance(item, dict):
                continue
            name = _safe_str(item.get("name"))
            system = _safe_str(item.get("dlpSystem"))
            if name:
                value = f"{system}:{name}" if system else name
                tags.add(_format_tag("dlp", value))
    return tags


def _extract_annotator_tags(payload: Dict[str, object]) -> Set[str]:
    tags: Set[str] = set()
    raw = payload.get("annotators")
    if isinstance(raw, list):
        for item in raw:
            if not isinstance(item, dict):
                continue
            name = _safe_str(item.get("name"))
            if name:
                tags.add(_format_tag("annotator", name))
            domain = item.get("domain")
            domain_name = _safe_str(domain.get("name")) if isinstance(domain, dict) else None
            if domain_name:
                tags.add(_format_tag("annotator-domain", domain_name))
    return tags


def _extract_entitlement_tags(payload: Dict[str, object]) -> Set[str]:
    tags: Set[str] = set()
    entitlements = payload.get("entitlements")
    if not isinstance(entitlements, dict):
        return tags
    principals = entitlements.get("whoCanAccess")
    if not isinstance(principals, list):
        return tags
    for principal in principals:
        if not isinstance(principal, dict):
            continue
        account_type = _safe_str(principal.get("accountType")) or "unknown"
        key = _safe_str(principal.get("email") or principal.get("name"))
        if key:
            tags.add(_format_tag("entitlement", f"{account_type}:{key}"))
    return tags


def _extract_metadata_tags(payload: Dict[str, object]) -> Set[str]:
    tags: Set[str] = set()
    raw = payload.get("extractedMetadata")
    if not isinstance(raw, list):
        return tags
    for item in raw:
        if not isinstance(item, dict):
            continue
        name = _safe_str(item.get("name"))
        value = _safe_str(item.get("value"))
        if name and value:
            tags.add(_format_tag("metadata", f"{name}={value}"))
    return tags


def _extract_category_tags(payload: Dict[str, object]) -> Set[str]:
    tags: Set[str] = set()
    raw = payload.get("categories")
    if isinstance(raw, list):
        for item in raw:
            if isinstance(item, dict):
                name = _safe_str(item.get("name"))
            else:
                name = _safe_str(item)
            if name:
                tags.add(_format_tag("category", name))
    elif isinstance(raw, dict):
        name = _safe_str(raw.get("name"))
        if name:
            tags.add(_format_tag("category", name))
    return tags


def _derive_source_url(
    identifier: str,
    path: Optional[str],
    dxr_base_url: Optional[str],
) -> Optional[str]:
    if path and _looks_like_url(path):
        return path
    if dxr_base_url and identifier:
        return urljoin(dxr_base_url + "/", f"files/{identifier}")
    return None


def _looks_like_url(path: str) -> bool:
    lowered = path.lower()
    return lowered.startswith(("http://", "https://", "s3://", "gs://", "box://"))


def _extract_owner(payload: Dict[str, object]) -> Optional[str]:
    owner = payload.get("owner")
    if isinstance(owner, dict):
        email = _safe_str(owner.get("email"))
        if email:
            return email
        name = _safe_str(owner.get("name"))
        if name:
            return name
    return None


def _format_bytes(size: int) -> str:
    if size < 1024:
        return f"{size} B"
    units = ["KB", "MB", "GB", "TB"]
    value = float(size)
    unit_index = min(int(math.log(value, 1024)), len(units))
    scaled = value / (1024 ** unit_index)
    suffix = units[unit_index - 1] if unit_index > 0 else "KB"
    return f"{scaled:.1f} {suffix}"


def _normalise_base_url(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    cleaned = value.rstrip("/")
    if cleaned.endswith("/api"):
        cleaned = cleaned[: -len("/api")]
    return cleaned


def _safe_str(value: object) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _format_tag(namespace: str, value: str) -> str:
    return f"{namespace}:{_slugify(value)}"


def _slugify(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
    lowered = ascii_text.lower()
    cleaned = re.sub(r"[^a-z0-9]+", "-", lowered).strip("-")
    return cleaned or "unknown"


__all__ = ["FileAssetBuilder"]
