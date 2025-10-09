"""Factory for constructing enriched Atlan File assets from DXR payloads."""

from __future__ import annotations

import logging
import math
import re
import unicodedata
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, Mapping, Optional, Sequence, Tuple
from urllib.parse import urljoin

from .atlan_types import TagColor
from .tag_registry import TagHandle, TagRegistry

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class BuiltFileAsset:
    """Result of building a file asset along with its tag handles."""

    asset: Dict[str, Any]
    tag_handles: Tuple[TagHandle, ...]


class FileAssetFactory:
    """Create Atlan File assets populated with DXR metadata and tags."""

    def __init__(
        self,
        *,
        tag_registry: TagRegistry,
        tag_namespace: str,
        dxr_base_url: Optional[str] = None,
    ) -> None:
        self._tag_registry = tag_registry
        self._tag_namespace = tag_namespace
        self._dxr_base_url = _normalise_base_url(dxr_base_url) if dxr_base_url else None

    def build(
        self,
        payload: Dict[str, object],
        *,
        connection_qualified_name: str,
        connection_name: str,
        classification_tags: Mapping[str, TagHandle],
    ) -> BuiltFileAsset:
        identifier = _file_identifier(payload)
        if not identifier:
            raise ValueError("DXR file payload missing unique identifier")

        name = _coalesce_str(
            payload.get("fileName"),
            payload.get("name"),
            payload.get("filename"),
            identifier,
        )

        qualified_name = f"{connection_qualified_name}/{identifier}"
        asset = {
            "typeName": "File",
            "attributes": {
                "qualifiedName": qualified_name,
                "name": name,
                "displayName": name,
                "connectionQualifiedName": connection_qualified_name,
                "connectionName": connection_name,
                "connectorName": None,
                "fileType": _resolve_file_type(payload, name=name),
            },
        }
        attrs = asset["attributes"]

        description = _build_description(payload)
        if description:
            attrs["description"] = description
            attrs["userDescription"] = description

        path = _coalesce_str(
            payload.get("filePath"),
            payload.get("path"),
            payload.get("filepath"),
        )
        if path:
            attrs["filePath"] = path

        owner = _extract_owner(payload)
        if owner:
            attrs["ownerUsers"] = sorted({owner})

        tags = self._build_tags(payload, classification_tags)
        handles = tuple(
            sorted(tags.handles.values(), key=lambda h: h.display_name)
        )
        if handles:
            attrs["assetTags"] = [handle.display_name for handle in handles]
            asset["classifications"] = [
                {
                    "typeName": handle.hashed_name,
                }
                for handle in handles
                if handle.hashed_name
            ]

        source_url = _derive_source_url(identifier, path, self._dxr_base_url)
        if source_url:
            attrs["sourceURL"] = source_url

        return BuiltFileAsset(asset=asset, tag_handles=handles)

    def _build_tags(
        self,
        payload: Dict[str, object],
        classification_tags: Mapping[str, TagHandle],
    ) -> "_TagAssignments":
        tags = _TagAssignments()

        def add_classification(identifier: Optional[str]) -> bool:
            if not identifier:
                return False
            handle = classification_tags.get(identifier)
            if handle:
                tags.register(handle)
                return True
            return False

        # Classifications / labels
        raw_labels = payload.get("labels") or payload.get("classifications")
        if isinstance(raw_labels, list):
            for item in raw_labels:
                if isinstance(item, dict):
                    identifier = _coalesce_str(
                        item.get("id"), item.get("classificationId")
                    )
                    if add_classification(identifier):
                        continue
                    name = _coalesce_str(item.get("name"))
                    if name:
                        tags.register(
                            self._tag_registry.ensure(
                                slug_parts=["label", name],
                                display_name=f"Label :: {name}",
                                color=TagColor.GRAY,
                            )
                        )
                elif isinstance(item, str):
                    add_classification(_coalesce_str(item))
        elif isinstance(raw_labels, dict):
            identifier = _coalesce_str(
                raw_labels.get("id"), raw_labels.get("classificationId")
            )
            add_classification(identifier)

        # Fallback for legacy helper
        for identifier in _extract_label_identifiers(payload):
            add_classification(identifier)

        # DLP labels
        for dlp in _extract_iterable_dict(payload.get("dlpLabels")):
            name = _coalesce_str(dlp.get("name"))
            system = _coalesce_str(dlp.get("dlpSystem"))
            if not name:
                continue
            display = f"DLP :: {system or 'Unspecified'} :: {name}"
            slug_parts = ["dlp", system or "unspecified", name]
            tags.register(
                self._tag_registry.ensure(
                    slug_parts=slug_parts,
                    display_name=display,
                    color=TagColor.YELLOW,
                )
            )

        # Annotators
        for annotator in _extract_iterable_dict(payload.get("annotators")):
            has_classification = add_classification(_coalesce_str(annotator.get("id")))
            name = _coalesce_str(annotator.get("name"))
            if name and not has_classification:
                tags.register(
                    self._tag_registry.ensure(
                        slug_parts=["annotator", name],
                        display_name=f"Annotator :: {name}",
                        color=TagColor.GREEN,
                    )
                )
            domain = annotator.get("domain")
            domain_id = None
            domain_name = None
            if isinstance(domain, dict):
                domain_id = _coalesce_str(domain.get("id"))
                domain_name = _coalesce_str(domain.get("name"))
            if not add_classification(domain_id) and domain_name:
                tags.register(
                    self._tag_registry.ensure(
                        slug_parts=["annotator-domain", domain_name],
                        display_name=f"Annotator Domain :: {domain_name}",
                        color=TagColor.GREEN,
                    )
                )

        # Entitlements
        entitlements = payload.get("entitlements")
        if isinstance(entitlements, dict):
            for principal in _extract_iterable_dict(entitlements.get("whoCanAccess")):
                account_type = (_coalesce_str(principal.get("accountType")) or "UNKNOWN").upper()
                identifier = _coalesce_str(principal.get("email"), principal.get("name"))
                if identifier:
                    tags.register(
                        self._tag_registry.ensure(
                            slug_parts=["entitlement", account_type, identifier],
                            display_name=f"Entitlement :: {account_type.title()} :: {identifier}",
                            color=TagColor.RED,
                        )
                    )

        # Extracted metadata
        for item in _extract_iterable_dict(payload.get("extractedMetadata")):
            if add_classification(_coalesce_str(item.get("id"))):
                # Still capture rich value context when available.
                name = _coalesce_str(item.get("name"))
                value = _coalesce_str(item.get("value"))
                if name and value:
                    tags.register(
                        self._tag_registry.ensure(
                            slug_parts=["metadata", name, value],
                            display_name=f"Metadata :: {name} = {value}",
                            color=TagColor.GRAY,
                        )
                    )
                continue

            name = _coalesce_str(item.get("name"))
            value = _coalesce_str(item.get("value"))
            if name and value:
                tags.register(
                    self._tag_registry.ensure(
                        slug_parts=["metadata", name, value],
                        display_name=f"Metadata :: {name} = {value}",
                        color=TagColor.GRAY,
                    )
                )

        # Categories
        raw_categories = payload.get("categories")
        for category in _normalize_to_list(raw_categories):
            if isinstance(category, dict):
                name = _coalesce_str(category.get("name"))
            else:
                name = _coalesce_str(category)
            if name:
                tags.register(
                    self._tag_registry.ensure(
                        slug_parts=["category", name],
                        display_name=f"Category :: {name}",
                        color=TagColor.GRAY,
                    )
                )

        return tags


@dataclass
class _TagAssignments:
    handles: Dict[str, TagHandle] = field(default_factory=dict)

    def register(self, handle: TagHandle) -> None:
        self.handles[handle.slug] = handle


def _extract_iterable_dict(value: object) -> Iterable[Dict[str, object]]:
    if isinstance(value, list):
        for item in value:
            if isinstance(item, dict):
                yield item
    elif isinstance(value, dict):
        yield value
    return []


def _normalize_to_list(value: object) -> Sequence[object]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def _file_identifier(payload: Dict[str, object]) -> Optional[str]:
    for key in ("fileId", "id", "contentSha256", "path", "filePath"):
        value = payload.get(key)
        text = _coalesce_str(value)
        if text:
            return _normalise_identifier(text)
    return None


def _resolve_file_type(payload: Dict[str, object], *, name: str) -> FileType:
    mime = _coalesce_str(payload.get("mimeType"))
    if mime:
        file_type = _MIME_TYPE_MAP.get(mime.lower())
        if file_type:
            return file_type

    extension = _extract_extension(name)
    if not extension:
        path = _coalesce_str(
            payload.get("filePath"), payload.get("path"), payload.get("filepath")
        )
        if path:
            extension = _extract_extension(path)

    if extension:
        file_type = _EXTENSION_MAP.get(extension)
        if file_type:
            return file_type

    LOGGER.debug(
        "Falling back to TXT file type for payload with unknown extension/mime: %s",
        payload,
    )
    return FileType.TXT


_MIME_TYPE_MAP: Dict[str, str] = {
    "application/pdf": "PDF",
    "application/msword": "DOC",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": "DOC",
    "application/vnd.ms-excel": "XLS",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "XLS",
    "application/vnd.ms-powerpoint": "PPT",
    "application/vnd.openxmlformats-officedocument.presentationml.presentation": "PPT",
    "text/csv": "CSV",
    "text/plain": "TXT",
    "application/json": "JSON",
    "application/xml": "XML",
    "application/zip": "ZIP",
}


_EXTENSION_MAP: Dict[str, str] = {
    ".pdf": "PDF",
    ".doc": "DOC",
    ".docx": "DOC",
    ".xls": "XLS",
    ".xlsx": "XLS",
    ".xlsm": "XLSM",
    ".ppt": "PPT",
    ".pptx": "PPT",
    ".csv": "CSV",
    ".txt": "TXT",
    ".json": "JSON",
    ".xml": "XML",
    ".zip": "ZIP",
    ".yxdb": "YXDB",
    ".hyper": "HYPER",
}


def _extract_extension(name: str) -> Optional[str]:
    if not name:
        return None
    parts = name.rsplit(".", 1)
    if len(parts) != 2:
        return None
    return f".{parts[1].lower()}"


def _build_description(payload: Dict[str, object]) -> Optional[str]:
    lines = []

    datasource = payload.get("datasource")
    if isinstance(datasource, dict):
        datasource_name = _coalesce_str(datasource.get("name"))
        datasource_type = _coalesce_str(
            (datasource.get("connector") or {}).get("type")
            if isinstance(datasource.get("connector"), dict)
            else None
        )
        if datasource_name:
            if datasource_type:
                lines.append(f"Data source: {datasource_name} ({datasource_type})")
            else:
                lines.append(f"Data source: {datasource_name}")

    path = _coalesce_str(
        payload.get("filePath"),
        payload.get("path"),
        payload.get("filepath"),
    )
    if path:
        lines.append(f"Path: {path}")

    size = payload.get("size")
    if isinstance(size, int) and size >= 0:
        lines.append(f"Size: {_format_bytes(size)}")

    scan_depth = _coalesce_str(payload.get("scanDepth"))
    if scan_depth:
        lines.append(f"Scan depth: {scan_depth}")

    return "\n".join(lines) if lines else None


def _extract_label_identifiers(payload: Dict[str, object]) -> Iterable[str]:
    raw = payload.get("labels") or payload.get("classifications")
    if isinstance(raw, list):
        for item in raw:
            if isinstance(item, dict):
                identifier = _coalesce_str(
                    item.get("id"),
                    item.get("classificationId"),
                )
                if identifier:
                    yield identifier
            elif isinstance(item, str):
                yield item
    elif isinstance(raw, dict):
        for key in ("id", "classificationId"):
            value = raw.get(key)
            if value:
                yield str(value)


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
        email = _coalesce_str(owner.get("email"))
        if email:
            return email
        name = _coalesce_str(owner.get("name"))
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


def _normalise_base_url(value: str) -> str:
    cleaned = value.rstrip("/")
    if cleaned.endswith("/api"):
        cleaned = cleaned[: -len("/api")]
    return cleaned


def _normalise_identifier(value: str) -> str:
    return value.replace(" ", "_")


def _coalesce_str(*values: object) -> Optional[str]:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def _slugify(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
    lowered = ascii_text.lower()
    cleaned = re.sub(r"[^a-z0-9]+", "-", lowered).strip("-")
    return cleaned or "unknown"


__all__ = ["BuiltFileAsset", "FileAssetFactory"]
