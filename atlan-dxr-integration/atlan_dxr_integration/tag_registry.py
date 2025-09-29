"""Utilities for provisioning and reusing Atlan tag definitions."""

from __future__ import annotations

import logging
import re
import unicodedata
from dataclasses import dataclass
from typing import Dict, Iterable, Optional

from pyatlan.client.atlan import AtlanClient
from pyatlan.errors import AtlanError, ConflictError
from pyatlan.model.enums import AtlanIcon, AtlanTagColor
from pyatlan.model.typedef import AtlanTagDef

LOGGER = logging.getLogger(__name__)


def _slugify(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
    lowered = ascii_text.lower()
    cleaned = re.sub(r"[^a-z0-9]+", "-", lowered).strip("-")
    return cleaned or "unknown"


@dataclass(frozen=True)
class TagHandle:
    """Handle that describes the tag definition attached to a DXR concept."""

    slug: str
    display_name: str
    hashed_name: str


class TagRegistry:
    """Ensures Atlan tag definitions exist for DXR-derived metadata."""

    def __init__(self, client: AtlanClient, *, namespace: str = "DXR") -> None:
        self._client = client
        self._namespace = namespace.strip() or "DXR"
        self._cache_loaded = False
        self._handles: Dict[str, TagHandle] = {}

    def ensure(
        self,
        *,
        slug_parts: Iterable[str],
        display_name: str,
        description: Optional[str] = None,
        color: AtlanTagColor = AtlanTagColor.GRAY,
        icon: AtlanIcon = AtlanIcon.ATLAN_TAG,
    ) -> TagHandle:
        """Guarantee a tag definition exists and return its handle."""

        normalized_slug = self._build_slug(slug_parts)
        cached = self._handles.get(normalized_slug)
        if cached:
            return cached

        display = self._build_display_name(display_name)
        hashed_name = self._ensure_tag_exists(display, description, color, icon)

        handle = TagHandle(
            slug=normalized_slug,
            display_name=display,
            hashed_name=hashed_name,
        )
        self._handles[normalized_slug] = handle
        return handle

    def _tag_exists(self, display_name: str) -> bool:
        self._ensure_cache_loaded()
        try:
            tag_id = self._client.atlan_tag_cache.get_id_for_name(display_name)
            return bool(tag_id)
        except Exception:
            return False

    def _ensure_cache_loaded(self) -> None:
        if self._cache_loaded:
            return
        try:
            self._client.atlan_tag_cache.refresh_cache()
        except Exception as exc:  # pragma: no cover - debug aid
            LOGGER.debug("Unable to refresh Atlan tag cache: %s", exc)
        finally:
            self._cache_loaded = True

    def _build_display_name(self, display_name: str) -> str:
        display = display_name.strip() or "Unknown"
        return f"{self._namespace.strip()} :: {display}"

    @staticmethod
    def _build_slug(parts: Iterable[str]) -> str:
        tokens = [_slugify(part) for part in parts if part]
        return "/".join(token for token in tokens if token) or "unknown"

    def _ensure_tag_exists(
        self,
        display_name: str,
        description: Optional[str],
        color: AtlanTagColor,
        icon: AtlanIcon,
    ) -> str:
        existed = self._tag_exists(display_name)

        if not existed:
            tag_def = AtlanTagDef.create(name=display_name, color=color, icon=icon)
            tag_def.entity_types = sorted({"Asset", "File"})
            if description:
                tag_def.description = description

            try:
                self._client.create_typedef(tag_def)
            except ConflictError:
                LOGGER.debug("Atlan tag '%s' already exists", display_name)
            except AtlanError as exc:
                LOGGER.error("Failed to create Atlan tag '%s': %s", display_name, exc)
                raise

        tag_id = self._resolve_type_name(display_name)
        if not tag_id:
            raise RuntimeError(
                f"Unable to resolve Atlan tag identifier for '{display_name}'"
            )

        self._ensure_entity_types(tag_id, display_name)
        return tag_id

    def _resolve_type_name(self, display_name: str) -> Optional[str]:
        try:
            tag_id = self._client.atlan_tag_cache.get_id_for_name(display_name)
        except Exception:
            tag_id = None

        if tag_id:
            return tag_id

        try:
            self._client.atlan_tag_cache.refresh_cache()
            self._cache_loaded = True
            tag_id = self._client.atlan_tag_cache.get_id_for_name(display_name)
        except Exception as exc:
            LOGGER.warning("Unable to refresh Atlan tag cache: %s", exc)
            tag_id = None
        return tag_id

    def _ensure_entity_types(self, type_name: str, display_name: str) -> None:
        try:
            tag_def = self._client.typedef.get_by_name(type_name)
        except Exception as exc:  # pragma: no cover - Atlan quirks
            LOGGER.debug(
                "Unable to fetch Atlan tag '%s' (id=%s) for verification: %s",
                display_name,
                type_name,
                exc,
            )
            return

        if not isinstance(tag_def, AtlanTagDef):
            return

        entity_types = set(tag_def.entity_types or [])
        required = {"Asset", "File"}
        if required.issubset(entity_types):
            return

        tag_def.entity_types = sorted(entity_types.union(required))
        try:
            self._client.update_typedef(tag_def)
        except AtlanError as exc:  # pragma: no cover - Atlan quirks
            LOGGER.warning(
                "Unable to update entity types for Atlan tag '%s': %s",
                display_name,
                exc,
            )


__all__ = ["TagRegistry", "TagHandle"]
