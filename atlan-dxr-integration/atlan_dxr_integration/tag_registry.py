"""Utilities for provisioning and reusing Atlan tag definitions."""

from __future__ import annotations

import logging
import re
import unicodedata
from dataclasses import dataclass
from typing import Dict, Iterable, Optional

from pyatlan.client.atlan import AtlanClient
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
    type_name: str
    display_name: str


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
        if normalized_slug in self._handles:
            return self._handles[normalized_slug]

        type_name = self._build_type_name(normalized_slug)
        display = self._build_display_name(display_name)
        if not self._tag_exists(type_name):
            LOGGER.debug(
                "Creating Atlan tag definition '%s' (slug=%s, type=%s)",
                display,
                normalized_slug,
                type_name,
            )
            tag_def = AtlanTagDef.create(name=type_name, color=color, icon=icon)
            tag_def.display_name = display
            tag_def.entity_types = sorted({"Asset", "File"})
            if description:
                tag_def.description = description
            try:
                self._client.typedef.create(tag_def)
            except Exception as exc:  # pragma: no cover - external API
                LOGGER.error(
                    "Failed to create Atlan tag '%s' (type=%s): %s",
                    display,
                    type_name,
                    exc,
                )
            else:
                # Refresh the cache so subsequent calls can resolve the tag's GUID.
                try:
                    self._client.atlan_tag_cache.refresh_cache()
                except Exception as refresh_exc:  # pragma: no cover - defensive
                    LOGGER.warning("Unable to refresh Atlan tag cache: %s", refresh_exc)
        else:
            self._ensure_tag_supports_assets(type_name)

        handle = TagHandle(slug=normalized_slug, type_name=type_name, display_name=display)
        self._handles[normalized_slug] = handle
        return handle

    def _tag_exists(self, tag_name: str) -> bool:
        self._ensure_cache_loaded()
        try:
            tag_id = self._client.atlan_tag_cache.get_id_for_name(tag_name)
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

    def _build_type_name(self, slug: str) -> str:
        namespace_slug = _slugify(self._namespace) or "tag"
        normalized_slug = slug.replace("/", "__")
        if normalized_slug:
            type_name = f"{namespace_slug}__{normalized_slug}"
        else:
            type_name = namespace_slug
        if len(type_name) > 128:
            type_name = type_name[:128]
        return type_name

    def _ensure_tag_supports_assets(self, type_name: str) -> None:
        try:
            tag_def = self._client.typedef.get_by_name(type_name)
        except Exception as exc:  # pragma: no cover - Atlan quirks
            LOGGER.warning(
                "Unable to fetch Atlan tag '%s' for verification: %s",
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

        updated_types = sorted(entity_types.union(required))
        tag_def.entity_types = updated_types
        try:
            self._client.typedef.update(tag_def)
        except Exception as exc:  # pragma: no cover - Atlan quirks
            LOGGER.warning(
                "Unable to update entity types for Atlan tag '%s': %s",
                type_name,
                exc,
            )
        else:
            LOGGER.debug(
                "Updated entity types for Atlan tag '%s' to %s",
                type_name,
                updated_types,
            )


__all__ = ["TagRegistry", "TagHandle"]
