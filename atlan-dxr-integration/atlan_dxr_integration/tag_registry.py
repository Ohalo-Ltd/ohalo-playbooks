"""Utilities for provisioning and reusing Atlan tag definitions."""

from __future__ import annotations

import logging
import re
import unicodedata
from dataclasses import dataclass
from typing import Iterable, Optional

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
    name: str


class TagRegistry:
    """Ensures Atlan tag definitions exist for DXR-derived metadata."""

    def __init__(self, client: AtlanClient, *, namespace: str = "DXR") -> None:
        self._client = client
        self._namespace = namespace.strip() or "DXR"
        self._cache_loaded = False

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
        tag_name = self._build_tag_name(display_name)
        if not self._tag_exists(tag_name):
            LOGGER.debug("Creating Atlan tag definition '%s' (slug=%s)", tag_name, normalized_slug)
            tag_def = AtlanTagDef.create(name=tag_name, color=color, icon=icon)
            if description:
                tag_def.description = description
            self._client.typedef.create(tag_def)
            # Refresh the cache so subsequent calls can resolve the tag's GUID.
            try:
                self._client.atlan_tag_cache.refresh_cache()
            except Exception as exc:  # pragma: no cover - defensive
                LOGGER.warning("Unable to refresh Atlan tag cache: %s", exc)

        handle = TagHandle(slug=normalized_slug, name=tag_name)
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

    def _build_tag_name(self, display_name: str) -> str:
        display = display_name.strip() or "Unknown"
        return f"{self._namespace.strip()} :: {display}"

    @staticmethod
    def _build_slug(parts: Iterable[str]) -> str:
        tokens = [_slugify(part) for part in parts if part]
        return "/".join(token for token in tokens if token) or "unknown"


__all__ = ["TagRegistry", "TagHandle"]
