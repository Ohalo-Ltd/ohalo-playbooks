"""Utilities for provisioning and reusing Atlan tag definitions."""

from __future__ import annotations

import logging
import re
import unicodedata
from dataclasses import dataclass
from typing import Dict, Iterable, Optional

from .atlan_service import AtlanRESTClient, AtlanRequestError

from .atlan_types import TagColor, TagIcon

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

    def __init__(self, client: AtlanRESTClient, *, namespace: str = "DXR") -> None:
        self._client = client
        self._namespace = namespace.strip() or "DXR"
        self._handles: Dict[str, TagHandle] = {}

    def ensure(
        self,
        *,
        slug_parts: Iterable[str],
        display_name: str,
        description: Optional[str] = None,
        color: TagColor = TagColor.GRAY,
        icon: TagIcon = TagIcon.ATLAN_TAG,
    ) -> TagHandle:
        """Guarantee a tag definition exists and return its handle."""

        slug = self._build_slug(slug_parts)
        cached = self._handles.get(slug)
        if cached:
            return cached

        full_display = self._build_display_name(display_name)
        typedef = self._fetch_typedef(full_display)
        if not typedef:
            self._create_typedef(full_display, description, color, icon)
            typedef = self._fetch_typedef(full_display)
            if not typedef:
                raise RuntimeError(
                    f"Unable to locate tag definition after creation for '{full_display}'"
                )

        handle = TagHandle(
            slug=slug,
            display_name=full_display,
            hashed_name=typedef.get("name", full_display),
        )
        self._handles[slug] = handle
        return handle

    def _fetch_typedef(self, name: str) -> Optional[Dict[str, object]]:
        try:
            response = self._client.get_typedef(name)
        except AtlanRequestError as exc:
            if exc.status_code == 404:
                return None
            raise
        if isinstance(response, dict):
            typedefs = response.get("classificationDefs") or []
            for typedef in typedefs:
                if isinstance(typedef, dict) and typedef.get("name") == name:
                    return typedef
        return None

    def _create_typedef(
        self,
        name: str,
        description: Optional[str],
        color: TagColor,
        icon: TagIcon,
    ) -> None:
        payload = {
            "classificationDefs": [
                {
                    "category": "CLASSIFICATION",
                    "name": name,
                    "description": description or "",
                    "attributeDefs": [],
                    "entityTypes": ["Asset", "File"],
                    "options": {
                        "color": color.value,
                        "icon": icon.value,
                    },
                }
            ]
        }

        try:
            self._client.create_typedefs(payload)
        except AtlanRequestError as exc:
            if exc.status_code == 409:
                LOGGER.debug("Tag '%s' already exists: %s", name, exc.details)
            else:
                raise

    def _build_display_name(self, display_name: str) -> str:
        display = display_name.strip() or "Unknown"
        return f"{self._namespace.strip()} :: {display}"

    @staticmethod
    def _build_slug(parts: Iterable[str]) -> str:
        tokens = [_slugify(part) for part in parts if part]
        return "/".join(token for token in tokens if token) or "unknown"


__all__ = ["TagRegistry", "TagHandle"]
