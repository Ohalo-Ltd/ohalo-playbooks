"""Utilities for provisioning and reusing Atlan tag definitions."""

from __future__ import annotations

import logging
import re
import time
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
        self._typedef_retry_attempts = 5
        self._typedef_retry_delay = 1.0
        self._typedef_cache: Dict[str, Dict[str, object]] = {}
        self._typedef_cache_loaded = False

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
        if cached and (cached.hashed_name or "").upper() != "DELETED":
            return cached
        if cached:
            self._handles.pop(slug, None)

        full_display = self._build_display_name(display_name)
        typedef = self._fetch_typedef(full_display)
        if not typedef:
            created = self._create_typedef(full_display, description, color, icon)
            typedef = self._wait_for_typedef(
                full_display,
                attempts=(self._typedef_retry_attempts if created else 1),
            )
            if not typedef:
                LOGGER.warning(
                    "Unable to confirm tag definition '%s' after creation; proceeding with best-effort handle.",
                    full_display,
                )
                typedef = {"name": full_display}

        hashed_name = None
        if isinstance(typedef, dict):
            candidate = typedef.get("name")
            if isinstance(candidate, str) and candidate.upper() != "DELETED":
                hashed_name = candidate
        if hashed_name:
            self._typedef_cache[hashed_name] = typedef  # type: ignore[index]
        self._typedef_cache[full_display] = typedef  # type: ignore[index]

        handle = TagHandle(
            slug=slug,
            display_name=full_display,
            hashed_name=hashed_name or full_display,
        )
        self._handles[slug] = handle
        return handle

    def _fetch_typedef(self, name: str) -> Optional[Dict[str, object]]:
        self._ensure_typedef_cache()
        typedef = self._typedef_cache.get(name)
        if typedef and not _is_active_typedef(typedef):
            self._remove_from_cache(name, typedef)
            return None
        if typedef:
            return typedef
        if self._typedef_cache_loaded:
            return None
        # Cache is still empty because the first load failed; try again.
        self._prime_typedef_cache(force=True)
        typedef = self._typedef_cache.get(name)
        if typedef and not _is_active_typedef(typedef):
            self._remove_from_cache(name, typedef)
            return None
        return typedef

    def _ensure_typedef_cache(self) -> None:
        if not self._typedef_cache_loaded:
            self._prime_typedef_cache(force=True)

    def _prime_typedef_cache(self, *, force: bool = False) -> None:
        if self._typedef_cache_loaded and not force:
            return
        try:
            typedefs = self._client.list_classification_typedefs()
        except AtlanRequestError:
            return
        for typedef in typedefs:
            if not _is_active_typedef(typedef):
                continue
            display = typedef.get("displayName") or typedef.get("name")
            hashed = typedef.get("name")
            if display:
                self._typedef_cache[str(display)] = typedef
            if hashed:
                self._typedef_cache[str(hashed)] = typedef
        self._typedef_cache_loaded = True

    def _wait_for_typedef(
        self,
        name: str,
        *,
        attempts: Optional[int] = None,
    ) -> Optional[Dict[str, object]]:
        total_attempts = attempts or self._typedef_retry_attempts
        for attempt in range(total_attempts):
            typedef = self._fetch_typedef(name)
            if typedef:
                return typedef
            if attempt < total_attempts - 1:
                time.sleep(self._typedef_retry_delay)
        return None

    def _create_typedef(
        self,
        name: str,
        description: Optional[str],
        color: TagColor,
        icon: TagIcon,
    ) -> bool:
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
            response = self._client.create_typedefs(payload)
            self._prime_typedef_cache(force=True)
            return bool(response.get("classificationDefs"))
        except AtlanRequestError as exc:
            if exc.status_code == 409:
                LOGGER.debug("Tag '%s' already exists: %s", name, exc.details)
                self._prime_typedef_cache(force=True)
                return False
            raise

    def _build_display_name(self, display_name: str) -> str:
        display = display_name.strip() or "Unknown"
        return f"{self._namespace.strip()} :: {display}"

    @staticmethod
    def _build_slug(parts: Iterable[str]) -> str:
        tokens = [_slugify(part) for part in parts if part]
        return "/".join(token for token in tokens if token) or "unknown"

    def _remove_from_cache(self, key: str, typedef: Dict[str, object]) -> None:
        """Remove cached entries for a stale typedef so we can recreate it."""

        self._typedef_cache.pop(str(key), None)
        if isinstance(typedef, dict):
            hashed = typedef.get("name")
            display = typedef.get("displayName")
            if hashed:
                self._typedef_cache.pop(str(hashed), None)
            if display:
                self._typedef_cache.pop(str(display), None)


def _is_active_typedef(typedef: Dict[str, object]) -> bool:
    """Return True when the typedef represents an active classification."""

    if not isinstance(typedef, dict):
        return False

    raw_name = typedef.get("name")
    if isinstance(raw_name, str) and raw_name.upper() == "DELETED":
        return False

    for key in ("entityStatus", "status", "state"):
        value = typedef.get(key)
        if isinstance(value, str) and value.upper() in {"DELETED", "PURGED", "DISABLED"}:
            return False

    return True


__all__ = ["TagRegistry", "TagHandle"]
