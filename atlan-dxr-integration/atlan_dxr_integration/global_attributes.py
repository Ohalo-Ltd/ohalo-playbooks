"""Provision global DXR metadata assets and their associated tags."""

from __future__ import annotations

import logging
from typing import Dict, Iterable, Optional

from pyatlan.client.atlan import AtlanClient
from pyatlan.model.enums import AtlanTagColor

from .dxr_client import Classification
from .tag_registry import TagHandle, TagRegistry

LOGGER = logging.getLogger(__name__)


_CLASSIFICATION_COLOR_MAP = {
    "ANNOTATOR": AtlanTagColor.YELLOW,
    "EXTRACTOR": AtlanTagColor.GREEN,
    "CLASSIFICATION": AtlanTagColor.RED,
}


class GlobalAttributeManager:
    """Create reusable tag handles for DXR classifications and related metadata."""

    def __init__(
        self,
        *,
        client: AtlanClient,
        tag_registry: TagRegistry,
    ) -> None:
        self._client = client
        self._tag_registry = tag_registry

    def ensure_classification_tags(
        self, classifications: Iterable[Classification]
    ) -> Dict[str, TagHandle]:
        """Return tag handles keyed by classification identifier."""

        mapping: Dict[str, TagHandle] = {}
        for classification in classifications:
            identifier = classification.identifier
            if not identifier:
                LOGGER.debug("Skipping classification without identifier: %s", classification)
                continue

            type_key = (classification.type or "classification").upper()
            color = _CLASSIFICATION_COLOR_MAP.get(type_key, AtlanTagColor.GRAY)
            display = classification.name or identifier
            description = classification.description
            slug_parts = ["classification", type_key, identifier]

            handle = self._tag_registry.ensure(
                slug_parts=slug_parts,
                display_name=f"{type_key.title()} :: {display}",
                description=description,
                color=color,
            )
            mapping[identifier] = handle
        return mapping


__all__ = ["GlobalAttributeManager"]
