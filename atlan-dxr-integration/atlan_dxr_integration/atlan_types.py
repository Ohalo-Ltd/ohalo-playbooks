"""Local Atlan enum helpers to avoid PyAtlan dependency."""

from __future__ import annotations

from enum import Enum


class TagColor(str, Enum):
    GRAY = "Gray"
    GREEN = "Green"
    YELLOW = "Yellow"
    RED = "Red"


class TagIcon(str, Enum):
    ATLAN_TAG = "atlanTags"


__all__ = ["TagColor", "TagIcon"]
