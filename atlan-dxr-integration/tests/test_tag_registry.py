"""Tests for tag registry and global attribute provisioning."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from pyatlan.errors import ErrorCode
from pyatlan.model.enums import AtlanTagColor

from atlan_dxr_integration.dxr_client import Classification
from atlan_dxr_integration.global_attributes import GlobalAttributeManager
from atlan_dxr_integration.tag_registry import TagRegistry


class _FakeTypeDefClient:
    def __init__(self, tag_cache):
        self.created = []
        self._tag_cache = tag_cache

    def create(self, tag_def):  # pragma: no cover - exercised via registry
        self.created.append(tag_def)
        self._tag_cache.register(tag_def.name)


class _FakeTagCache:
    def __init__(self):
        self.refresh_called = False
        self._ids: dict[str, str] = {}

    def refresh_cache(self):
        self.refresh_called = True

    def get_id_for_name(self, name: str) -> str | None:
        return self._ids.get(name)

    def register(self, name: str) -> None:
        self._ids[name] = f"tag-{len(self._ids)+1}"


class _FakeClient:
    def __init__(self):
        self.atlan_tag_cache = _FakeTagCache()
        self.typedef = _FakeTypeDefClient(self.atlan_tag_cache)


def test_tag_registry_creates_missing_tags():
    client = _FakeClient()
    registry = TagRegistry(client, namespace="DXR")

    handle = registry.ensure(
        slug_parts=["classification", "annotator", "card"],
        display_name="Credit Card",
        description="Annotator tag",
        color=AtlanTagColor.YELLOW,
    )

    assert handle.name == "DXR :: Credit Card"
    assert client.typedef.created, "Expected tag definition to be created"
    assert client.atlan_tag_cache.refresh_called is True

    # Calling again should reuse the server-side definition without creating another tag
    client.typedef.created.clear()
    handle_again = registry.ensure(
        slug_parts=["classification", "annotator", "card"],
        display_name="Credit Card",
        description="Annotator tag",
        color=AtlanTagColor.YELLOW,
    )
    assert handle_again == handle
    assert client.typedef.created == []


def test_global_attribute_manager_generates_handles():
    client = _FakeClient()
    registry = TagRegistry(client, namespace="DXR")
    manager = GlobalAttributeManager(client=client, tag_registry=registry)

    classifications = [
        Classification(
            identifier="cls-1",
            name="Sensitive",
            type="ANNOTATOR",
            subtype=None,
            description="Sensitive data annotator",
            link=None,
            search_link=None,
        )
    ]

    mapping = manager.ensure_classification_tags(classifications)

    assert set(mapping) == {"cls-1"}
    handle = mapping["cls-1"]
    assert handle.name == "DXR :: Annotator :: Sensitive"
