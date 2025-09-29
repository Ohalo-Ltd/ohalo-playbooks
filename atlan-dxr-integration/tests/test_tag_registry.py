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
        self._stored: dict[str, SimpleNamespace] = {}

    def create(self, tag_def):  # pragma: no cover - exercised via registry
        self.created.append(tag_def)
        hashed = f"hashed::{tag_def.display_name}"
        tag_def.name = hashed
        self._stored[hashed] = SimpleNamespace(
            name=hashed,
            display_name=tag_def.display_name,
            entity_types=list(tag_def.entity_types or []),
        )
        self._tag_cache.register(tag_def.display_name, hashed)

    def get_by_name(self, name):
        return self._stored.get(name)

    def update(self, tag_def):
        self._stored[tag_def.name] = tag_def


class _FakeTagCache:
    def __init__(self):
        self.refresh_called = False
        self._ids: dict[str, str] = {}

    def refresh_cache(self):
        self.refresh_called = True

    def get_id_for_name(self, name: str) -> str | None:
        return self._ids.get(name)

    def register(self, display_name: str, internal_name: str) -> None:
        self._ids[display_name] = internal_name


class _FakeClient:
    def __init__(self):
        self.atlan_tag_cache = _FakeTagCache()
        self.typedef = _FakeTypeDefClient(self.atlan_tag_cache)

    def create_typedef(self, tag_def):
        self.typedef.create(tag_def)

    def update_typedef(self, tag_def):
        self.typedef.update(tag_def)


def test_tag_registry_creates_missing_tags():
    client = _FakeClient()
    registry = TagRegistry(client, namespace="DXR")

    handle = registry.ensure(
        slug_parts=["classification", "annotator", "card"],
        display_name="Credit Card",
        description="Annotator tag",
        color=AtlanTagColor.YELLOW,
    )

    assert handle.display_name == "DXR :: Credit Card"
    assert handle.hashed_name == "hashed::DXR :: Credit Card"
    assert client.typedef.created, "Expected tag definition to be created"
    assert client.atlan_tag_cache.refresh_called is True
    created_def = client.typedef.created[-1]
    assert created_def.name == handle.hashed_name
    assert created_def.display_name == handle.display_name

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
    assert handle.display_name == "DXR :: Annotator :: Sensitive"
    assert handle.hashed_name == "hashed::DXR :: Annotator :: Sensitive"
