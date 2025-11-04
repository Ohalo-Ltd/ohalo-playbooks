"""Tests for tag registry and global attribute provisioning."""

from __future__ import annotations

from atlan_dxr_integration.atlan_service import AtlanRequestError
from atlan_dxr_integration.atlan_types import TagColor
from atlan_dxr_integration.dxr_client import Classification
from atlan_dxr_integration.global_attributes import GlobalAttributeManager
from atlan_dxr_integration.tag_registry import TagRegistry


class _StubRESTClient:
    def __init__(self):
        self.created_payloads = []
        self.typedefs: dict[str, dict] = {}

    def create_typedefs(self, payload):
        self.created_payloads.append(payload)
        for typedef in payload.get("classificationDefs", []):
            name = typedef["name"]
            hashed = f"hashed::{name}"
            enriched = typedef | {"name": hashed, "displayName": name}
            self.typedefs[name] = enriched
            self.typedefs[hashed] = enriched
        return {"classificationDefs": list(self.typedefs.values())}

    def get_typedef(self, name: str):
        typedef = self.typedefs.get(name)
        if not typedef:
            raise AtlanRequestError("not found", status_code=404, details=None)
        return {"classificationDefs": [typedef]}

    def list_classification_typedefs(self):
        return list(self.typedefs.values())


class _DelayedRESTClient(_StubRESTClient):
    def __init__(self, *, fail_reads_before_visible: int, always_missing: bool = False):
        super().__init__()
        self.fail_reads_before_visible = fail_reads_before_visible
        self.always_missing = always_missing
        self._read_attempts: dict[str, int] = {}

    def get_typedef(self, name: str):
        attempts = self._read_attempts.get(name, 0)
        self._read_attempts[name] = attempts + 1

        typedef = self.typedefs.get(name)
        if not typedef:
            raise AtlanRequestError("not found", status_code=404, details=None)

        if self.always_missing or attempts < self.fail_reads_before_visible:
            raise AtlanRequestError("not yet available", status_code=404, details=None)

        return {"classificationDefs": [typedef]}


def test_tag_registry_creates_missing_tags():
    client = _StubRESTClient()
    registry = TagRegistry(client, namespace="DXR")

    handle = registry.ensure(
        slug_parts=["classification", "annotator", "card"],
        display_name="Annotator :: Card",
        description="Annotator tag",
        color=TagColor.YELLOW,
    )

    assert handle.display_name == "DXR :: Annotator :: Card"
    assert handle.hashed_name.startswith("hashed::DXR :: Annotator :: Card")
    assert client.created_payloads, "Expected create_typedefs to be called"

    # Second ensure should reuse existing typedef without additional create call.
    client.created_payloads.clear()
    handle_again = registry.ensure(
        slug_parts=["classification", "annotator", "card"],
        display_name="Annotator :: Card",
        color=TagColor.YELLOW,
    )
    assert handle_again == handle
    assert not client.created_payloads


def test_tag_registry_retries_until_typedef_visible():
    client = _DelayedRESTClient(fail_reads_before_visible=2)
    registry = TagRegistry(client, namespace="DXR")
    registry._typedef_retry_attempts = 4
    registry._typedef_retry_delay = 0

    handle = registry.ensure(
        slug_parts=["classification", "annotator", "card"],
        display_name="Annotator :: Card",
        description="Annotator tag",
        color=TagColor.YELLOW,
    )

    assert handle.hashed_name.startswith("hashed::DXR :: Annotator :: Card")


def test_tag_registry_falls_back_when_typedef_never_visible():
    client = _DelayedRESTClient(fail_reads_before_visible=5, always_missing=True)
    registry = TagRegistry(client, namespace="DXR")
    registry._typedef_retry_attempts = 3
    registry._typedef_retry_delay = 0

    handle = registry.ensure(
        slug_parts=["classification", "annotator", "card"],
        display_name="Annotator :: Card",
        description="Annotator tag",
        color=TagColor.YELLOW,
    )

    assert handle.hashed_name.startswith("hashed::DXR :: Annotator :: Card")
    assert client.created_payloads, "Expected create_typedefs to be called despite fallback"


def test_tag_registry_recreates_deleted_typedef():
    client = _StubRESTClient()
    deleted_name = "DXR :: Annotator :: Card"
    client.typedefs[deleted_name] = {
        "name": "DELETED",
        "displayName": deleted_name,
        "entityStatus": "DELETED",
    }
    registry = TagRegistry(client, namespace="DXR")

    handle = registry.ensure(
        slug_parts=["classification", "annotator", "card"],
        display_name="Annotator :: Card",
        description="Annotator tag",
        color=TagColor.YELLOW,
    )

    assert handle.hashed_name != "DELETED"
    assert client.created_payloads, "Expected create_typedefs to be called to replace deleted tag"


def test_global_attribute_manager_generates_handles():
    registry = TagRegistry(_StubRESTClient(), namespace="DXR")
    manager = GlobalAttributeManager(tag_registry=registry)

    classifications = [
        Classification(
            identifier="cls-1",
            name="Sensitive",
            type="ANNOTATOR",
            subtype=None,
            description="desc",
            link=None,
            search_link=None,
        )
    ]

    mapping = manager.ensure_classification_tags(classifications)
    handle = mapping["cls-1"]
    assert handle.display_name == "DXR :: Annotator :: Sensitive"
