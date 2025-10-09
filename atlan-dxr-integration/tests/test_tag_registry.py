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
            self.typedefs[name] = typedef | {"name": name}

    def get_typedef(self, name: str):
        typedef = self.typedefs.get(name)
        if not typedef:
            raise AtlanRequestError("not found", status_code=404, details=None)
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
    assert handle.hashed_name == "DXR :: Annotator :: Card"
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
