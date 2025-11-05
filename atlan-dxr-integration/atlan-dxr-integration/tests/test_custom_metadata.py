"""Tests for custom metadata provisioning logic."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Dict

import pytest
from pyatlan.errors import ErrorCode, NotFoundError
from pyatlan.model.enums import AtlanCustomAttributePrimitiveType, AtlanIcon, AtlanTagColor

from atlan_dxr_integration import custom_metadata
from atlan_dxr_integration.custom_metadata import (
    AttributeSpec,
    CustomMetadataManager,
    MetadataSetSpec,
)


class _StubEnumCache:
    def get_by_name(self, name: str):
        return None


class _StubAssetClient:
    def search(self, request):  # noqa: ARG002 - signature required by AttributeDef.create
        return [SimpleNamespace(qualified_name="default/custom/dxr-unstructured-attributes")]


class _StubCustomMetadataCache:
    def __init__(self):
        self.defs: Dict[str, object] = {}
        self.refresh_count = 0

    def refresh_cache(self):
        self.refresh_count += 1

    def get_custom_metadata_def(self, name: str):
        if name not in self.defs:
            raise NotFoundError(ErrorCode.CM_NOT_FOUND_BY_NAME, name)
        return self.defs[name]

    def store(self, definition):
        self.defs[definition.display_name] = definition


class _StubTypedefClient:
    def __init__(self, client):
        self.client = client
        self.created = []
        self.updated = []

    def create(self, typedef):
        self.created.append(typedef)
        self.client.custom_metadata_cache.store(typedef)
        return SimpleNamespace(custom_metadata_defs=[typedef])

    def update(self, typedef):
        self.updated.append(typedef)
        self.client.custom_metadata_cache.store(typedef)
        return SimpleNamespace(custom_metadata_defs=[typedef])


class _StubAtlanClient:
    def __init__(self):
        self.enum_cache = _StubEnumCache()
        self.asset = _StubAssetClient()
        self.custom_metadata_cache = _StubCustomMetadataCache()
        self.typedef = _StubTypedefClient(self)


def test_ensure_specifications_creates_missing_sets(monkeypatch: pytest.MonkeyPatch) -> None:
    spec = MetadataSetSpec(
        display_name="DXR Test Metadata",
        description="Test metadata set",
        attributes=[
            AttributeSpec(
                display_name="Test Field",
                attribute_type=AtlanCustomAttributePrimitiveType.STRING,
                multi_valued=True,
                applicable_asset_types={"File"},
                description="text",
            )
        ],
        icon=AtlanIcon.FILE,
        color=AtlanTagColor.GRAY,
    )
    monkeypatch.setattr(custom_metadata, "_default_metadata_specs", lambda: (spec,))
    monkeypatch.setattr(
        "pyatlan.model.typedef._get_all_qualified_names",
        lambda client, asset_type: {"default/custom/dxr-unstructured-attributes"},
    )

    client = _StubAtlanClient()
    manager = CustomMetadataManager(client)
    manager.ensure_specifications()

    assert client.typedef.created, "expected create call"
    created_def = client.custom_metadata_cache.defs["DXR Test Metadata"]
    assert len(created_def.attribute_defs) == 1
    assert created_def.attribute_defs[0].display_name == "Test Field"


def test_ensure_specifications_extends_existing(monkeypatch: pytest.MonkeyPatch) -> None:
    spec = MetadataSetSpec(
        display_name="DXR Extend Metadata",
        description="Extend test",
        attributes=[
            AttributeSpec(
                display_name="Existing Field",
                attribute_type=AtlanCustomAttributePrimitiveType.STRING,
                applicable_asset_types={"Table"},
            ),
            AttributeSpec(
                display_name="New Field",
                attribute_type=AtlanCustomAttributePrimitiveType.INTEGER,
                applicable_asset_types={"Table"},
            ),
        ],
        icon=AtlanIcon.TABLE,
        color=AtlanTagColor.GREEN,
    )
    monkeypatch.setattr(custom_metadata, "_default_metadata_specs", lambda: (spec,))
    monkeypatch.setattr(
        "pyatlan.model.typedef._get_all_qualified_names",
        lambda client, asset_type: {"default/custom/dxr-unstructured-attributes"},
    )

    client = _StubAtlanClient()
    manager = CustomMetadataManager(client)

    baseline_spec = MetadataSetSpec(
        display_name="DXR Extend Metadata",
        description="Baseline",
        attributes=[spec.attributes[0]],
        icon=spec.icon,
        color=spec.color,
    )
    baseline = manager._build_definition(baseline_spec)
    client.custom_metadata_cache.store(baseline)

    manager.ensure_specifications()

    assert not client.typedef.created, "should not create new set when one exists"
    assert client.typedef.updated, "expected update to append missing attribute"

    updated_def = client.custom_metadata_cache.defs["DXR Extend Metadata"]
    names = {attr.display_name for attr in updated_def.attribute_defs}
    assert names == {"Existing Field", "New Field"}
