"""Tests for the DXR → Atlan file asset factory."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from atlan_dxr_integration.file_asset_builder import FileAssetFactory
from atlan_dxr_integration.tag_registry import TagRegistry
from pyatlan.errors import ErrorCode
from pyatlan.model.enums import FileType


class _FakeTypeDefClient:
    def __init__(self):
        self.created = []
        self._existing = {}

    def get_by_name(self, name):
        if name not in self._existing:
            raise ErrorCode.ATLAN_TAG_NOT_FOUND_BY_NAME.exception_with_parameters(name)
        return self._existing[name]

    def create(self, tag_def):
        self.created.append(tag_def)
        self._existing[tag_def.name] = tag_def


class _FakeTagCache:
    def refresh_cache(self):
        return None


class _FakeClient:
    def __init__(self):
        self.typedef = _FakeTypeDefClient()
        self.atlan_tag_cache = _FakeTagCache()


def _build_factory():
    tag_registry = TagRegistry(_FakeClient(), namespace="DXR")
    return tag_registry, FileAssetFactory(
        tag_registry=tag_registry,
        tag_namespace="DXR",
        dxr_base_url="https://dxr.example.com/api",
    )


def _sample_payload() -> dict:
    return {
        "datasource": {
            "id": "c3d4e5f6-a7b8-4012-8def-345678901234",
            "name": "Finance Department SharePoint",
            "connector": {
                "type": "BOX",
                "userId": "user-12345",
            },
        },
        "fileName": "2011_audited_financial_statement_msword.doc",
        "fileId": "0KQUMpgBVo-c9i0dUnJ8",
        "path": "Documents/Confidential Folder/2011_audited_financial_statement_msword.doc",
        "size": 1_048_576,
        "mimeType": "application/pdf",
        "scanDepth": "DISCOVERY_AND_CLASSIFICATION",
        "labels": [
            {
                "id": "a1b2c3d4-e5f6-4890-abcd-ef1234567890",
                "name": "Sales Data",
            }
        ],
        "annotators": [
            {
                "name": "Credit card",
                "domain": {"name": "Financially Sensitive"},
            }
        ],
        "dlpLabels": [
            {
                "name": "Confidential",
                "dlpSystem": "PURVIEW",
            }
        ],
        "extractedMetadata": [
            {
                "id": "b2c3d4e5-f617-4901-bcde-f23456789012",
                "name": "Contract Type",
                "value": "Annual Service Agreement",
            }
        ],
        "entitlements": {
            "whoCanAccess": [
                {
                    "accountType": "USER",
                    "name": "Sarah Conner",
                    "email": "sarah@ohalo.onmicrosoft.com",
                }
            ]
        },
        "categories": ["Finance", {"name": "Audit"}],
        "owner": {
            "name": "Sarah Conner",
            "email": "sarah@ohalo.onmicrosoft.com",
        },
    }


def test_factory_builds_enriched_file_asset():
    tag_registry, factory = _build_factory()
    classification_handle = tag_registry.ensure(
        slug_parts=["classification", "annotator", "sales-data"],
        display_name="Annotator :: Sales Data",
    )
    annotator_handle = tag_registry.ensure(
        slug_parts=["classification", "annotator", "credit-card"],
        display_name="Annotator :: Credit card",
    )
    annotator_domain_handle = tag_registry.ensure(
        slug_parts=["classification", "annotator-domain", "financially-sensitive"],
        display_name="Annotator Domain :: Financially Sensitive",
    )
    extractor_handle = tag_registry.ensure(
        slug_parts=["classification", "extractor", "contract-type"],
        display_name="Extractor :: Contract Type",
    )

    asset = factory.build(
        _sample_payload(),
        connection_qualified_name="default/custom/dxr-datasource-finance",
        connection_name="dxr-datasource-finance",
        classification_tags={
            "a1b2c3d4-e5f6-4890-abcd-ef1234567890": classification_handle,
            "e5f6a7b8-c9d0-4234-9f01-567890123456": annotator_handle,
            "f6a7b8c9-d0e1-4345-a012-678901234567": annotator_domain_handle,
            "b2c3d4e5-f617-4901-bcde-f23456789012": extractor_handle,
        },
    )

    attrs = asset.attributes
    assert attrs.connection_qualified_name == "default/custom/dxr-datasource-finance"
    assert attrs.connection_name == "dxr-datasource-finance"
    assert attrs.file_type == FileType.PDF
    assert attrs.file_path == (
        "Documents/Confidential Folder/2011_audited_financial_statement_msword.doc"
    )
    assert attrs.source_url == "https://dxr.example.com/files/0KQUMpgBVo-c9i0dUnJ8"
    assert set(attrs.owner_users or []) == {"sarah@ohalo.onmicrosoft.com"}

    tag_names = set(attrs.asset_tags or [])
    expected = {
        "DXR :: Annotator :: Sales Data",
        "DXR :: Annotator :: Credit card",
        "DXR :: Annotator Domain :: Financially Sensitive",
        "DXR :: Extractor :: Contract Type",
        "DXR :: DLP :: PURVIEW :: Confidential",
        "DXR :: Entitlement :: User :: sarah@ohalo.onmicrosoft.com",
        "DXR :: Metadata :: Contract Type = Annual Service Agreement",
        "DXR :: Category :: Finance",
        "DXR :: Category :: Audit",
    }
    missing = expected - tag_names
    assert not missing, (tag_names, missing)

    atlan_tag_names = {
        str(tag.type_name)
        for tag in asset.atlan_tags or []
        if getattr(tag, "type_name", None)
    }
    assert classification_handle.type_name in atlan_tag_names
    assert any("credit-card" in name for name in atlan_tag_names)
    assert any("financially-sensitive" in name for name in atlan_tag_names)
    assert any("contract-type" in name for name in atlan_tag_names)
    registry_type_names = {handle.type_name for handle in tag_registry._handles.values()}  # type: ignore[attr-defined]
    assert atlan_tag_names <= registry_type_names


def test_factory_defaults_to_txt_without_type():
    _, factory = _build_factory()
    payload = {
        "fileId": "abc123",
        "fileName": "readme",
    }
    asset = factory.build(
        payload,
        connection_qualified_name="default/custom/dxr-datasource-sample",
        connection_name="dxr-datasource-sample",
        classification_tags={},
    )
    assert asset.attributes.file_type == FileType.TXT
