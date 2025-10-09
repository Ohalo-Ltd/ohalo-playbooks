"""Tests for the DXR → Atlan file asset factory."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from atlan_dxr_integration.atlan_service import AtlanRequestError
from atlan_dxr_integration.atlan_types import TagColor
from atlan_dxr_integration.file_asset_builder import BuiltFileAsset, FileAssetFactory
from atlan_dxr_integration.tag_registry import TagRegistry


class _StubRESTClient:
    def __init__(self):
        self.typedefs: dict[str, dict] = {}

    def create_typedefs(self, payload):
        for typedef in payload.get("classificationDefs", []):
            name = typedef["name"]
            self.typedefs[name] = typedef | {"name": name}

    def get_typedef(self, name: str):
        typedef = self.typedefs.get(name)
        if not typedef:
            raise AtlanRequestError("not found", status_code=404, details=None)
        return {"classificationDefs": [typedef]}


def _build_registry() -> TagRegistry:
    client = _StubRESTClient()
    return TagRegistry(client, namespace="DXR"), client


def _sample_payload() -> dict:
    return {
        "datasource": {
            "id": "c3d4e5f6-a7b8-4012-8def-345678901234",
            "name": "Finance Department SharePoint",
            "connector": {"type": "BOX", "userId": "user-12345"},
        },
        "fileName": "2011_audited_financial_statement_msword.doc",
        "fileId": "0KQUMpgBVo-c9i0dUnJ8",
        "path": "Documents/Confidential Folder/2011_audited_financial_statement_msword.doc",
        "mimeType": "application/pdf",
        "labels": [
            {"id": "a1", "name": "Sales Data"},
        ],
        "annotators": [
            {"id": "b2", "name": "Credit card", "domain": {"id": "c3", "name": "Financially Sensitive"}},
        ],
        "dlpLabels": [
            {"name": "Confidential", "dlpSystem": "PURVIEW"},
        ],
        "extractedMetadata": [
            {"id": "d4", "name": "Contract Type", "value": "Annual Service Agreement"},
        ],
        "entitlements": {
            "whoCanAccess": [
                {"accountType": "USER", "name": "Sarah Conner", "email": "sarah@example.com"}
            ]
        },
        "categories": ["Finance", {"name": "Audit"}],
        "owner": {"name": "Sarah Conner", "email": "sarah@example.com"},
    }


def test_factory_builds_enriched_file_asset():
    registry, client = _build_registry()
    factory = FileAssetFactory(tag_registry=registry, tag_namespace="DXR", dxr_base_url="https://dxr.example.com/api")

    classification_handle = registry.ensure(
        slug_parts=["classification", "annotator", "sales-data"],
        display_name="Annotator :: Sales Data",
        color=TagColor.YELLOW,
    )

    built = factory.build(
        _sample_payload(),
        connection_qualified_name="default/custom/dxr-datasource-finance",
        connection_name="dxr-datasource-finance",
        classification_tags={"a1": classification_handle},
    )

    assert isinstance(built, BuiltFileAsset)

    asset = built.asset
    attrs = asset["attributes"]
    assert attrs["connectionQualifiedName"] == "default/custom/dxr-datasource-finance"
    assert attrs["connectionName"] == "dxr-datasource-finance"
    assert attrs["fileType"] == "PDF"
    assert attrs["filePath"].startswith("Documents/Confidential Folder")
    assert attrs["sourceURL"] == "https://dxr.example.com/files/0KQUMpgBVo-c9i0dUnJ8"

    tag_names = set(attrs["assetTags"])
    assert "DXR :: Annotator :: Sales Data" in tag_names
    assert "DXR :: DLP :: PURVIEW :: Confidential" in tag_names

    classifications = asset.get("classifications", [])
    assert any(c.get("typeName") == classification_handle.hashed_name for c in classifications)


def test_factory_defaults_to_txt_without_type():
    registry, _ = _build_registry()
    factory = FileAssetFactory(tag_registry=registry, tag_namespace="DXR")

    payload = {"fileId": "abc123", "fileName": "readme"}
    built = factory.build(
        payload,
        connection_qualified_name="default/custom/dxr-datasource-test",
        connection_name="dxr-datasource-test",
        classification_tags={},
    )

    attrs = built.asset["attributes"]
    assert attrs["fileType"] == "TXT"
