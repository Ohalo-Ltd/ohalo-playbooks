"""Tests for the DXR → Atlan file asset transformer."""

from __future__ import annotations

from atlan_dxr_integration.file_asset_builder import FileAssetBuilder
from pyatlan.model.enums import FileType


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


def test_builder_converts_payload_into_file_asset():
    builder = FileAssetBuilder(
        connection_qualified_name="default/custom/dxr-connection",
        connection_name="dxr-connection",
        dxr_base_url="https://dxr.example.com/api",
    )

    payload = _sample_payload()
    builder.consume(payload)
    builder.consume(payload)  # dedupe repeated payloads

    assets = builder.build()
    assert len(assets) == 1

    file_asset = assets[0]
    attrs = file_asset.attributes

    assert attrs.connection_qualified_name == "default/custom/dxr-connection"
    assert attrs.connection_name == "dxr-connection"
    assert attrs.qualified_name.endswith("/0KQUMpgBVo-c9i0dUnJ8")
    assert attrs.display_name == payload["fileName"]
    assert attrs.file_path == payload["path"]
    assert attrs.file_type == FileType.PDF
    assert set(attrs.owner_users or []) == {"sarah@ohalo.onmicrosoft.com"}
    assert "Finance Department SharePoint" in attrs.description
    assert "Sales Data" in attrs.description
    assert attrs.user_description == attrs.description
    assert attrs.source_url == "https://dxr.example.com/files/0KQUMpgBVo-c9i0dUnJ8"
    expected_tags = {
        "dxr-label:sales-data",
        "dlp:purview-confidential",
        "annotator:credit-card",
        "annotator-domain:financially-sensitive",
        "entitlement:user-sarah-ohalo-onmicrosoft-com",
        "metadata:contract-type-annual-service-agreement",
        "category:finance",
        "category:audit",
    }
    assert set(attrs.asset_tags or []) == expected_tags


def test_builder_falls_back_to_txt_when_no_type():
    builder = FileAssetBuilder(
        connection_qualified_name="default/custom/dxr-connection",
        connection_name="dxr-connection",
    )
    payload = {
        "fileId": "abc123",
        "fileName": "readme",
    }
    builder.consume(payload)
    asset = builder.build()[0]
    assert asset.attributes.file_type == FileType.TXT
