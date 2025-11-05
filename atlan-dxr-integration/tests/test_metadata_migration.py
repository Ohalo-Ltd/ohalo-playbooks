"""Tests for legacy metadata migration helpers."""

from __future__ import annotations

from types import SimpleNamespace

from atlan_dxr_integration.metadata_migration import (
    build_business_attributes,
    parse_legacy_tags,
)


class _StubCustomMetadataCache:
    def __init__(self):
        self.map_name_to_id = {
            "DXR File Metadata": "cm_dxr_file",
        }
        self.map_attr_id_to_name = {
            "cm_dxr_file": {
                "attr_dlp": "DLP Labels",
                "attr_categories": "Categories",
            }
        }
        self.map_attr_name_to_id = {
            set_id: {name: attr_id for attr_id, name in attrs.items()}
            for set_id, attrs in self.map_attr_id_to_name.items()
        }

    def refresh_cache(self):
        return None

    def get_id_for_name(self, name: str) -> str:
        return self.map_name_to_id[name]

    def get_attr_id_for_name(self, set_name: str, attr_name: str) -> str:
        set_id = self.get_id_for_name(set_name)
        return self.map_attr_name_to_id[set_id][attr_name]

    def is_attr_archived(self, attr_id: str) -> bool:
        return False


class _StubAtlanClient:
    def __init__(self):
        self.custom_metadata_cache = _StubCustomMetadataCache()
        self.enum_cache = SimpleNamespace(get_by_name=lambda _name: None)


class _StubRESTClient:
    def __init__(self):
        self._typedefs = {
            "hash_dlp": {
                "classificationDefs": [
                    {"displayName": "DXR :: DLP :: PURVIEW :: Confidential"}
                ]
            },
            "hash_category": {
                "classificationDefs": [
                    {"displayName": "DXR :: Category :: Finance"}
                ]
            },
            "hash_label": {
                "classificationDefs": [
                    {"displayName": "DXR :: Annotator :: Sensitive"}
                ]
            },
        }
        self.atlan_client = _StubAtlanClient()

    def get_typedef(self, name: str):
        return self._typedefs.get(name)


def test_parse_legacy_tags_extracts_metadata():
    rest = _StubRESTClient()
    entity = {
        "typeName": "File",
        "guid": "file-guid",
        "attributes": {
            "assetTags": [
                "DXR :: DLP :: PURVIEW :: Confidential",
                "DXR :: Category :: Finance",
                "DXR :: Annotator :: Sensitive",
            ]
        },
        "classifications": [
            {"typeName": "hash_dlp"},
            {"typeName": "hash_category"},
            {"typeName": "hash_label"},
        ],
    }

    metadata, tags, classifications = parse_legacy_tags(entity, rest_client=rest)

    assert metadata == {
        "DXR File Metadata": {
            "Annotators": ["Sensitive"],
            "Categories": ["Finance"],
            "DLP Labels": ["PURVIEW :: Confidential"],
        }
    }
    assert tags == ["DXR :: Annotator :: Sensitive"]
    assert len(classifications) == 1
    assert classifications[0]["typeName"] == "hash_label"


def test_build_business_attributes_resolves_hashed_ids():
    rest = _StubRESTClient()
    metadata = {
        "DXR File Metadata": {
            "Categories": ["Finance"],
            "DLP Labels": ["PURVIEW :: Confidential"],
        }
    }

    business = build_business_attributes(rest, metadata)

    set_id = rest.atlan_client.custom_metadata_cache.get_id_for_name("DXR File Metadata")
    dlp_attr = rest.atlan_client.custom_metadata_cache.get_attr_id_for_name(
        "DXR File Metadata",
        "DLP Labels",
    )
    assert business[set_id][dlp_attr] == ["PURVIEW :: Confidential"]
