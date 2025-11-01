"""Unit tests for the Atlan uploader."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from atlan_dxr_integration import atlan_uploader
from atlan_dxr_integration.atlan_service import AtlanRequestError
from atlan_dxr_integration.connection_utils import ConnectionHandle
from atlan_dxr_integration.dataset_builder import DatasetRecord
from atlan_dxr_integration.dxr_client import Classification


class _StubRESTClient:
    def __init__(self, *, responses=None, existing=None):
        self.responses = list(responses or [])
        self.existing = existing or {}
        self.calls: list[list[dict]] = []
        self.deleted: list[str] = []
        self.purged: list[str] = []

    def upsert_assets(self, assets):
        self.calls.append(assets)
        if self.responses:
            response = self.responses.pop(0)
            if isinstance(response, Exception):
                raise response
            return response
        return {"mutatedEntities": {"CREATE": assets}}

    def get_asset(self, type_name: str, qualified_name: str):
        return self.existing.get((type_name, qualified_name))

    def delete_asset(self, guid: str) -> None:
        self.deleted.append(guid)

    def purge_asset(self, guid: str) -> None:
        self.purged.append(guid)

    def search_assets(self, _payload):  # pragma: no cover - not used in tests
        return {"entities": []}


class _StubProvisioner:
    def __init__(self, *_args, **_kwargs):
        connection = SimpleNamespace(attributes=SimpleNamespace(name="dxr-unstructured-attributes"))
        self.handle = ConnectionHandle(
            connector_name="custom-connector",
            qualified_name="default/custom/dxr-unstructured-attributes",
            connection=connection,
        )

    def ensure_connection(self, **_kwargs):
        return self.handle

    def ensure_database(self, *, qualified_name: str, **_kwargs) -> str:
        return qualified_name

    def ensure_schema(self, *, qualified_name: str, **_kwargs) -> str:
        return qualified_name


class _TestConfig:
    atlan_base_url = "https://atlan.example.com"
    atlan_api_token = "token"
    atlan_global_connection_name = "dxr-unstructured-attributes"
    atlan_global_connection_qualified_name = "default/custom/dxr-unstructured-attributes"
    atlan_global_connector_name = "custom-connector"
    atlan_global_domain_name = "DXR Unstructured"
    atlan_datasource_connection_prefix = "dxr-datasource"
    atlan_datasource_domain_prefix = "DXR"
    atlan_database_name = "dxr"
    atlan_schema_name = "labels"
    atlan_dataset_path_prefix = "dxr"
    atlan_batch_size = 10
    atlan_tag_namespace = "DXR"
    atlan_connection_admin_user = None

    @property
    def database_qualified_name(self) -> str:
        return f"{self.atlan_global_connection_qualified_name}/{self.atlan_database_name}"

    @property
    def schema_qualified_name(self) -> str:
        return f"{self.database_qualified_name}/{self.atlan_schema_name}"


@pytest.fixture
def uploader(monkeypatch: pytest.MonkeyPatch):
    rest = _StubRESTClient()

    monkeypatch.setattr(atlan_uploader, "AtlanRESTClient", lambda **_: rest)
    monkeypatch.setattr(atlan_uploader, "ConnectionProvisioner", _StubProvisioner)

    instance = atlan_uploader.AtlanUploader(_TestConfig())
    instance._client = rest  # type: ignore[attr-defined]
    return instance, rest


def _build_record(identifier: str = "cls-1") -> DatasetRecord:
    return DatasetRecord(
        classification=Classification(
            identifier=identifier,
            name="Sensitive",
            type="ANNOTATOR",
            subtype=None,
            description="desc",
            link=None,
            search_link=None,
        ),
        file_count=1,
        sample_files=[],
        description="local desc",
        source_url="https://dxr/file",
    )


def test_upsert_builds_table_payload(uploader):
    instance, rest = uploader
    instance.upsert([_build_record()])

    assert rest.calls, "Expected an upsert call"
    entity = rest.calls[0][0]
    assert entity["typeName"] == "Table"
    attrs = entity["attributes"]
    assert attrs["qualifiedName"].endswith("/cls-1")
    assert attrs["connectionName"] == "dxr-unstructured-attributes"
    assert attrs["schemaName"] == "labels"


def test_upsert_handles_existing_assets(uploader):
    instance, rest = uploader
    rest.responses.append({"mutatedEntities": {}})
    rest.existing[("Table", "default/custom/dxr-unstructured-attributes/dxr/labels/cls-1")] = {
        "attributes": {"qualifiedName": "default/custom/dxr-unstructured-attributes/dxr/labels/cls-1"}
    }

    instance.upsert([_build_record()])


def test_upsert_raises_on_failure(uploader):
    instance, rest = uploader
    rest.responses.append(AtlanRequestError("boom", status_code=500, details={}))

    with pytest.raises(atlan_uploader.AtlanUploadError):
        instance.upsert([_build_record()])


def test_upsert_files_delegates_to_rest(uploader):
    instance, rest = uploader

    asset = {
        "typeName": "File",
        "attributes": {
            "qualifiedName": "default/custom/dxr-unstructured-attributes/file-1",
            "name": "file-1",
            "displayName": "file-1",
        },
    }

    instance.upsert_files([asset])

    assert rest.calls, "Expected file upsert call"
    entity = rest.calls[-1][0]
    assert entity["typeName"] == "File"
