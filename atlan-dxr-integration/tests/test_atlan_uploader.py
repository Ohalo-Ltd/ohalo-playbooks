"""Unit tests for the Atlan uploader."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from pyatlan.errors import ErrorCode
from pyatlan.model.assets.core.file import File
from pyatlan.model.assets.core.table import Table
from pyatlan.model.enums import AtlanConnectorType, FileType

from atlan_dxr_integration import atlan_uploader
from atlan_dxr_integration.connection_utils import ConnectionHandle


class _FakeMutationResponse:
    def __init__(self, *, request_id: str = "req", created=None, updated=None, partial=None):
        self.request_id = request_id
        self._created = created or []
        self._updated = updated or []
        self._partial = partial or []

    def assets_created(self, asset_type):
        return [asset for asset in self._created if isinstance(asset, asset_type)]

    def assets_updated(self, asset_type):
        return [asset for asset in self._updated if isinstance(asset, asset_type)]

    def assets_partially_updated(self, asset_type):
        return [asset for asset in self._partial if isinstance(asset, asset_type)]


class _FakeAssetClient:
    def __init__(self, responses=None, lookups=None):
        self._responses = list(responses or [])
        self.saved_batches = []
        self.lookups = lookups or {}

    def save(self, batch):
        self.saved_batches.append(list(batch))
        if self._responses:
            response = self._responses.pop(0)
            if isinstance(response, Exception):
                raise response
            return response
        return _FakeMutationResponse(created=list(batch))

    def get_by_qualified_name(self, qualified_name, asset_type, **kwargs):
        value = self.lookups.get((qualified_name, asset_type))
        if isinstance(value, Exception):
            raise value
        if value is None:
            raise ErrorCode.ASSET_NOT_FOUND_BY_QN.exception_with_parameters(
                asset_type.__name__, qualified_name
            )
        return value


class _FakeClient:
    def __init__(self, *, responses=None, lookups=None):
        self.asset = _FakeAssetClient(responses=responses, lookups=lookups)


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

    @property
    def qualified_name_prefix(self) -> str:
        return f"{self.schema_qualified_name}/{self.atlan_dataset_path_prefix}"

    @property
    def schema_qualified_name(self) -> str:
        return f"{self.database_qualified_name}/{self.atlan_schema_name}"

    @property
    def database_qualified_name(self) -> str:
        return f"{self.atlan_global_connection_qualified_name}/{self.atlan_database_name}"


class _FakeProvisioner:
    def __init__(self, client):
        self.client = client
        self.ensure_connection_calls = 0

    def ensure_connection(self, **_):
        self.ensure_connection_calls += 1
        connection = SimpleNamespace(attributes=SimpleNamespace(name="dxr-unstructured-attributes"))
        return ConnectionHandle(
            connector_type=AtlanConnectorType.CUSTOM,
            qualified_name="default/custom/dxr-unstructured-attributes",
            connection=connection,
        )

    def ensure_database(self, *, qualified_name, **_):
        return qualified_name

    def ensure_schema(self, *, qualified_name, **_):
        return qualified_name


def _build_uploader(responses=None, lookups=None):
    fake_client = _FakeClient(responses=responses, lookups=lookups)

    class _StubProvisioner(_FakeProvisioner):
        def __init__(self, client):
            super().__init__(client)

    uploader_module = atlan_uploader
    original_provisioner = uploader_module.ConnectionProvisioner
    uploader_module.ConnectionProvisioner = _StubProvisioner  # type: ignore

    try:
        uploader = uploader_module.AtlanUploader(_TestConfig())
    finally:
        uploader_module.ConnectionProvisioner = original_provisioner  # type: ignore
    uploader._client = fake_client  # type: ignore[attr-defined]
    uploader._provisioner = _StubProvisioner(fake_client)
    return uploader, uploader._provisioner  # type: ignore[attr-defined]


@pytest.mark.parametrize(
    "exception",
    [
        atlan_uploader.AtlanPermissionError(
            ErrorCode.PERMISSION_PASSTHROUGH,
            "ATLAS-403-00-001",
            "not authorized",
            "permission denied",
        ),
        atlan_uploader.AtlanError(ErrorCode.CONNECTION_ERROR, "socket timeout"),
    ],
)
def test_save_assets_wraps_atlan_errors(exception) -> None:
    uploader, _ = _build_uploader(responses=[exception])

    with pytest.raises(atlan_uploader.AtlanUploadError) as exc_info:
        uploader._save_assets([object()], Table, "tables", "table")

    assert "Atlan rejected" in str(exc_info.value)


def test_save_assets_no_mutation_success_when_assets_exist():
    response = _FakeMutationResponse(created=[], updated=[], partial=[])
    uploader, _ = _build_uploader(responses=[response], lookups={
        ("default/custom/dxr-unstructured-attributes/dxr/labels/sample", Table):
            SimpleNamespace(attributes=SimpleNamespace(qualified_name="default/custom/dxr-unstructured-attributes/dxr/labels/sample"))
    })

    table = Table.creator(
        name="sample",
        schema_qualified_name="default/custom/dxr-unstructured-attributes/dxr/labels",
        database_name="dxr",
        database_qualified_name="default/custom/dxr-unstructured-attributes/dxr",
        schema_name="labels",
        connection_qualified_name="default/custom/dxr-unstructured-attributes",
    )
    table.attributes.qualified_name = (
        "default/custom/dxr-unstructured-attributes/dxr/labels/sample"
    )

    uploader._save_assets([table], Table, "tables", "table")


def test_save_assets_no_mutation_raises_when_missing():
    response = _FakeMutationResponse(created=[], updated=[], partial=[])
    uploader, _ = _build_uploader(responses=[response])

    table = Table.creator(
        name="sample",
        schema_qualified_name="default/custom/dxr-unstructured-attributes/dxr/labels",
        database_name="dxr",
        database_qualified_name="default/custom/dxr-unstructured-attributes/dxr",
        schema_name="labels",
        connection_qualified_name="default/custom/dxr-unstructured-attributes",
    )
    table.attributes.qualified_name = (
        "default/custom/dxr-unstructured-attributes/dxr/labels/sample"
    )

    with pytest.raises(atlan_uploader.AtlanUploadError):
        uploader._save_assets([table], Table, "tables", "table")


def test_upsert_invokes_batch_save(monkeypatch: pytest.MonkeyPatch) -> None:
    uploader, _ = _build_uploader()
    calls = []

    def _capture(batch, *_args, **_kwargs):
        calls.append(list(batch))

    monkeypatch.setattr(uploader, "_save_assets", _capture)

    record = SimpleNamespace(
        identifier="classification-1",
        name="Sample",
        description="desc",
        classification=SimpleNamespace(description="global desc"),
        source_url="https://dxr",
    )
    uploader.upsert([record])

    assert calls, "Expected tables to be saved"
    table = calls[0][0]
    assert isinstance(table, Table)
    assert table.attributes.connection_name == "dxr-unstructured-attributes"


def test_upsert_files_sets_metadata():
    uploader, _ = _build_uploader()
    file_asset = File.creator(
        name="invoice.pdf",
        connection_qualified_name=uploader.connection_qualified_name,
        file_type=FileType.PDF,
    )
    file_asset.attributes.qualified_name = (
        f"{uploader.connection_qualified_name}/file-123"
    )

    captured = []

    def _capture(batch, *_args, **_kwargs):
        captured.append(list(batch))

    uploader._save_assets = _capture  # type: ignore
    uploader.upsert_files([file_asset])

    saved_file = captured[0][0]
    attrs = saved_file.attributes
    assert attrs.connection_name == "dxr-unstructured-attributes"
    assert attrs.connector_name == AtlanConnectorType.CUSTOM.value
    assert attrs.qualified_name == (
        f"{uploader.connection_qualified_name}/file-123"
    )
