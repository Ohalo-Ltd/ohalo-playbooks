"""Unit tests for the Atlan uploader helpers."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Optional

import pytest

from atlan_dxr_integration import atlan_uploader
from atlan_dxr_integration.dataset_builder import DatasetFile, DatasetRecord
from atlan_dxr_integration.dxr_client import Classification
from pyatlan.errors import ErrorCode, NotFoundError
from pyatlan.model.assets.connection import Connection
from pyatlan.model.assets.core.database import Database
from pyatlan.model.assets.core.file import File
from pyatlan.model.assets.core.schema import Schema
from pyatlan.model.assets.core.table import Table
from pyatlan.model.enums import FileType


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


class _FakeTagCache:
    def __init__(self, existing: Optional[set[str]] = None) -> None:
        self.existing: set[str] = existing or set()

    def get_id_for_name(self, name: str) -> Optional[str]:
        return f"{name}-id" if name in self.existing else None

    def refresh_cache(self) -> None:  # pragma: no cover - not needed in tests
        return None


class _FakeTypeDefClient:
    def __init__(self, tag_cache: _FakeTagCache) -> None:
        self.tag_cache = tag_cache
        self.created: list[str] = []

    def create(self, typedef):
        self.created.append(typedef.display_name)
        self.tag_cache.existing.add(typedef.display_name)
        return SimpleNamespace()


class _FailingBatchAsset:
    def __init__(self, exception: Exception) -> None:
        self._exception = exception

    def save(self, batch):  # pragma: no cover - exercised via uploader
        raise self._exception


class _FailingBatchClient:
    def __init__(self, exception: Exception) -> None:
        self.asset = _FailingBatchAsset(exception)
        self.atlan_tag_cache = _FakeTagCache()
        self.typedef = _FakeTypeDefClient(self.atlan_tag_cache)


class _SuccessfulBatchClient:
    def __init__(self, response: _FakeMutationResponse, *, lookups: dict[str, object]) -> None:
        def _get(qualified_name, asset_type, **kwargs):
            value = lookups.get(qualified_name)
            if isinstance(value, Exception):
                raise value
            return value

        self.asset = SimpleNamespace(
            save=lambda batch: response,
            get_by_qualified_name=_get,
        )
        self.atlan_tag_cache = _FakeTagCache()
        self.typedef = _FakeTypeDefClient(self.atlan_tag_cache)


class _FakeRoleCache:
    def __init__(self, guid: str = "role-guid") -> None:
        self.guid = guid
        self.requested: list = []
        self.validated: list = []

    def get_id_for_name(self, name: str) -> str:
        self.requested.append(name)
        return self.guid

    def validate_idstrs(self, idstrs):
        self.validated = list(idstrs)


class _FakeUserCache:
    def __init__(self) -> None:
        self.validated: list = []

    def validate_names(self, names):
        self.validated = list(names)


class _FakeGroupCache:
    def __init__(self) -> None:
        self.validated: list = []

    def validate_aliases(self, aliases):
        self.validated = list(aliases)


class _FakeAssetService:
    def __init__(
        self,
        *,
        lookups: dict[tuple[str, type], object] | None = None,
        save_responses: list[object] | None = None,
    ) -> None:
        self.lookups = lookups or {}
        self.save_responses = list(save_responses or [])
        self.lookup_calls: list[tuple[str, type]] = []
        self.saved_assets: list[object] = []

    def get_by_qualified_name(self, qualified_name, asset_type, **_):
        self.lookup_calls.append((qualified_name, asset_type))
        key = (qualified_name, asset_type)
        if key in self.lookups:
            value = self.lookups[key]
            if isinstance(value, Exception):
                raise value
            return value
        raise NotFoundError(
            ErrorCode.ASSET_NOT_FOUND_BY_QN,
            asset_type.__name__,
            qualified_name,
        )

    def save(self, asset):
        response = (
            self.save_responses.pop(0)
            if self.save_responses
            else _FakeMutationResponse(request_id="req", created=[asset])
        )
        if isinstance(response, Exception):
            raise response
        if isinstance(asset, (list, tuple)):
            self.saved_assets.extend(asset)
        else:
            self.saved_assets.append(asset)
        return response


class _FakeAtlanClient:
    def __init__(
        self,
        *,
        lookups: dict[tuple[str, type], object] | None = None,
        save_responses: list[object] | None = None,
    ) -> None:
        self.asset = _FakeAssetService(lookups=lookups, save_responses=save_responses)
        self.role_cache = _FakeRoleCache()
        self.user_cache = _FakeUserCache()
        self.group_cache = _FakeGroupCache()
        self.atlan_tag_cache = _FakeTagCache()
        self.typedef = _FakeTypeDefClient(self.atlan_tag_cache)


class _TestConfig:
    def __init__(self) -> None:
        self.atlan_base_url = "https://atlan.example.com"
        self.atlan_api_token = "token"
        self.atlan_connection_name = "dxr-connection"
        self.atlan_connection_qualified_name = "default/custom/dxr-connection"
        self.atlan_connector_name = "custom-connector"
        self.atlan_database_name = "dxr"
        self.atlan_schema_name = "labels"
        self.atlan_dataset_path_prefix = "dxr"
        self.atlan_batch_size = 10
        self.dxr_file_fetch_limit = 200

    @property
    def qualified_name_prefix(self) -> str:
        return f"{self.schema_qualified_name}/{self.atlan_dataset_path_prefix}"

    @property
    def database_qualified_name(self) -> str:
        return f"{self.atlan_connection_qualified_name}/{self.atlan_database_name}"

    @property
    def schema_qualified_name(self) -> str:
        return f"{self.database_qualified_name}/{self.atlan_schema_name}"


def _build_config() -> _TestConfig:
    return _TestConfig()


@pytest.mark.parametrize(
    "exception_cls",
    [atlan_uploader.AtlanPermissionError, atlan_uploader.AtlanError],
)
def test_save_batch_wraps_atlan_errors(monkeypatch: pytest.MonkeyPatch, exception_cls) -> None:
    """Uploader re-raises Atlan errors as AtlanUploadError with guidance."""

    if exception_cls is atlan_uploader.AtlanPermissionError:
        exception = exception_cls(
            ErrorCode.PERMISSION_PASSTHROUGH,
            "ATLAS-403-00-001",
            "not authorized",
            "permission denied",
        )
    else:
        exception = exception_cls(ErrorCode.CONNECTION_ERROR, "socket timeout")

    def _fake_client_factory(*args, **kwargs):
        return _FailingBatchClient(exception)

    monkeypatch.setattr(
        atlan_uploader.AtlanUploader,
        "_ensure_connection_exists",
        lambda self: (
            atlan_uploader.AtlanConnectorType.CUSTOM,
            "default/connection",
        ),
    )
    monkeypatch.setattr(
        atlan_uploader.AtlanUploader,
        "_ensure_database_exists",
        lambda self, _: "default/connection/dxr",
    )
    monkeypatch.setattr(
        atlan_uploader.AtlanUploader,
        "_ensure_schema_exists",
        lambda self, *__: "default/connection/dxr/labels",
    )
    monkeypatch.setattr(atlan_uploader, "AtlanClient", _fake_client_factory)
    config = _build_config()
    uploader = atlan_uploader.AtlanUploader(config)

    table = Table.creator(
        name="classification-1",
        schema_qualified_name=config.schema_qualified_name,
        schema_name=config.atlan_schema_name,
        database_name=config.atlan_database_name,
        database_qualified_name=config.database_qualified_name,
        connection_qualified_name=config.atlan_connection_qualified_name,
    )
    table.attributes.qualified_name = (
        f"{config.schema_qualified_name}/classification-1"
    )

    with pytest.raises(atlan_uploader.AtlanUploadError) as exc_info:
        uploader._save_batch([table])

    message = str(exc_info.value)
    assert "Atlan" in message
    if isinstance(exception, atlan_uploader.AtlanPermissionError):
        assert "permissions" in message.lower()


def test_ensure_connection_exists_skips_when_present(monkeypatch: pytest.MonkeyPatch) -> None:
    lookups = {
        ("default/custom/dxr-connection", Connection): SimpleNamespace(
            attributes=SimpleNamespace(
                qualified_name="default/custom/dxr-connection",
                connector_name="custom",
            )
        ),
        ("default/custom/dxr-connection/dxr", Database): SimpleNamespace(
            attributes=SimpleNamespace(
                qualified_name="default/custom/dxr-connection/dxr",
                connector_name="custom",
                connection_name="dxr-connection",
            )
        ),
        ("default/custom/dxr-connection/dxr/labels", Schema): SimpleNamespace(
            attributes=SimpleNamespace(
                qualified_name="default/custom/dxr-connection/dxr/labels",
                connector_name="custom",
                connection_name="dxr-connection",
            )
        ),
    }
    fake_client = _FakeAtlanClient(lookups=lookups)
    monkeypatch.setattr(atlan_uploader, "AtlanClient", lambda *args, **kwargs: fake_client)

    uploader = atlan_uploader.AtlanUploader(_build_config())

    assert fake_client.asset.lookup_calls == [
        ("default/custom/dxr-connection", Connection),
        ("default/custom/dxr-connection/dxr", Database),
        ("default/custom/dxr-connection/dxr/labels", Schema),
    ]
    assert fake_client.asset.saved_assets == []
    assert uploader._connector_type == atlan_uploader.AtlanConnectorType.CUSTOM
    assert uploader._connection_qualified_name == "default/custom/dxr-connection"
    assert uploader._schema_qualified_name == "default/custom/dxr-connection/dxr/labels"


def test_ensure_connection_exists_creates_when_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_client = _FakeAtlanClient(lookups={})
    monkeypatch.setattr(atlan_uploader, "AtlanClient", lambda *args, **kwargs: fake_client)

    config = _build_config()
    uploader = atlan_uploader.AtlanUploader(config)

    saved = fake_client.asset.saved_assets
    assert len(saved) == 3, "Expected connection, database, and schema to be created"
    assert isinstance(saved[0], Connection)
    assert saved[0].attributes.name == config.atlan_connection_name
    assert saved[0].attributes.qualified_name == config.atlan_connection_qualified_name

    assert isinstance(saved[1], Database)
    assert saved[1].attributes.qualified_name == config.database_qualified_name

    assert isinstance(saved[2], Schema)
    assert saved[2].attributes.qualified_name == config.schema_qualified_name

    assert uploader._connector_type == atlan_uploader.AtlanConnectorType.CUSTOM
    assert uploader._connection_qualified_name == config.atlan_connection_qualified_name
    assert uploader._schema_qualified_name == config.schema_qualified_name


def test_ensure_connection_exists_raises_on_permission_error(monkeypatch: pytest.MonkeyPatch) -> None:
    permission_error = atlan_uploader.AtlanPermissionError(
        ErrorCode.PERMISSION_PASSTHROUGH,
        "ATLAS-403-00-001",
        "not authorized",
        "permission denied",
    )
    fake_client = _FakeAtlanClient(lookups={}, save_responses=[permission_error])
    monkeypatch.setattr(atlan_uploader, "AtlanClient", lambda *args, **kwargs: fake_client)

    with pytest.raises(atlan_uploader.AtlanUploadError) as exc_info:
        atlan_uploader.AtlanUploader(_build_config())

    assert "connection" in str(exc_info.value).lower()


def test_save_batch_no_mutation_succeeds_when_assets_exist(monkeypatch: pytest.MonkeyPatch) -> None:
    """Uploader treats zero-mutation responses as success when assets already exist."""

    config = _build_config()
    qualified_name = f"{config.schema_qualified_name}/classification-1"
    response = _FakeMutationResponse(request_id="req-table", created=[], updated=[], partial=[])

    existing = SimpleNamespace(attributes=SimpleNamespace(qualified_name=qualified_name))

    monkeypatch.setattr(
        atlan_uploader.AtlanUploader,
        "_ensure_connection_exists",
        lambda self: (
            atlan_uploader.AtlanConnectorType.CUSTOM,
            config.atlan_connection_qualified_name,
        ),
    )
    monkeypatch.setattr(
        atlan_uploader.AtlanUploader,
        "_ensure_database_exists",
        lambda self, _: config.database_qualified_name,
    )
    monkeypatch.setattr(
        atlan_uploader.AtlanUploader,
        "_ensure_schema_exists",
        lambda self, *__: config.schema_qualified_name,
    )
    monkeypatch.setattr(
        atlan_uploader,
        "AtlanClient",
        lambda *args, **kwargs: _SuccessfulBatchClient(response, lookups={qualified_name: existing}),
    )

    uploader = atlan_uploader.AtlanUploader(config)
    table = Table.creator(
        name="classification-1",
        schema_qualified_name=config.schema_qualified_name,
        schema_name=config.atlan_schema_name,
        database_name=config.atlan_database_name,
        database_qualified_name=config.database_qualified_name,
        connection_qualified_name=config.atlan_connection_qualified_name,
    )
    table.attributes.qualified_name = qualified_name

    uploader._save_batch([table])


def test_save_batch_no_mutation_raises_when_assets_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    """Uploader errors if zero-mutation response and tables cannot be found."""

    config = _build_config()
    qualified_name = f"{config.schema_qualified_name}/classification-1"
    response = _FakeMutationResponse(request_id="req-table", created=[], updated=[], partial=[])

    error = NotFoundError(
        ErrorCode.ASSET_NOT_FOUND_BY_QN,
        "Table",
        qualified_name,
    )

    monkeypatch.setattr(
        atlan_uploader.AtlanUploader,
        "_ensure_connection_exists",
        lambda self: (
            atlan_uploader.AtlanConnectorType.CUSTOM,
            config.atlan_connection_qualified_name,
        ),
    )
    monkeypatch.setattr(
        atlan_uploader.AtlanUploader,
        "_ensure_database_exists",
        lambda self, _: config.database_qualified_name,
    )
    monkeypatch.setattr(
        atlan_uploader.AtlanUploader,
        "_ensure_schema_exists",
        lambda self, *__: config.schema_qualified_name,
    )
    monkeypatch.setattr(
        atlan_uploader,
        "AtlanClient",
        lambda *args, **kwargs: _SuccessfulBatchClient(response, lookups={qualified_name: error}),
    )

    uploader = atlan_uploader.AtlanUploader(config)
    table = Table.creator(
        name="classification-1",
        schema_qualified_name=config.schema_qualified_name,
        schema_name=config.atlan_schema_name,
        database_name=config.atlan_database_name,
        database_qualified_name=config.database_qualified_name,
        connection_qualified_name=config.atlan_connection_qualified_name,
    )
    table.attributes.qualified_name = qualified_name

    with pytest.raises(atlan_uploader.AtlanUploadError) as exc_info:
        uploader._save_batch([table])

    assert "did not mutate any assets" in str(exc_info.value)


def test_upsert_creates_file_assets(monkeypatch: pytest.MonkeyPatch) -> None:
    """Dataset files result in file assets alongside tables."""

    config = _build_config()
    classification = Classification(
        identifier="classification-1",
        name="Sensitive Data",
        type="ANNOTATOR",
        subtype="REGEX",
        description="Contains sensitive DXR-labelled content",
        link=None,
        search_link=None,
    )
    record = DatasetRecord(classification=classification, file_count=0)
    dataset_file = DatasetFile(
        identifier="file-1",
        name="patient-data.csv",
        path="s3://bucket/patient-data.csv",
        link="https://dxr.example.com/files/file-1",
        labels=["Sensitive Data"],
    )
    record.add_file(dataset_file)

    captured_batches: list[list[object]] = []

    class _CapturingAssetClient:
        def save(self, assets):
            batch = list(assets)
            captured_batches.append(batch)
            return _FakeMutationResponse(created=batch)

        def get_by_qualified_name(self, qualified_name, asset_type, **kwargs):
            return SimpleNamespace(attributes=SimpleNamespace(qualified_name=qualified_name))

    class _CapturingClient:
        def __init__(self):
            self.asset = _CapturingAssetClient()
            self.atlan_tag_cache = _FakeTagCache()
            self.typedef = _FakeTypeDefClient(self.atlan_tag_cache)

    monkeypatch.setattr(
        atlan_uploader.AtlanUploader,
        "_ensure_connection_exists",
        lambda self: (
            atlan_uploader.AtlanConnectorType.CUSTOM,
            config.atlan_connection_qualified_name,
        ),
    )
    monkeypatch.setattr(
        atlan_uploader.AtlanUploader,
        "_ensure_database_exists",
        lambda self, _: config.database_qualified_name,
    )
    monkeypatch.setattr(
        atlan_uploader.AtlanUploader,
        "_ensure_schema_exists",
        lambda self, *__: config.schema_qualified_name,
    )
    monkeypatch.setattr(
        atlan_uploader,
        "AtlanClient",
        lambda *args, **kwargs: _CapturingClient(),
    )

    uploader = atlan_uploader.AtlanUploader(config)
    uploader.upsert([record])

    assert captured_batches, "Expected at least one batch to be persisted"
    batch = captured_batches[0]
    tables = [asset for asset in batch if isinstance(asset, Table)]
    files = [asset for asset in batch if isinstance(asset, File)]
    assert len(tables) == 1
    assert len(files) == 1

    table = tables[0]
    file_asset = files[0]

    expected_table_qn = f"{config.schema_qualified_name}/classification-1"
    assert table.attributes.qualified_name == expected_table_qn
    table_tags = {str(tag.type_name) for tag in (table.atlan_tags or [])}
    assert table_tags == {"Sensitive Data", "ANNOTATOR", "REGEX"}

    expected_file_qn = uploader.file_qualified_name(record, dataset_file)
    assert file_asset.attributes.qualified_name == expected_file_qn
    assert file_asset.attributes.display_name == dataset_file.name
    assert file_asset.attributes.connection_name == config.atlan_connection_name
    assert file_asset.attributes.connector_name == "custom"
    assert file_asset.attributes.file_type == FileType.CSV
    file_tags = [str(tag.type_name) for tag in (file_asset.atlan_tags or [])]
    assert file_tags == ["Sensitive Data"]
