"""Unit tests for the Atlan uploader helpers."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from atlan_dxr_integration import atlan_uploader
from pyatlan.errors import ErrorCode, NotFoundError
from pyatlan.model.assets.connection import Connection


class _FailingBatchAsset:
    def __init__(self, exception: Exception) -> None:
        self._exception = exception

    def save(self, batch):  # pragma: no cover - exercised via uploader
        raise self._exception


class _FailingBatchClient:
    def __init__(self, exception: Exception) -> None:
        self.asset = _FailingBatchAsset(exception)


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


class _FakeConnectionAsset:
    def __init__(self, *, exists: bool = True, save_exception: Exception | None = None) -> None:
        self.exists = exists
        self.save_exception = save_exception
        self.lookups: list = []
        self.saved_assets: list = []

    def get_by_qualified_name(self, qualified_name, asset_type, **_):
        self.lookups.append((qualified_name, asset_type))
        if not self.exists:
            raise NotFoundError(
                ErrorCode.ASSET_NOT_FOUND_BY_QN,
                asset_type.__name__,
                qualified_name,
            )
        return asset_type()

    def save(self, asset):
        if self.save_exception:
            raise self.save_exception
        self.saved_assets.append(asset)
        return SimpleNamespace(
            request_id="req-conn",
            assets_created=lambda asset_type: [asset] if asset_type is Connection else [],
        )


class _FakeConnectionClient:
    def __init__(self, asset: _FakeConnectionAsset) -> None:
        self.asset = asset
        self.role_cache = _FakeRoleCache()
        self.user_cache = _FakeUserCache()
        self.group_cache = _FakeGroupCache()


def _build_config() -> SimpleNamespace:
    return SimpleNamespace(
        atlan_base_url="https://atlan.example.com",
        atlan_api_token="token",
        atlan_connection_name="dxr-connection",
        atlan_connection_qualified_name="default/connection",
        atlan_connector_name="custom-connector",
        atlan_batch_size=10,
        qualified_name_prefix="default/connection",
        atlan_dataset_path_prefix="dxr",
    )


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
    monkeypatch.setattr(atlan_uploader, "AtlanClient", _fake_client_factory)
    uploader = atlan_uploader.AtlanUploader(_build_config())

    with pytest.raises(atlan_uploader.AtlanUploadError) as exc_info:
        uploader._save_batch([object()])

    message = str(exc_info.value)
    assert "Atlan" in message
    if isinstance(exception, atlan_uploader.AtlanPermissionError):
        assert "permissions" in message.lower()


def test_ensure_connection_exists_skips_when_present(monkeypatch: pytest.MonkeyPatch) -> None:
    asset_client = _FakeConnectionAsset(exists=True)
    fake_client = _FakeConnectionClient(asset_client)
    monkeypatch.setattr(atlan_uploader, "AtlanClient", lambda *args, **kwargs: fake_client)

    uploader = atlan_uploader.AtlanUploader(_build_config())

    assert asset_client.lookups == [("default/connection", Connection)]
    assert asset_client.saved_assets == []
    assert uploader._connector_type == atlan_uploader.AtlanConnectorType.CUSTOM
    assert uploader._connection_qualified_name == "default/connection"
    assert uploader._qualified_name_prefix == "default/connection/dxr"


def test_ensure_connection_exists_creates_when_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    asset_client = _FakeConnectionAsset(exists=False)
    fake_client = _FakeConnectionClient(asset_client)
    monkeypatch.setattr(atlan_uploader, "AtlanClient", lambda *args, **kwargs: fake_client)

    config = _build_config()
    uploader = atlan_uploader.AtlanUploader(config)

    assert uploader is not None
    assert asset_client.saved_assets, "Expected connection to be created"
    created = asset_client.saved_assets[0]
    assert isinstance(created, Connection)
    assert created.attributes.name == config.atlan_connection_name
    assert created.attributes.qualified_name == config.atlan_connection_qualified_name
    assert (
        uploader._connector_type == atlan_uploader.AtlanConnectorType.CUSTOM
    )
    assert uploader._connection_qualified_name == config.atlan_connection_qualified_name
    assert uploader._qualified_name_prefix == "default/connection/dxr"


def test_ensure_connection_exists_raises_on_permission_error(monkeypatch: pytest.MonkeyPatch) -> None:
    permission_error = atlan_uploader.AtlanPermissionError(
        ErrorCode.PERMISSION_PASSTHROUGH,
        "ATLAS-403-00-001",
        "not authorized",
        "permission denied",
    )
    asset_client = _FakeConnectionAsset(exists=False, save_exception=permission_error)
    fake_client = _FakeConnectionClient(asset_client)
    monkeypatch.setattr(atlan_uploader, "AtlanClient", lambda *args, **kwargs: fake_client)

    with pytest.raises(atlan_uploader.AtlanUploadError) as exc_info:
        atlan_uploader.AtlanUploader(_build_config())

    assert "connection" in str(exc_info.value).lower()
