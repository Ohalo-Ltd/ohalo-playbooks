"""Compatibility wrapper that routes legacy REST-style helpers through the Atlan Application SDK."""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional

from http import HTTPStatus

import pyatlan.model.assets as asset_models
from application_sdk.clients.atlan import get_client as get_atlan_client
from application_sdk.constants import ATLAN_API_KEY, ATLAN_BASE_URL
from application_sdk.observability.logger_adaptor import get_logger
from pydantic.v1 import ValidationError
from pyatlan.client.atlan import AtlanClient
from pyatlan.errors import AtlanError, NotFoundError
from pyatlan.utils import API, EndPoint, HTTPMethod
from pyatlan.model.assets.core.asset import Asset as AtlanAsset
from pyatlan.model.enums import (
    AtlanDeleteType,
    AtlanTagColor,
    AtlanIcon,
    AtlanTypeCategory,
)
from pyatlan.model.response import AssetMutationResponse
from pyatlan.model.search import IndexSearchRequest
from pyatlan.model.typedef import AtlanTagDef

logger = get_logger(__name__)


class AtlanRequestError(RuntimeError):
    """Raised when the Atlan API reports an error."""

    def __init__(self, message: str, *, status_code: int, details: Any | None = None):
        super().__init__(message)
        self.status_code = status_code
        self.details = details


class AtlanRESTClient:
    """Shim that preserves the previous helper surface while delegating to the official SDK."""

    def __init__(
        self,
        *,
        base_url: str | None = None,
        api_key: str | None = None,
        timeout: float = 120.0,  # noqa: ARG002 - retained for backwards compatibility
    ) -> None:
        resolved_base_url = (base_url or ATLAN_BASE_URL or "").rstrip("/")
        if not resolved_base_url:
            raise ValueError("ATLAN_BASE_URL must be configured to use AtlanRESTClient.")

        self._client: AtlanClient = get_atlan_client(
            base_url=resolved_base_url,
            api_key=api_key or ATLAN_API_KEY,
        )

    @property
    def atlan_client(self) -> AtlanClient:
        """Expose the underlying PyAtlan client for advanced interactions."""

        return self._client

    # --------------------------------------------------------------------- Assets
    def upsert_assets(self, assets: List[Dict[str, Any]]) -> Dict[str, Any]:
        try:
            payload = [_ensure_asset(entity) for entity in assets]
            response = self._client.asset.save(payload)
            return _mutation_response_to_dict(response)
        except AtlanError as exc:  # pragma: no cover - exercised through higher layers
            raise _wrap_error(exc) from exc

    def get_asset(self, type_name: str, qualified_name: str) -> Optional[Dict[str, Any]]:
        asset_type = _resolve_asset_type(type_name)
        try:
            asset = self._client.asset.get_by_qualified_name(
                qualified_name=qualified_name,
                asset_type=asset_type,
                ignore_relationships=True,
            )
        except NotFoundError:
            return None
        except AtlanError as exc:  # pragma: no cover - defensive
            raise _wrap_error(exc) from exc
        return {"entity": _asset_to_dict(asset)}

    def search_assets(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        try:
            request = IndexSearchRequest.parse_obj(payload)
        except ValidationError as exc:
            raise AtlanRequestError(
                "Invalid search payload supplied to Atlan.",
                status_code=400,
                details=exc.errors(),
            ) from exc

        try:
            results = self._client.asset.search(request)
        except AtlanError as exc:  # pragma: no cover - exercised through callers
            raise _wrap_error(exc) from exc

        entities = [_asset_to_dict(asset) for asset in results.current_page()]
        return {
            "entities": entities,
            "approximateCount": results.count,
        }

    def delete_asset(self, guid: str) -> None:
        try:
            self._client.asset.delete_by_guid(guid)
        except AtlanError as exc:  # pragma: no cover - defensive
            raise _wrap_error(exc) from exc

    def purge_asset(
        self,
        guid: str,
        *,
        delete_type: AtlanDeleteType = AtlanDeleteType.PURGE,
    ) -> None:
        try:
            self._client.asset.purge_by_guid(guid, delete_type=delete_type)
        except AtlanError as exc:  # pragma: no cover - defensive
            raise _wrap_error(exc) from exc

    def classify_asset(self, guid: str, tag_names: Iterable[str]) -> None:
        try:
            asset = self._client.asset.retrieve_minimal(guid=guid, asset_type=AtlanAsset)
            if not asset or not asset.qualified_name:
                raise AtlanRequestError(
                    f"Unable to resolve asset for GUID {guid}",
                    status_code=404,
                    details=None,
                )
            self._client.asset.add_atlan_tags(
                asset_type=type(asset),
                qualified_name=asset.qualified_name,
                atlan_tag_names=[str(name) for name in tag_names],
            )
        except AtlanError as exc:  # pragma: no cover - defensive
            raise _wrap_error(exc) from exc

    def remove_classification(self, guid: str, tag_name: str) -> None:
        try:
            asset = self._client.asset.retrieve_minimal(guid=guid, asset_type=AtlanAsset)
            if not asset or not asset.qualified_name:
                raise AtlanRequestError(
                    f"Unable to resolve asset for GUID {guid}",
                    status_code=404,
                    details=None,
                )
            self._client.asset.remove_atlan_tag(
                asset_type=type(asset),
                qualified_name=asset.qualified_name,
                atlan_tag_name=str(tag_name),
            )
        except AtlanError as exc:  # pragma: no cover - defensive
            raise _wrap_error(exc) from exc

    def get_role_id(self, role_name: str) -> Optional[str]:
        """Return the GUID for a named Atlan role, if it exists."""

        try:
            role_id = self._client.role_cache.get_id_for_name(role_name)
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("Unable to resolve role '%s': %s", role_name, exc)
            return None
        return str(role_id) if role_id else None

    # ------------------------------------------------------------------- Typedefs
    def create_typedefs(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        classification_defs = payload.get("classificationDefs") or []
        created: List[Dict[str, Any]] = []

        for definition in classification_defs:
            name = str(definition.get("name") or "").strip()
            if not name:
                raise AtlanRequestError(
                    "Classification definition must include a name.",
                    status_code=400,
                    details=definition,
                )

            options = definition.get("options") or {}
            color = _coerce_tag_color(options.get("color", AtlanTagColor.GRAY.value))
            icon = _coerce_tag_icon(options.get("icon", AtlanIcon.ATLAN_TAG.value))

            try:
                typedef = AtlanTagDef.create(name=name, color=color, icon=icon)
                typedef.description = definition.get("description") or ""
                typedef.entity_types = definition.get("entityTypes")
                response = self._client.typedef.create(typedef)
                created.extend(
                    response.dict(by_alias=True, exclude_none=True).get(
                        "classificationDefs", []
                    )
                )
            except AtlanError as exc:  # pragma: no cover - defensive
                raise _wrap_error(exc) from exc

        return {"classificationDefs": created}

    def list_classification_typedefs(self) -> List[Dict[str, Any]]:
        try:
            response = self._client.typedef.get(AtlanTypeCategory.CLASSIFICATION)
        except AtlanError as exc:  # pragma: no cover - defensive
            raise _wrap_error(exc) from exc

        typedefs: List[Dict[str, Any]] = []
        for typedef in response.atlan_tag_defs:
            payload = typedef.dict(by_alias=True, exclude_none=True)
            payload.setdefault("displayName", typedef.display_name)
            typedefs.append(payload)
        return typedefs

    def get_typedef(self, name: str) -> Optional[Dict[str, Any]]:
        try:
            typedef = self._client.typedef.get_by_name(name)
        except NotFoundError:
            return None
        except AtlanError as exc:  # pragma: no cover - defensive
            raise _wrap_error(exc) from exc

        payload = typedef.dict(by_alias=True, exclude_none=True)
        payload.setdefault("displayName", typedef.display_name)
        return payload

    def purge_typedef(self, internal_name: str) -> None:
        try:
            api = API(
                f"types/typedef/name/{internal_name}",
                HTTPMethod.DELETE,
                HTTPStatus.NO_CONTENT,
                EndPoint.ATLAS,
            )
            self._client._call_api(api, None)
        except AtlanError as exc:  # pragma: no cover - defensive
            raise _wrap_error(exc) from exc

    # ---------------------------------------------------------------- Lifecycle
    def close(self) -> None:  # pragma: no cover - retained for API compatibility
        """SDK clients manage their own session lifecycle; method retained for parity."""
        return


# --------------------------------------------------------------------------- Helpers
def _ensure_asset(entity: Dict[str, Any] | AtlanAsset) -> AtlanAsset:
    if isinstance(entity, AtlanAsset):
        return entity
    if isinstance(entity, dict):
        return AtlanAsset._convert_to_real_type_(entity)
    raise TypeError(f"Unsupported asset payload type: {type(entity)!r}")


def _asset_to_dict(asset: AtlanAsset) -> Dict[str, Any]:
    return asset.dict(by_alias=True, exclude_none=True)


def _resolve_asset_type(type_name: str):
    try:
        return getattr(asset_models, type_name)
    except AttributeError:
        logger.debug("Falling back to generic Asset type for '%s'.", type_name)
        return AtlanAsset


def _mutation_response_to_dict(response: AssetMutationResponse) -> Dict[str, Any]:
    return response.dict(by_alias=True, exclude_none=True)


def _wrap_error(error: AtlanError) -> AtlanRequestError:
    details = {
        "errorId": error.error_code.error_id,
        "backendErrorId": getattr(error.error_code, "backend_error_id", ""),
    }
    return AtlanRequestError(
        str(error),
        status_code=error.error_code.http_error_code,
        details=details,
    )


def _coerce_tag_color(raw: str) -> AtlanTagColor:
    try:
        return AtlanTagColor(raw)
    except ValueError:
        logger.debug("Unknown tag color '%s'; defaulting to Gray.", raw)
        return AtlanTagColor.GRAY


def _coerce_tag_icon(raw: str) -> AtlanIcon:
    try:
        return AtlanIcon(raw)
    except ValueError:
        logger.debug("Unknown tag icon '%s'; defaulting to ATLAN_TAG.", raw)
        return AtlanIcon.ATLAN_TAG


__all__ = ["AtlanRESTClient", "AtlanRequestError"]
from pyatlan.errors import AtlanError
