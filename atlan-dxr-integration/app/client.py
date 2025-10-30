"""Client utilities for the DXR → Atlan application."""

from __future__ import annotations

from dataclasses import replace
from typing import Any, Dict, Optional, Set

from application_sdk.observability.logger_adaptor import get_logger

from atlan_dxr_integration.atlan_uploader import AtlanUploader
from atlan_dxr_integration.config import Config
from atlan_dxr_integration.dxr_client import DXRClient

_INT_FIELDS: Set[str] = {
    "dxr_sample_file_limit",
    "dxr_file_fetch_limit",
    "atlan_batch_size",
}
_SET_FIELDS: Set[str] = {"dxr_classification_types"}
_CONFIG_FIELDS: Set[str] = set(Config.__dataclass_fields__.keys())

logger = get_logger(__name__)


class ClientClass:
    """Lifecycle helpers for DXR and Atlan clients."""

    def __init__(self, config: Optional[Config] = None):
        if config is not None:
            self._config = config
        else:
            try:
                self._config = Config.from_env()
            except ValueError:
                # Allow the application to boot without local environment variables.
                self._config = Config.from_env(require_all=False)

    @property
    def config(self) -> Config:
        """Return the cached configuration."""

        return self._config

    def with_overrides(self, overrides: Dict[str, Any] | None = None) -> "ClientClass":
        """Return a new client with configuration overrides applied."""

        if not overrides:
            return ClientClass(config=self._config)

        updates: Dict[str, Any] = {}
        for field_name, raw_value in overrides.items():
            if field_name not in _CONFIG_FIELDS or raw_value is None:
                continue
            try:
                updates[field_name] = self._coerce_value(field_name, raw_value)
            except ValueError as exc:
                logger.warning("Skipping invalid override for %s: %s", field_name, exc)

        merged_config = replace(self._config, **updates) if updates else self._config
        return ClientClass(config=merged_config)

    @staticmethod
    def _coerce_value(field_name: str, raw_value: Any) -> Any:
        """Coerce overrides into compatible dataclass field values."""

        if field_name in _INT_FIELDS:
            return int(raw_value)
        if field_name in _SET_FIELDS:
            if isinstance(raw_value, str):
                values = {
                    item.strip().upper()
                    for item in raw_value.split(",")
                    if item.strip()
                }
                return values or None
            if isinstance(raw_value, (list, set, tuple)):
                values = {
                    str(item).strip().upper() for item in raw_value if str(item).strip()
                }
                return values or None
            raise ValueError("Expected comma separated string or iterable of strings")
        return raw_value

    def create_dxr_client(self) -> DXRClient:
        """Instantiate a DXR client using the configured credentials."""

        return DXRClient(self._config.dxr_base_url, self._config.dxr_pat)

    def create_uploader(self) -> AtlanUploader:
        """Instantiate an Atlan uploader for dataset and file assets."""

        return AtlanUploader(self._config)
