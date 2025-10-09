"""Temporal activities for the DXR → Atlan application."""

from __future__ import annotations

from datetime import timedelta
from typing import Any, Dict

from application_sdk.activities import ActivitiesInterface
from application_sdk.observability.logger_adaptor import get_logger
from temporalio import activity

from atlan_dxr_integration.config import Config
from atlan_dxr_integration.pipeline import run as run_pipeline

from .handler import HandlerClass

logger = get_logger(__name__)
activity.logger = logger


class ActivitiesClass(ActivitiesInterface):
    """Activity implementations that wrap the original DXR integration helpers."""

    def __init__(self, handler: HandlerClass | None = None):
        self._handler = handler

    def _get_handler(self) -> HandlerClass:
        if self._handler is None:
            self._handler = HandlerClass()
        return self._handler

    @activity.defn
    def get_workflow_args(self, initial_config: Dict[str, Any] | None = None) -> Dict[str, Any]:
        """Return the runtime configuration for the workflow."""

        handler = self._get_handler()
        merged_config = handler.build_runtime_config(initial_config or {})
        logger.debug("Resolved workflow configuration: %s", merged_config)
        return merged_config

    @activity.defn
    def execute_dxr_sync(self, workflow_config: Dict[str, Any]) -> None:
        """Run the DXR → Atlan pipeline using the provided configuration."""

        config_payload = dict(workflow_config)
        types_value = config_payload.get("dxr_classification_types")
        if types_value:
            if isinstance(types_value, str):
                cleaned = {
                    item.strip().upper()
                    for item in types_value.split(",")
                    if item.strip()
                }
            elif isinstance(types_value, (list, tuple, set)):
                cleaned = {
                    str(item).strip().upper()
                    for item in types_value
                    if str(item).strip()
                }
            else:
                cleaned = set()
        config_payload["dxr_classification_types"] = cleaned or None
        config = Config(**config_payload)
        logger.info("Executing DXR sync via activity")
        run_pipeline(config=config)

    @staticmethod
    def default_timeouts() -> Dict[str, timedelta]:
        """Return default timeout settings for activities."""

        return {
            "get_workflow_args": timedelta(seconds=10),
            "execute_dxr_sync": timedelta(minutes=30),
        }
