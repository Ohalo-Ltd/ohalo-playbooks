"""Temporal activities for the DXR → Atlan application."""

from __future__ import annotations

from datetime import timedelta
from typing import Any, Dict, Optional

from application_sdk.activities import ActivitiesInterface
from application_sdk.observability.logger_adaptor import get_logger
from temporalio import activity

from application_sdk.services.statestore import StateStore, StateType
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
    async def get_workflow_args(
        self, initial_config: Dict[str, Any] | None = None
    ) -> Dict[str, Any]:
        """Return the runtime configuration for the workflow."""

        handler = self._get_handler()
        overrides: Dict[str, Any] = dict(initial_config or {})
        workflow_id = overrides.get("workflow_id")
        if workflow_id:
            try:
                saved_state = await StateStore.get_state(
                    workflow_id, StateType.WORKFLOWS
                )
            except Exception as exc:
                logger.warning(
                    "Unable to load workflow overrides for %s: %s", workflow_id, exc
                )
                saved_state = {}
            # State store payload already includes workflow_id, so ensure request wins.
            merged_input = {**saved_state, **overrides}
        else:
            merged_input = overrides

        merged_config = handler.build_runtime_config(merged_input)
        logger.debug("Resolved workflow configuration: %s", merged_config)
        return merged_config

    @activity.defn
    def execute_dxr_sync(self, workflow_config: Dict[str, Any]) -> None:
        """Run the DXR → Atlan pipeline using the provided configuration."""

        config_payload = dict(workflow_config)
        types_value = config_payload.get("dxr_classification_types")
        cleaned: Optional[set[str]] = None
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

        required_fields = (
            "dxr_base_url",
            "dxr_pat",
            "atlan_base_url",
            "atlan_api_token",
            "atlan_global_connection_qualified_name",
            "atlan_global_connection_name",
            "atlan_global_connector_name",
        )
        missing = [field for field in required_fields if not config_payload.get(field)]
        if missing:
            raise ValueError(
                "Missing required configuration values: " + ", ".join(missing)
            )

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
