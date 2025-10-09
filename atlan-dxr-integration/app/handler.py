"""Application handler for orchestrating the DXR workflow."""

from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict

from application_sdk.observability.logger_adaptor import get_logger

from .client import ClientClass

logger = get_logger(__name__)


class HandlerClass:
    """Bridge between the Atlan SDK runtime and DXR helpers."""

    def __init__(self, client: ClientClass | None = None):
        self.client = client or ClientClass()

    async def get_configmap(self, config_map_id: str) -> Dict[str, Any]:
        """Serve the workflow configuration metadata for the UI."""

        workflow_json_path = Path(__file__).resolve().parent / "frontend" / "workflow.json"

        with workflow_json_path.open(encoding="utf-8") as file_handle:
            return json.load(file_handle)

    def build_runtime_config(self, overrides: Dict[str, Any] | None = None) -> Dict[str, Any]:
        """Combine base configuration with workflow overrides for activities."""

        client = self.client.with_overrides(overrides)
        config = client.config
        serialized = asdict(config)

        # Ensure nested sets are serialized consistently for downstream activities.
        if serialized.get("dxr_classification_types") is not None:
            serialized["dxr_classification_types"] = sorted(
                serialized["dxr_classification_types"]
            )
        return serialized
