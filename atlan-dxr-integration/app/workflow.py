"""Temporal workflow definition for orchestrating the DXR ingestion."""

from __future__ import annotations

from typing import Any, Callable, Dict, Sequence

from application_sdk.observability.logger_adaptor import get_logger
from application_sdk.workflows import WorkflowInterface
from temporalio import workflow

from .activities import ActivitiesClass

logger = get_logger(__name__)
workflow.logger = logger


@workflow.defn
class WorkflowClass(WorkflowInterface):
    """Primary workflow responsible for running the DXR → Atlan sync."""

    @workflow.run
    async def run(self, workflow_config: Dict[str, Any]) -> None:
        activities = ActivitiesClass()
        timeouts = activities.default_timeouts()

        resolved_args: Dict[str, Any] = await workflow.execute_activity_method(
            activities.get_workflow_args,
            workflow_config,
            start_to_close_timeout=timeouts["get_workflow_args"],
        )

        await workflow.execute_activity_method(
            activities.execute_dxr_sync,
            resolved_args,
            start_to_close_timeout=timeouts["execute_dxr_sync"],
        )

    @staticmethod
    def get_activities(
        activities: ActivitiesClass,
    ) -> Sequence[Callable[..., Any]]:
        if not isinstance(activities, ActivitiesClass):
            raise TypeError("Activities must be an instance of ActivitiesClass")
        return [
            activities.get_workflow_args,
            activities.execute_dxr_sync,
        ]
