"""Executable entrypoint for the DXR → Atlan application."""

from __future__ import annotations

import asyncio

from app.activities import ActivitiesClass
from app.client import ClientClass
from app.handler import HandlerClass
from app.workflow import WorkflowClass
from application_sdk.application import BaseApplication
from application_sdk.common.error_codes import ApiError
from application_sdk.observability.decorators.observability_decorator import (
    observability,
)
from application_sdk.observability.logger_adaptor import get_logger
from application_sdk.observability.metrics_adaptor import get_metrics
from application_sdk.observability.traces_adaptor import get_traces

APP_NAME = "dxr-integration-app"
logger = get_logger(__name__)
metrics = get_metrics()
traces = get_traces()


@observability(logger=logger, metrics=metrics, traces=traces)
async def main() -> None:
    """Configure the Atlan Application SDK runtime and start workers."""

    try:
        application = BaseApplication(
            name=APP_NAME,
            client_class=ClientClass,
            handler_class=HandlerClass,
        )

        await application.setup_workflow(
            workflow_and_activities_classes=[(WorkflowClass, ActivitiesClass)]
        )
        await application.start_worker()
        await application.setup_server(
            workflow_class=WorkflowClass,
            has_configmap=True,
        )
        await application.start_server()
    except ApiError:
        logger.error("%s", ApiError.SERVER_START_ERROR, exc_info=True)
        raise


if __name__ == "__main__":
    asyncio.run(main())
