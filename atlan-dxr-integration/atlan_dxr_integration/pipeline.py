"""Orchestration for the DXR → Atlan integration."""

from __future__ import annotations

import logging

from .atlan_uploader import AtlanUploadError, AtlanUploader
from .config import Config
from .dataset_builder import DatasetBuilder
from .dxr_client import DXRClient

LOGGER = logging.getLogger(__name__)


def run() -> None:
    """Entry point for running the integration once."""

    config = Config.from_env()
    logging.basicConfig(level=config.log_level.upper())
    LOGGER.info("Starting DXR → Atlan sync")

    with DXRClient(config.dxr_base_url, config.dxr_pat) as dxr_client:
        classifications = dxr_client.fetch_classifications()
        builder = DatasetBuilder(
            classifications,
            allowed_types=config.dxr_classification_types,
            sample_file_limit=config.dxr_sample_file_limit,
            dxr_base_url=config.dxr_base_url,
        )

        max_files = config.dxr_file_fetch_limit or None
        for classification in classifications:
            if not classification.identifier:
                continue
            files = dxr_client.fetch_files_for_label(
                classification.identifier,
                label_name=classification.name,
                max_items=max_files,
            )
            for file_payload in files:
                builder.consume_file(file_payload)

    records = builder.build()
    if not records:
        LOGGER.warning("No dataset records generated; nothing to upsert.")
        return

    uploader = AtlanUploader(config)
    try:
        uploader.upsert(records)
    except AtlanUploadError as exc:
        LOGGER.error("DXR → Atlan sync aborted: %s", exc)
        raise SystemExit(1) from exc
    LOGGER.info("DXR → Atlan sync completed")


__all__ = ["run"]
