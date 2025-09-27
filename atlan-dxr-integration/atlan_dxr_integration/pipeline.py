"""Orchestration for the DXR → Atlan integration."""

from __future__ import annotations

import logging

from .atlan_uploader import AtlanUploadError, AtlanUploader
from .config import Config
from .dataset_builder import DatasetBuilder
from .dxr_client import DXRClient
from .file_asset_builder import FileAssetBuilder

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
            file_fetch_limit=config.dxr_file_fetch_limit,
        )
        file_builder = FileAssetBuilder(
            connection_qualified_name=config.atlan_connection_qualified_name,
            connection_name=config.atlan_connection_name,
            dxr_base_url=config.dxr_base_url,
        )

        for file_payload in dxr_client.stream_files():
            builder.consume_file(file_payload)
            file_builder.consume(file_payload)

    records = builder.build()
    file_assets = file_builder.build()

    uploader = AtlanUploader(config)
    try:
        if records:
            uploader.upsert(records)
        else:
            LOGGER.warning("No dataset records generated; dataset sync skipped.")

        if file_assets:
            uploader.upsert_files(file_assets)
        else:
            LOGGER.warning("No file assets generated; file sync skipped.")
    except AtlanUploadError as exc:
        LOGGER.error("DXR → Atlan sync aborted: %s", exc)
        raise SystemExit(1) from exc
    LOGGER.info("DXR → Atlan sync completed")


__all__ = ["run"]
