"""Orchestration for the DXR → Atlan integration."""

from __future__ import annotations

import logging
from typing import Optional

from .atlan_uploader import AtlanUploadError, AtlanUploader
from .config import Config
from .connection_utils import ConnectionProvisioner
from .dataset_builder import DatasetBuilder
from .datasource_ingestor import DatasourceIngestionCoordinator
from .dxr_client import DXRClient
from .file_asset_builder import FileAssetFactory
from .global_attributes import GlobalAttributeManager
from .tag_registry import TagRegistry

LOGGER = logging.getLogger(__name__)


def run(config: Optional[Config] = None) -> None:
    """Entry point for running the integration once.

    Args:
        config: Optional pre-built configuration. When omitted the configuration is
            loaded from environment variables via :meth:`Config.from_env`.
    """

    config = config or Config.from_env()
    logging.basicConfig(level=config.log_level.upper())
    LOGGER.info("Starting DXR → Atlan sync")

    uploader = AtlanUploader(config)
    provisioner = ConnectionProvisioner(uploader.client)
    tag_registry = TagRegistry(uploader.client, namespace=config.atlan_tag_namespace)
    attribute_manager = GlobalAttributeManager(
        client=uploader.client,
        tag_registry=tag_registry,
    )

    dataset_builder: DatasetBuilder
    datasource_coordinator: Optional[DatasourceIngestionCoordinator] = None
    classification_tag_map = {}

    with DXRClient(config.dxr_base_url, config.dxr_pat) as dxr_client:
        classifications = dxr_client.fetch_classifications()
        classification_tag_map = attribute_manager.ensure_classification_tags(
            classifications
        )
        dataset_builder = DatasetBuilder(
            classifications,
            allowed_types=config.dxr_classification_types,
            sample_file_limit=config.dxr_sample_file_limit,
            dxr_base_url=config.dxr_base_url,
            file_fetch_limit=config.dxr_file_fetch_limit,
        )
        file_factory = FileAssetFactory(
            tag_registry=tag_registry,
            tag_namespace=config.atlan_tag_namespace,
            dxr_base_url=config.dxr_base_url,
        )
        datasource_coordinator = DatasourceIngestionCoordinator(
            config=config,
            client=uploader.client,
            provisioner=provisioner,
            factory=file_factory,
        )

        for file_payload in dxr_client.stream_files():
            dataset_builder.consume_file(file_payload)
            datasource_coordinator.consume(
                file_payload,
                classification_tags=classification_tag_map,
            )

    records = dataset_builder.build()
    try:
        if records:
            uploader.upsert(records)
        else:
            LOGGER.warning("No dataset records generated; dataset sync skipped.")
        if datasource_coordinator:
            datasource_coordinator.flush()
        else:
            LOGGER.warning("No file assets generated; file sync skipped.")
    except AtlanUploadError as exc:
        LOGGER.error("DXR → Atlan sync aborted: %s", exc)
        raise SystemExit(1) from exc
    LOGGER.info("DXR → Atlan sync completed")


__all__ = ["run"]
