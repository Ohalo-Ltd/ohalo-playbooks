"""Utilities for writing dataset records into Atlan."""

from __future__ import annotations

import logging
from typing import Iterable, List

from pyatlan.client.atlan import AtlanClient
from pyatlan.errors import AtlanError
from pyatlan.model.assets.data_set import DataSet

from .config import Config
from .dataset_builder import DatasetRecord

LOGGER = logging.getLogger(__name__)


class AtlanUploader:
    """Wrapper around Atlan's Asset API for upserting datasets."""

    def __init__(self, config: Config) -> None:
        self._config = config
        self._client = AtlanClient(
            base_url=config.atlan_base_url,
            api_key=config.atlan_api_token,
        )

    def upsert(self, records: Iterable[DatasetRecord]) -> None:
        batch: List[DataSet] = []
        for record in records:
            dataset = self._build_dataset(record)
            batch.append(dataset)
            if len(batch) >= self._config.atlan_batch_size:
                self._save_batch(batch)
                batch = []
        if batch:
            self._save_batch(batch)

    def _build_dataset(self, record: DatasetRecord) -> DataSet:
        qualified_name = f"{self._config.qualified_name_prefix}/{record.identifier}"
        attributes = DataSet.Attributes(
            qualified_name=qualified_name,
            name=record.name,
            description=record.classification.description,
            user_description=record.description,
            connection_name=self._config.atlan_connection_name,
            connection_qualified_name=self._config.atlan_connection_qualified_name,
            connector_name=self._config.atlan_connector_name,
            source_url=record.source_url,
        )
        dataset = DataSet(attributes=attributes)
        return dataset

    def _save_batch(self, batch: List[DataSet]) -> None:
        try:
            response = self._client.asset.save(batch)
            LOGGER.info(
                "Upserted %d datasets into Atlan (request id: %s)",
                len(batch),
                getattr(response, "request_id", "unknown"),
            )
        except AtlanError as exc:
            LOGGER.error("Failed to upsert datasets into Atlan: %s", exc)
            raise


__all__ = ["AtlanUploader"]
