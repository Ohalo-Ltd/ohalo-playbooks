"""Operational sanity check for the DXR → Atlan integration."""

from __future__ import annotations

import argparse
import logging
from typing import Iterable, List

from pyatlan.errors import AtlanError
from pyatlan.model.assets.core.table import Table

from .config import Config
from .dataset_builder import DatasetBuilder, DatasetRecord
from .dxr_client import DXRClient
from .atlan_uploader import AtlanUploader

LOGGER = logging.getLogger(__name__)


def run(*, sample_labels: int = 1, max_files: int = 500) -> None:
    """Fetch a sample of DXR labels and verify they are materialised in Atlan."""

    if sample_labels < 1:
        raise ValueError("sample_labels must be >= 1")
    if max_files < 1:
        raise ValueError("max_files must be >= 1")

    config = Config.from_env()
    logging.basicConfig(level=config.log_level.upper())

    LOGGER.info("Starting sanity check: requesting up to %d label(s)", sample_labels)

    with DXRClient(config.dxr_base_url, config.dxr_pat) as dxr_client:
        classifications = dxr_client.fetch_classifications()
        if not classifications:
            raise SystemExit("No classifications returned from DXR; aborting.")
        LOGGER.info("Fetched %d classifications from DXR", len(classifications))

        builder = DatasetBuilder(
            classifications,
            allowed_types=config.dxr_classification_types,
            sample_file_limit=config.dxr_sample_file_limit,
            dxr_base_url=config.dxr_base_url,
        )

        remaining = max_files
        per_label_limit = config.dxr_file_fetch_limit or None

        for classification in classifications:
            if not classification.identifier:
                continue
            if remaining is not None and remaining <= 0:
                LOGGER.debug("Reached overall file cap; stopping ingestion loop")
                break

            limit = per_label_limit
            if remaining is not None:
                limit = remaining if limit is None else min(limit, remaining)

            files = dxr_client.fetch_files_for_label(
                classification.identifier,
                label_name=classification.name,
                max_items=limit,
            )
            for payload in files:
                builder.consume_file(payload)

            if remaining is not None:
                remaining -= len(files)

    all_records = builder.build()
    if not all_records:
        raise SystemExit(
            "Sample did not produce any dataset records to upsert. Try increasing "
            "--max-files or widening DXR filters."
        )

    sorted_records = sorted(all_records, key=lambda r: r.file_count, reverse=True)
    records = sorted_records[:sample_labels]
    LOGGER.info(
        "Prepared %d dataset record(s); verifying %d", len(all_records), len(records)
    )

    uploader = AtlanUploader(config)
    uploader.upsert(records)

    _verify_assets(records, uploader)
    LOGGER.info("Sanity check completed successfully.")


def _verify_assets(records: Iterable[DatasetRecord], uploader: AtlanUploader) -> None:
    """Fetch each table from Atlan to confirm it exists after upsert."""

    for record in records:
        qualified_name = uploader.table_qualified_name(record)
        try:
            table: Table = uploader.client.asset.get_by_qualified_name(
                qualified_name,
                Table,
                ignore_relationships=True,
            )
        except AtlanError as exc:
            raise SystemExit(
                f"Failed to verify table '{qualified_name}' in Atlan: {exc}"
            ) from exc

        attrs = table.attributes
        LOGGER.info(
            "Verified table '%s' (display_name='%s', files=%d)",
            qualified_name,
            getattr(attrs, "name", record.name),
            record.file_count,
        )


def main(argv: list[str] | None = None) -> None:  # pragma: no cover - CLI glue
    parser = argparse.ArgumentParser(description="DXR → Atlan sanity check")
    parser.add_argument(
        "--labels",
        type=int,
        default=1,
        help="Number of DXR classifications to verify (default: 1)",
    )
    parser.add_argument(
        "--max-files",
        type=int,
        default=500,
        help="Maximum number of DXR file payloads to inspect (default: 500)",
    )
    args = parser.parse_args(argv)

    try:
        run(sample_labels=args.labels, max_files=args.max_files)
    except SystemExit:
        raise
    except Exception as exc:  # pragma: no cover - defensive logging
        LOGGER.error("Sanity check failed: %s", exc)
        raise SystemExit(1) from exc


if __name__ == "__main__":  # pragma: no cover - CLI glue
    main()
