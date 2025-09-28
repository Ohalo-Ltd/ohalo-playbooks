"""Dump tag-related details for DXR file payloads before they are sent to Atlan."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from collections import Counter
from pathlib import Path
from typing import Iterable

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from atlan_dxr_integration.atlan_uploader import AtlanUploader
from atlan_dxr_integration.config import Config
from atlan_dxr_integration.dxr_client import DXRClient
from atlan_dxr_integration.file_asset_builder import FileAssetFactory
from atlan_dxr_integration.global_attributes import GlobalAttributeManager
from atlan_dxr_integration.tag_registry import TagRegistry


def _stringify(tag) -> str:
    type_name = getattr(tag, "type_name", None)
    return str(type_name) if type_name else repr(tag)


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--limit",
        type=int,
        default=5,
        help="Maximum number of DXR file payloads to inspect (default: 5)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level for the sanity check (default: INFO)",
    )

    args = parser.parse_args(list(argv) if argv is not None else None)

    logging.basicConfig(level=args.log_level.upper())
    logger = logging.getLogger("dxr-debug")

    config = Config.from_env()
    uploader = AtlanUploader(config)
    tag_registry = TagRegistry(uploader.client, namespace=config.atlan_tag_namespace)
    attribute_manager = GlobalAttributeManager(
        client=uploader.client,
        tag_registry=tag_registry,
    )
    factory = FileAssetFactory(
        tag_registry=tag_registry,
        tag_namespace=config.atlan_tag_namespace,
        dxr_base_url=config.dxr_base_url,
    )

    with DXRClient(config.dxr_base_url, config.dxr_pat) as dxr_client:
        classifications = dxr_client.fetch_classifications()
        tag_map = attribute_manager.ensure_classification_tags(classifications)
        logger.info("Resolved %d classification handles", len(tag_map))

        counts = Counter()
        for idx, payload in enumerate(dxr_client.stream_files(), start=1):
            identifier = payload.get("fileId") or payload.get("id")
            logger.info("\n=== DXR file %d: %s ===", idx, identifier or "<unknown>")

            fake_connection_qn = "debug/custom/dxr"

            asset = factory.build(
                payload,
                connection_qualified_name=fake_connection_qn,
                connection_name="debug",
                classification_tags=tag_map,
            )
            asset.attributes.connection_qualified_name = fake_connection_qn

            tag_names = sorted(asset.attributes.asset_tags or [])
            logger.info("Asset tag strings: %s", tag_names or "<none>")

            real_tags = asset.atlan_tags or []
            if real_tags:
                try:
                    sample_tag_def = uploader.client.typedef.get_by_name(
                        real_tags[0].type_name
                    )
                    entity_types = getattr(sample_tag_def, "entity_types", None)
                    tag_id = uploader.client.atlan_tag_cache.get_id_for_name(
                        real_tags[0].type_name
                    )
                except Exception:  # pragma: no cover - debug aid
                    entity_types = None
                    tag_id = None
                logger.info(
                    "AtlanTag assignments: %s",
                    [
                        {
                            "name": _stringify(tag),
                            "propagate": bool(getattr(tag, "propagate", False)),
                            "restrictHierarchy": bool(
                                getattr(tag, "restrict_propagation_through_hierarchy", False)
                            ),
                        }
                        for tag in real_tags
                    ],
                )
                logger.info(
                    "Sample tag entity types: %s (id=%s)", entity_types, tag_id
                )
            else:
                logger.warning("No AtlanTag assignments computed for this file")

            asset_json = json.loads(
                asset.json(by_alias=True, exclude_none=True, exclude_unset=True)
            )
            logger.info(
                "Payload excerpt: classifications=%s, atlanTags=%s",
                asset_json.get("classifications"),
                asset_json.get("atlanTags"),
            )

            counts[len(real_tags)] += 1
            if args.limit and idx >= args.limit:
                break

        logger.info(
            "Summary: inspected %d file(s); distribution of Atlan tag counts: %s",
            sum(counts.values()),
            dict(counts),
        )

    return 0


if __name__ == "__main__":  # pragma: no cover - script entry point
    raise SystemExit(main())
