"""Dump tag-related details for DXR file payloads before they are sent to Atlan."""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import unicodedata
from collections import Counter
from pathlib import Path
from typing import Dict, Iterable, Optional, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from atlan_dxr_integration.atlan_uploader import AtlanUploader
from atlan_dxr_integration.config import Config
from atlan_dxr_integration.connection_utils import resolve_connector_type
from atlan_dxr_integration.dxr_client import DXRClient
from atlan_dxr_integration.file_asset_builder import FileAssetFactory
from atlan_dxr_integration.global_attributes import GlobalAttributeManager
from atlan_dxr_integration.tag_registry import TagRegistry

from pyatlan.errors import AtlanError, NotFoundError
from pyatlan.model.assets.core.file import File


def _coalesce_str(*values: object) -> Optional[str]:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def _slugify(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
    lowered = ascii_text.lower()
    cleaned = re.sub(r"[^a-z0-9]+", "-", lowered).strip("-")
    return cleaned or "datasource"


def _derive_connection_details(
    payload: Dict[str, object], config: Config
) -> Tuple[str, str]:
    datasource = payload.get("datasource")
    datasource_id = None
    datasource_name = None
    connector_name = None

    if isinstance(datasource, dict):
        datasource_id = _coalesce_str(datasource.get("id"))
        datasource_name = _coalesce_str(datasource.get("name"))
        connector = datasource.get("connector")
        if isinstance(connector, dict):
            connector_name = _coalesce_str(connector.get("type"))

    key = datasource_id or datasource_name or "unknown"
    slug = _slugify(datasource_name or key)
    connection_name = (
        f"{config.atlan_datasource_connection_prefix}-{slug}".strip("-")
    )

    connector_type = resolve_connector_type(
        connector_name or config.atlan_global_connector_name
    )
    namespace = config.global_connection_namespace
    connection_qn = f"{namespace}/{connector_type.value}/{connection_name}"
    return connection_name, connection_qn


def _lookup_tag_display_name(
    client, type_name: Optional[str], cache: Dict[str, Optional[str]]
) -> Optional[str]:
    if not type_name:
        return None
    if type_name in cache:
        return cache[type_name]
    display = None
    try:
        typedef = client.typedef.get_by_name(type_name)
        display = getattr(typedef, "display_name", None)
    except AtlanError as exc:  # pragma: no cover - remote call defensive
        logging.getLogger("dxr-debug").debug(
            "Unable to resolve display name for tag '%s': %s", type_name, exc
        )
    cache[type_name] = display
    return display


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
    parser.add_argument(
        "--skip-fetch-existing",
        action="store_true",
        help="Skip fetching corresponding Atlan assets (useful when offline)",
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
        typedef_cache: Dict[str, Optional[str]] = {}
        for idx, payload in enumerate(dxr_client.stream_files(), start=1):
            identifier = payload.get("fileId") or payload.get("id")
            logger.info("\n=== DXR file %d: %s ===", idx, identifier or "<unknown>")

            connection_name, connection_qn = _derive_connection_details(payload, config)
            logger.info(
                "Expected connection: name=%s qualifiedName=%s",
                connection_name,
                connection_qn,
            )

            built = factory.build(
                payload,
                connection_qualified_name=connection_qn,
                connection_name=connection_name,
                classification_tags=tag_map,
            )

            asset = built.asset

            expected_qn = asset.attributes.qualified_name
            logger.info("Expected file qualified name: %s", expected_qn)

            tag_names = sorted(asset.attributes.asset_tags or [])
            logger.info("Asset tag strings: %s", tag_names or "<none>")

            tag_display_names = [handle.display_name for handle in built.tag_handles]
            tag_hashed_names = [handle.hashed_name for handle in built.tag_handles]
            logger.info(
                "Expected Atlan tag display names (%d): %s",
                len(tag_display_names),
                tag_display_names or "<none>",
            )
            logger.info(
                "Expected Atlan tag hashed names: %s",
                tag_hashed_names or "<none>",
            )

            entity_types = None
            sample_tag_id = None
            if tag_hashed_names:
                sample_tag_id = tag_hashed_names[0]
                try:
                    sample_tag_def = uploader.client.typedef.get_by_name(sample_tag_id)
                    entity_types = getattr(sample_tag_def, "entity_types", None)
                except AtlanError as exc:  # pragma: no cover - debug aid
                    logger.debug(
                        "Unable to fetch typedef for %s: %s", sample_tag_id, exc
                    )
            if sample_tag_id:
                logger.info(
                    "Sample tag entity types: %s (hashed id=%s)",
                    entity_types,
                    sample_tag_id,
                )

            asset_json = json.loads(
                asset.json(by_alias=True, exclude_none=True, exclude_unset=True)
            )
            logger.info(
                "Payload excerpt: classifications=%s, atlanTags=%s",
                asset_json.get("classifications"),
                asset_json.get("atlanTags"),
            )

            expected_classification_ids = tag_hashed_names

            counts[len(built.tag_handles)] += 1
            if not args.skip_fetch_existing and expected_qn:
                _inspect_existing_asset(
                    logger,
                    uploader.client,
                    expected_qn,
                    typedef_cache,
                    expected_classification_ids,
                    tag_hashed_names,
                    tag_names,
                )
            if args.limit and idx >= args.limit:
                break

        logger.info(
            "Summary: inspected %d file(s); distribution of Atlan tag counts: %s",
            sum(counts.values()),
            dict(counts),
        )

    return 0


def _inspect_existing_asset(
    logger,
    client,
    qualified_name: str,
    cache,
    expected_classification_ids,
    expected_atlan_tag_names,
    expected_display_tags,
) -> None:
    try:
        existing = client.asset.get_by_qualified_name(
            qualified_name,
            asset_type=File,
            ignore_relationships=False,
        )
    except NotFoundError:
        logger.warning(
            "No existing Atlan File asset found for qualified name '%s'",
            qualified_name,
        )
        return
    except AtlanError as exc:  # pragma: no cover - remote call defensive
        logger.error(
            "Unable to fetch Atlan asset '%s' for inspection: %s",
            qualified_name,
            exc,
        )
        return

    attrs = existing.attributes
    logger.info(
        "Existing asset located: name=%s, connection=%s",
        getattr(attrs, "display_name", None),
        getattr(attrs, "connection_qualified_name", None),
    )

    classifications = getattr(existing, "classifications", None) or []
    if classifications:
        details = []
        actual_classification_ids = []
        for classification in classifications:
            type_name = getattr(classification, "type_name", None)
            actual_classification_ids.append(type_name)
            details.append(
                {
                    "typeName": type_name,
                    "displayName": _lookup_tag_display_name(client, type_name, cache),
                    "propagate": bool(getattr(classification, "propagate", False)),
                    "restrictHierarchy": bool(
                        getattr(
                            classification,
                            "restrict_propagation_through_hierarchy",
                            False,
                        )
                    ),
                }
            )
        logger.info("Existing classifications (%d): %s", len(details), details)
        expected_set = {value for value in expected_classification_ids if value}
        actual_set = {value for value in actual_classification_ids if value}
        missing = sorted(expected_set - actual_set)
        unexpected = sorted(actual_set - expected_set)
        if missing:
            logger.warning("Missing classification typeNames on asset: %s", missing)
        if unexpected:
            logger.warning("Unexpected classification typeNames on asset: %s", unexpected)
    else:
        logger.warning("Existing asset has no classifications populated")

    existing_tags = getattr(existing, "atlan_tags", None) or []
    if existing_tags:
        tag_info = []
        actual_atlan_tag_names = []
        for tag in existing_tags:
            type_name = getattr(tag, "type_name", None)
            actual_atlan_tag_names.append(type_name)
            tag_info.append(
                {
                    "typeName": type_name,
                    "displayName": _lookup_tag_display_name(client, type_name, cache),
                    "propagate": bool(getattr(tag, "propagate", False)),
                }
            )
        logger.info("Existing Atlan tags (%d): %s", len(tag_info), tag_info)
        expected_set = {value for value in expected_atlan_tag_names if value}
        actual_set = {value for value in actual_atlan_tag_names if value}
        missing = sorted(expected_set - actual_set)
        unexpected = sorted(actual_set - expected_set)
        if missing:
            logger.warning("Missing Atlan tag typeNames on asset: %s", missing)
        if unexpected:
            logger.warning("Unexpected Atlan tag typeNames on asset: %s", unexpected)
    else:
        logger.info("Existing asset exposes no Atlan-specific tags")

    actual_asset_tag_strings = sorted(getattr(attrs, "asset_tags", []) or [])
    logger.info("Existing asset tag strings: %s", actual_asset_tag_strings or "<none>")
    expected_set = set(expected_display_tags or [])
    actual_set = set(actual_asset_tag_strings or [])
    missing = sorted(expected_set - actual_set)
    unexpected = sorted(actual_set - expected_set)
    if missing:
        logger.warning("Missing asset tag strings: %s", missing)
    if unexpected:
        logger.warning("Unexpected asset tag strings: %s", unexpected)


if __name__ == "__main__":  # pragma: no cover - script entry point
    raise SystemExit(main())
