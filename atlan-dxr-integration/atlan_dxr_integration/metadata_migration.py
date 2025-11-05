"""Utilities to migrate legacy DXR metadata tags into custom metadata fields."""

from __future__ import annotations

import argparse
import logging
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from pyatlan.errors import AtlanError
from pyatlan.model.custom_metadata import CustomMetadataDict

from .atlan_service import AtlanRESTClient, AtlanRequestError
from .config import Config
from .custom_metadata import ensure_default_sets

LOGGER = logging.getLogger(__name__)

_FILE_METADATA_SET = "DXR File Metadata"
_CLASSIFICATION_METADATA_SET = "DXR Classification Metadata"
_METADATA_ATTRIBUTES = (
    "DLP Labels",
    "Annotators",
    "Annotator Domains",
    "Entitlements",
    "Extracted Metadata",
    "Categories",
)


def parse_legacy_tags(
    entity: Dict[str, object],
    *,
    rest_client: AtlanRESTClient,
    classification_cache: Optional[Dict[str, str]] = None,
) -> Tuple[Dict[str, Dict[str, object]], List[str], List[Dict[str, object]]]:
    """Translate legacy tag-based enrichments into the new custom metadata model."""

    attributes = entity.get("attributes") or {}
    asset_tags = list(attributes.get("assetTags") or [])
    classifications = list(entity.get("classifications") or [])

    metadata_values: Dict[str, List[str]] = {
        "DLP Labels": [],
        "Annotators": [],
        "Annotator Domains": [],
        "Entitlements": [],
        "Extracted Metadata": [],
        "Categories": [],
    }
    removable_display_names: set[str] = set()

    for tag in asset_tags:
        if not isinstance(tag, str):
            continue
        normalized = tag.strip()
        if normalized.startswith("DXR :: DLP :: "):
            metadata_values["DLP Labels"].append(normalized.split("DXR :: DLP :: ", 1)[1].strip())
            removable_display_names.add(normalized)
        elif normalized.startswith("DXR :: Entitlement :: "):
            metadata_values["Entitlements"].append(
                normalized.split("DXR :: Entitlement :: ", 1)[1].strip()
            )
            removable_display_names.add(normalized)
        elif normalized.startswith("DXR :: Metadata :: "):
            metadata_values["Extracted Metadata"].append(
                normalized.split("DXR :: Metadata :: ", 1)[1].strip()
            )
            removable_display_names.add(normalized)
        elif normalized.startswith("DXR :: Category :: "):
            metadata_values["Categories"].append(
                normalized.split("DXR :: Category :: ", 1)[1].strip()
            )
            removable_display_names.add(normalized)
        elif normalized.startswith("DXR :: Annotator Domain :: "):
            metadata_values["Annotator Domains"].append(
                normalized.split("DXR :: Annotator Domain :: ", 1)[1].strip()
            )
            removable_display_names.add(normalized)
        elif normalized.startswith("DXR :: Annotator :: "):
            metadata_values["Annotators"].append(
                normalized.split("DXR :: Annotator :: ", 1)[1].strip()
            )
        else:
            continue

    filtered_asset_tags = [tag for tag in asset_tags if tag not in removable_display_names]

    cache = classification_cache if classification_cache is not None else {}
    filtered_classifications: List[Dict[str, object]] = []
    for item in classifications:
        if not isinstance(item, dict):
            continue
        type_name = item.get("typeName")
        if not isinstance(type_name, str) or not type_name.strip():
            filtered_classifications.append(item)
            continue
        if type_name not in cache:
            display_name = ""
            try:
                typedef = rest_client.get_typedef(type_name)
                defs = (typedef or {}).get("classificationDefs") or []
                if defs:
                    display_name = str(defs[0].get("displayName") or "")
            except AtlanRequestError:
                display_name = ""
            cache[type_name] = display_name
        display = cache[type_name]
        if display and display in removable_display_names:
            continue
        filtered_classifications.append(item)

    file_metadata = {
        key: sorted({value for value in values if value})
        for key, values in metadata_values.items()
        if values
    }
    metadata_map: Dict[str, Dict[str, object]] = {}
    if file_metadata:
        metadata_map[_FILE_METADATA_SET] = file_metadata

    return metadata_map, filtered_asset_tags, filtered_classifications


def build_business_attributes(
    rest_client: AtlanRESTClient,
    metadata: Dict[str, Dict[str, object]],
) -> Dict[str, Dict[str, object]]:
    """Convert metadata mapping into the hashed businessAttributes payload required by Atlan."""

    client = rest_client.atlan_client
    business_attributes: Dict[str, Dict[str, object]] = {}

    for set_name, attributes in metadata.items():
        if not attributes:
            continue
        try:
            cm_dict = CustomMetadataDict(client=client, name=set_name)
        except Exception as exc:  # pragma: no cover - defensive guard
            LOGGER.warning("Unable to initialize custom metadata set '%s': %s", set_name, exc)
            continue

        for attr_name, value in attributes.items():
            if value is None:
                continue
            if isinstance(value, list) and not value:
                continue
            try:
                cm_dict[attr_name] = value
            except KeyError:
                LOGGER.warning(
                    "Skipping unknown custom metadata attribute '%s' in set '%s'.",
                    attr_name,
                    set_name,
                )
            except Exception as exc:  # pragma: no cover - defensive guard
                LOGGER.warning(
                    "Failed setting custom metadata attribute '%s' in set '%s': %s",
                    attr_name,
                    set_name,
                    exc,
                )
        if not cm_dict.data:
            continue
        try:
            set_id = client.custom_metadata_cache.get_id_for_name(set_name)
        except Exception as exc:  # pragma: no cover - defensive guard
            LOGGER.warning("Unable to resolve custom metadata set '%s': %s", set_name, exc)
            continue
        business_attributes.setdefault(set_id, {}).update(cm_dict.business_attributes)

    return business_attributes


def backfill_file_metadata(
    config: Config,
    *,
    dry_run: bool = False,
    batch_size: int = 200,
) -> None:
    """Migrate existing file assets to the custom metadata model."""

    ensure_default_sets(config)
    rest_client = AtlanRESTClient(
        base_url=config.atlan_base_url,
        api_key=config.atlan_api_token,
    )

    connection_namespace = config.global_connection_namespace
    offset = 0
    updated = 0
    examined = 0
    classification_cache: Dict[str, str] = {}

    while True:
        payload = {
            "dsl": {
                "from": offset,
                "size": batch_size,
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"__typeName.keyword": "File"}},
                            {"wildcard": {"connectionQualifiedName.keyword": f"{connection_namespace}/*"}},
                        ]
                    }
                },
            }
        }
        results = rest_client.search_assets(payload)
        entities = results.get("entities") or []
        if not entities:
            break
        for entity in entities:
            examined += 1
            metadata_map, filtered_tags, filtered_classifications = parse_legacy_tags(
                entity,
                rest_client=rest_client,
                classification_cache=classification_cache,
            )

            original_attributes = entity.get("attributes") or {}
            original_tags = list(original_attributes.get("assetTags") or [])
            original_classifications = list(entity.get("classifications") or [])

            needs_metadata = bool(metadata_map)
            tags_changed = filtered_tags != original_tags
            classifications_changed = filtered_classifications != original_classifications

            if not (needs_metadata or tags_changed or classifications_changed):
                continue

            qualified_name = original_attributes.get("qualifiedName")
            LOGGER.info("Updating legacy metadata for file '%s'", qualified_name)

            update_entity: Dict[str, object] = {
                "typeName": entity.get("typeName", "File"),
                "guid": entity.get("guid"),
            }
            if tags_changed:
                update_entity.setdefault("attributes", {})["assetTags"] = filtered_tags
            if classifications_changed:
                update_entity["classifications"] = filtered_classifications
            if needs_metadata:
                business_attributes = build_business_attributes(rest_client, metadata_map)
                if business_attributes:
                    update_entity["businessAttributes"] = business_attributes

            if dry_run:
                LOGGER.debug("Dry-run payload: %s", update_entity)
            else:
                try:
                    rest_client.upsert_assets([update_entity])
                    updated += 1
                except AtlanError as exc:  # pragma: no cover - defensive guard
                    LOGGER.error(
                        "Failed to backfill metadata for '%s': %s",
                        qualified_name,
                        exc,
                    )
        offset += len(entities)

    LOGGER.info(
        "Processed %d file asset(s); %supdated %d.",
        examined,
        "would have " if dry_run else "",
        updated,
    )


def main(argv: Optional[Sequence[str]] = None) -> None:  # pragma: no cover - CLI glue
    parser = argparse.ArgumentParser(description="DXR metadata migration helpers.")
    subparsers = parser.add_subparsers(dest="command")

    backfill_parser = subparsers.add_parser(
        "backfill-files",
        help="Populate custom metadata for existing DXR file assets.",
    )
    backfill_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Log the operations that would be performed without persisting changes.",
    )
    backfill_parser.add_argument(
        "--batch-size",
        type=int,
        default=200,
        help="Number of assets to fetch per request (default: 200).",
    )
    backfill_parser.add_argument(
        "--log-level",
        default=None,
        help="Optional logging level override (e.g. DEBUG).",
    )

    args = parser.parse_args(argv)
    if args.command != "backfill-files":
        parser.print_help()
        raise SystemExit(1)

    config = Config.from_env(require_all=False)
    if not config.atlan_base_url or not config.atlan_api_token:
        raise SystemExit("ATLAN_BASE_URL and ATLAN_API_TOKEN must be configured.")

    log_level = (args.log_level or config.log_level).upper()
    logging.basicConfig(level=log_level)

    backfill_file_metadata(
        config,
        dry_run=args.dry_run,
        batch_size=args.batch_size,
    )


if __name__ == "__main__":  # pragma: no cover - CLI glue
    main()


__all__ = ["backfill_file_metadata", "parse_legacy_tags", "build_business_attributes"]
