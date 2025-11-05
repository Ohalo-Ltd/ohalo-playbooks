"""Custom metadata provisioning and helpers for the DXR → Atlan integration."""

from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from typing import Iterable, List, Optional, Sequence, Set

from application_sdk.clients.atlan import get_client as get_atlan_client
from pyatlan.client.atlan import AtlanClient
from pyatlan.errors import AtlanError, NotFoundError
from pyatlan.model.enums import (
    AtlanCustomAttributePrimitiveType,
    AtlanIcon,
    AtlanTagColor,
)
from pyatlan.model.typedef import AttributeDef, CustomMetadataDef

from .config import Config

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class AttributeSpec:
    """Specification for a single custom metadata attribute."""

    display_name: str
    attribute_type: AtlanCustomAttributePrimitiveType
    multi_valued: bool = False
    description: Optional[str] = None
    applicable_asset_types: Optional[Set[str]] = None


@dataclass(frozen=True)
class MetadataSetSpec:
    """Specification for a custom metadata set."""

    display_name: str
    description: Optional[str]
    attributes: Sequence[AttributeSpec]
    icon: Optional[AtlanIcon] = None
    color: Optional[AtlanTagColor] = None


def _default_metadata_specs() -> Sequence[MetadataSetSpec]:
    """Return the built-in custom metadata sets used by the DXR integration."""

    file_attributes = [
        AttributeSpec(
            display_name="DLP Labels",
            attribute_type=AtlanCustomAttributePrimitiveType.STRING,
            multi_valued=True,
            description="DXR DLP labels captured as '<system> :: <label>' strings.",
            applicable_asset_types={"File"},
        ),
        AttributeSpec(
            display_name="Annotators",
            attribute_type=AtlanCustomAttributePrimitiveType.STRING,
            multi_valued=True,
            description="Names of DXR annotators associated with the file.",
            applicable_asset_types={"File"},
        ),
        AttributeSpec(
            display_name="Annotator Domains",
            attribute_type=AtlanCustomAttributePrimitiveType.STRING,
            multi_valued=True,
            description="Domains reported by DXR annotators.",
            applicable_asset_types={"File"},
        ),
        AttributeSpec(
            display_name="Entitlements",
            attribute_type=AtlanCustomAttributePrimitiveType.STRING,
            multi_valued=True,
            description="Who-can-access principals expressed as '<type> :: <identifier>'.",
            applicable_asset_types={"File"},
        ),
        AttributeSpec(
            display_name="Extracted Metadata",
            attribute_type=AtlanCustomAttributePrimitiveType.STRING,
            multi_valued=True,
            description="DXR extracted metadata pairs rendered as '<key> = <value>'.",
            applicable_asset_types={"File"},
        ),
        AttributeSpec(
            display_name="Categories",
            attribute_type=AtlanCustomAttributePrimitiveType.STRING,
            multi_valued=True,
            description="DXR categories assigned to the file.",
            applicable_asset_types={"File"},
        ),
    ]

    classification_attributes = [
        AttributeSpec(
            display_name="DXR Classification ID",
            attribute_type=AtlanCustomAttributePrimitiveType.STRING,
            description="Stable DXR classification identifier.",
            applicable_asset_types={"Table"},
        ),
        AttributeSpec(
            display_name="DXR Classification Type",
            attribute_type=AtlanCustomAttributePrimitiveType.STRING,
            description="High-level DXR classification type.",
            applicable_asset_types={"Table"},
        ),
        AttributeSpec(
            display_name="DXR Classification Subtype",
            attribute_type=AtlanCustomAttributePrimitiveType.STRING,
            description="DXR classification subtype, when provided.",
            applicable_asset_types={"Table"},
        ),
        AttributeSpec(
            display_name="DXR File Count",
            attribute_type=AtlanCustomAttributePrimitiveType.INTEGER,
            description="Number of DXR files aggregated into the dataset.",
            applicable_asset_types={"Table"},
        ),
        AttributeSpec(
            display_name="DXR Sample Files",
            attribute_type=AtlanCustomAttributePrimitiveType.STRING,
            multi_valued=True,
            description="Sample file summaries captured during ingestion.",
            applicable_asset_types={"Table"},
        ),
        AttributeSpec(
            display_name="DXR Detail URL",
            attribute_type=AtlanCustomAttributePrimitiveType.URL,
            description="Link to the DXR detail view for this classification.",
            applicable_asset_types={"Table"},
        ),
        AttributeSpec(
            display_name="DXR Search URL",
            attribute_type=AtlanCustomAttributePrimitiveType.URL,
            description="Link to the DXR search view filtered for this classification.",
            applicable_asset_types={"Table"},
        ),
    ]

    return (
        MetadataSetSpec(
            display_name="DXR File Metadata",
            description="DXR-derived annotations that enrich file assets without using Atlan classifications.",
            attributes=file_attributes,
            icon=AtlanIcon.FILE,
            color=AtlanTagColor.GRAY,
        ),
        MetadataSetSpec(
            display_name="DXR Classification Metadata",
            description="Reference metadata captured for each DXR classification dataset.",
            attributes=classification_attributes,
            icon=AtlanIcon.TABLE,
            color=AtlanTagColor.GREEN,
        ),
    )


class CustomMetadataManager:
    """Provisioner that guarantees DXR custom metadata structures exist in Atlan."""

    def __init__(self, client: AtlanClient, *, specs: Optional[Sequence[MetadataSetSpec]] = None) -> None:
        self._client = client
        self._specs: Sequence[MetadataSetSpec] = specs or _default_metadata_specs()

    @classmethod
    def from_config(cls, config: Config) -> "CustomMetadataManager":
        client = get_atlan_client(
            base_url=config.atlan_base_url,
            api_key=config.atlan_api_token,
        )
        return cls(client)

    def ensure_specifications(self) -> None:
        """Ensure each configured custom metadata set exists (creating or extending as needed)."""

        cache = self._client.custom_metadata_cache
        try:
            cache.refresh_cache()
        except AtlanError as exc:  # pragma: no cover - defensive guard
            LOGGER.warning("Unable to refresh custom metadata cache: %s", exc)

        for spec in self._specs:
            self._ensure_set(cache, spec)

    def _ensure_set(
        self,
        cache,
        spec: MetadataSetSpec,
    ) -> None:
        existing = None
        try:
            existing = cache.get_custom_metadata_def(spec.display_name)
        except NotFoundError:
            LOGGER.info("Custom metadata set '%s' not found; creating it.", spec.display_name)
        except AtlanError as exc:  # pragma: no cover - defensive
            LOGGER.error("Unable to inspect custom metadata '%s': %s", spec.display_name, exc)

        if not existing:
            definition = self._build_definition(spec)
            try:
                self._client.typedef.create(definition)
            except AtlanError as exc:
                LOGGER.error("Failed to create custom metadata '%s': %s", spec.display_name, exc)
                raise
            try:
                cache.refresh_cache()
            except AtlanError:
                LOGGER.debug("Failed to refresh custom metadata cache after creation; continuing.")
            LOGGER.info(
                "Created custom metadata set '%s' with %d attribute(s).",
                spec.display_name,
                len(spec.attributes),
            )
            return

        existing_names = {attr.display_name for attr in existing.attribute_defs or []}
        missing_specs = [attr for attr in spec.attributes if attr.display_name not in existing_names]
        if not missing_specs:
            LOGGER.debug("Custom metadata set '%s' already up to date.", spec.display_name)
            return

        LOGGER.info(
            "Extending custom metadata set '%s' with %d new attribute(s).",
            spec.display_name,
            len(missing_specs),
        )
        additions = [self._build_attribute(attr_spec) for attr_spec in missing_specs]
        updated = CustomMetadataDef(
            category=existing.category,
            description=spec.description or existing.description,
            display_name=existing.display_name,
            name=existing.name,
            attribute_defs=list(existing.attribute_defs or []) + additions,
            options=existing.options,
        )
        try:
            self._client.typedef.update(updated)
        except AtlanError as exc:
            LOGGER.error("Failed to extend custom metadata '%s': %s", spec.display_name, exc)
            raise
        try:
            cache.refresh_cache()
        except AtlanError:
            LOGGER.debug("Failed to refresh custom metadata cache after update; continuing.")

    def _build_definition(self, spec: MetadataSetSpec) -> CustomMetadataDef:
        definition = CustomMetadataDef.create(display_name=spec.display_name, description=spec.description)
        if spec.icon and spec.color:
            definition.options = CustomMetadataDef.Options.with_logo_from_icon(
                icon=spec.icon,
                color=spec.color,
                locked=False,
            )
        definition.attribute_defs = [self._build_attribute(attr) for attr in spec.attributes]
        return definition

    def _build_attribute(self, spec: AttributeSpec) -> AttributeDef:
        applicable_assets: Optional[Set[str]] = (
            set(spec.applicable_asset_types) if spec.applicable_asset_types else None
        )
        return AttributeDef.create(
            client=self._client,
            display_name=spec.display_name,
            attribute_type=spec.attribute_type,
            multi_valued=spec.multi_valued,
            applicable_asset_types=applicable_assets,
            description=spec.description,
        )


def ensure_default_sets(config: Config) -> None:
    """Convenience helper to provision the integration's default custom metadata structures."""

    manager = CustomMetadataManager.from_config(config)
    manager.ensure_specifications()


def main(argv: Optional[List[str]] = None) -> None:  # pragma: no cover - CLI glue
    parser = argparse.ArgumentParser(description="DXR custom metadata utilities.")
    subparsers = parser.add_subparsers(dest="command")

    provision_parser = subparsers.add_parser(
        "provision",
        help="Ensure DXR custom metadata sets exist in Atlan.",
    )
    provision_parser.add_argument(
        "--log-level",
        default=None,
        help="Optional logging level override (e.g. DEBUG).",
    )

    args = parser.parse_args(argv)
    if args.command != "provision":
        parser.print_help()
        raise SystemExit(1)

    config = Config.from_env(require_all=False)
    if not config.atlan_base_url or not config.atlan_api_token:
        raise SystemExit("ATLAN_BASE_URL and ATLAN_API_TOKEN must be configured.")

    log_level = (args.log_level or config.log_level).upper()
    logging.basicConfig(level=log_level)

    ensure_default_sets(config)


if __name__ == "__main__":  # pragma: no cover - CLI glue
    main()


__all__ = [
    "AttributeSpec",
    "MetadataSetSpec",
    "CustomMetadataManager",
    "ensure_default_sets",
]
