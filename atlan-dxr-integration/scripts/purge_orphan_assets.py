"""Remove DXR artefacts (connections, assets, tags, domains) from the dev tenant."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Iterable

from dotenv import load_dotenv
from pyatlan.client.atlan import AtlanClient
from pyatlan.errors import AtlanError
from pyatlan.errors import AtlanError
from pyatlan.model.assets.connection import Connection
from pyatlan.model.assets.core.data_domain import DataDomain
from pyatlan.model.assets.core.file import File
from pyatlan.model.assets.core.table import Table
from pyatlan.model.enums import AtlanDeleteType, AtlanTypeCategory
from pyatlan.model.search import Bool, DSL, IndexSearchRequest, Prefix, Term
from pyatlan.model.typedef import AtlanTagDef

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from atlan_dxr_integration.config import Config

LOGGER = logging.getLogger("purge_orphan_assets")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--delete-type",
        default="purge",
        choices=[member.value.lower() for member in AtlanDeleteType],
        help="Deletion strategy to use (default: purge).",
    )
    parser.add_argument(
        "--skip-soft-delete",
        action="store_true",
        help="Delete assets without issuing an initial soft delete.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Optional logging level override (e.g. DEBUG).",
    )
    return parser.parse_args()


def _delete_assets(
    client: AtlanClient,
    *,
    asset_type,
    query: Bool,
    delete_type: AtlanDeleteType,
    soft_delete_first: bool,
) -> None:
    request = IndexSearchRequest(dsl=DSL(query=query, size=1000))
    try:
        results = client.asset.search(request)
    except AtlanError as exc:  # pragma: no cover - defensive logging
        LOGGER.warning("Search for %s assets failed: %s", asset_type.__name__, exc)
        return

    entities = getattr(results, "entities", []) or []
    if not entities:
        LOGGER.info("No %s assets matched the purge filter.", asset_type.__name__)
        return

    LOGGER.info("Deleting %d %s asset(s).", len(entities), asset_type.__name__)
    for entity in entities:
        guid = getattr(entity, "guid", None)
        attrs = getattr(entity, "attributes", None)
        qualified_name = getattr(attrs, "qualified_name", None) if attrs else None
        if not guid:
            LOGGER.warning(
                "Skipping %s asset without GUID (qualifiedName=%s)",
                asset_type.__name__,
                qualified_name,
            )
            continue
        if soft_delete_first:
            client.asset.delete_by_guid(guid)
        client.asset.purge_by_guid(guid, delete_type=delete_type)
        LOGGER.debug("Deleted %s asset %s", asset_type.__name__, qualified_name or guid)


def _prefix(prefix: str) -> str:
    return prefix if prefix.endswith("/") else f"{prefix}/"


def _purge_tags(client: AtlanClient, namespace: str) -> None:
    namespace = namespace.strip()
    if not namespace:
        LOGGER.warning("Skipping tag purge: namespace is empty.")
        return

    display_prefix = f"{namespace} ::"
    try:
        typedefs = client.typedef.get(type_category=AtlanTypeCategory.CLASSIFICATION)
    except AtlanError as exc:
        LOGGER.warning("Unable to retrieve classification typedefs: %s", exc)
        return

    definitions = getattr(typedefs, "atlan_tag_defs", None) or []
    targets = [td for td in definitions if (td.display_name or "").startswith(display_prefix)]

    if not targets:
        LOGGER.info("No Atlan tags found with namespace '%s'.", namespace)
        return

    LOGGER.info("Purging %d Atlan tag(s) in namespace '%s'.", len(targets), namespace)
    for tag_def in targets:
        try:
            client.typedef.purge(name=tag_def.name, typedef_type=AtlanTagDef)
            LOGGER.debug("Purged tag '%s' (id=%s)", tag_def.display_name, tag_def.name)
        except AtlanError as exc:
            LOGGER.warning("Failed to purge tag '%s': %s", tag_def.display_name, exc)


def _purge_domains(client: AtlanClient, names: Iterable[str], prefix: Optional[str]) -> None:
    queries = []
    for name in names:
        name = (name or "").strip()
        if name:
            queries.append(Term(field="name.keyword", value=name))

    if prefix:
        queries.append(Prefix(field="name.keyword", value=prefix))

    if not queries:
        LOGGER.debug("No domain names supplied; skipping domain purge.")
        return

    request = IndexSearchRequest(
        dsl=DSL(
            query=Bool(should=queries, minimum_should_match=1, must=[Term(field="__typeName.keyword", value="DataDomain")]),
            size=500,
        )
    )

    try:
        results = client.asset.search(request)
    except AtlanError as exc:
        LOGGER.warning("Domain search failed: %s", exc)
        return

    entities = getattr(results, "entities", []) or []
    if not entities:
        LOGGER.info("No matching data domains found to purge.")
        return

    LOGGER.info("Purging %d data domain(s).", len(entities))
    for entity in entities:
        guid = getattr(entity, "guid", None)
        name = getattr(getattr(entity, "attributes", None), "name", None)
        if not guid:
            continue
        try:
            client.asset.purge_by_guid(guid, delete_type=AtlanDeleteType.PURGE)
            LOGGER.debug("Purged domain '%s' (%s)", name or guid, guid)
        except AtlanError as exc:
            LOGGER.warning("Failed to purge domain '%s': %s", name or guid, exc)


def main() -> None:
    args = _parse_args()
    logging.basicConfig(level=args.log_level.upper())

    load_dotenv()
    config = Config.from_env()
    client = AtlanClient(api_key=config.atlan_api_token, base_url=config.atlan_base_url)

    delete_type = AtlanDeleteType(args.delete_type.upper())
    soft_delete_first = not args.skip_soft_delete

    # Purge Atlan tags with the DXR namespace.
    _purge_tags(client, namespace=config.atlan_tag_namespace)

    # Drop dataset tables beneath the global connection.
    tables_query = Bool(
        must=[
            Term(field="__typeName.keyword", value="Table"),
            Prefix(
                field="qualifiedName",
                value=_prefix(config.atlan_global_connection_qualified_name),
            ),
        ]
    )
    _delete_assets(
        client,
        asset_type=Table,
        query=tables_query,
        delete_type=delete_type,
        soft_delete_first=soft_delete_first,
    )

    # Drop file assets for any connections that share the configured namespace.
    namespace_prefix = f"{config.global_connection_namespace}/"
    files_query = Bool(
        must=[
            Term(field="__typeName.keyword", value="File"),
            Prefix(field="connectionQualifiedName", value=namespace_prefix),
        ]
    )
    _delete_assets(
        client,
        asset_type=File,
        query=files_query,
        delete_type=delete_type,
        soft_delete_first=soft_delete_first,
    )

    # Drop connections themselves.
    connections_query = Bool(
        must=[
            Term(field="__typeName.keyword", value="Connection"),
            Prefix(field="qualifiedName", value=_prefix(config.global_connection_namespace)),
        ]
    )
    _delete_assets(
        client,
        asset_type=Connection,
        query=connections_query,
        delete_type=delete_type,
        soft_delete_first=soft_delete_first,
    )

    # Purge domains created for DXR.
    _purge_domains(
        client,
        names=[config.atlan_global_domain_name],
        prefix=(config.atlan_datasource_domain_prefix or "").strip() or None,
    )


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    try:
        main()
    except AtlanError as exc:  # pragma: no cover - defensive exit
        LOGGER.error("Failed to purge DXR assets: %s", exc)
        raise SystemExit(1) from exc
