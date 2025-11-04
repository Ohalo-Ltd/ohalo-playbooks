"""Utilities for managing Atlan connections during development and testing."""

from __future__ import annotations

import argparse
import logging
from typing import Any, Dict, Iterable, List, Optional

from .atlan_service import AtlanRESTClient, AtlanRequestError
from .config import Config

LOGGER = logging.getLogger(__name__)


class DeleteType(str):
    HARD = "hard"
    PURGE = "purge"


_DELETE_TYPE_MAP = {
    "hard": DeleteType.HARD,
    "purge": DeleteType.PURGE,
}


def purge_connection(
    config: Config,
    *,
    delete_type: str = DeleteType.HARD,
    soft_delete_first: bool = True,
    purge_tags: bool = False,
    client: Optional[AtlanRESTClient] = None,
) -> None:
    """Hard-delete matching Atlan connections for development reset scenarios."""

    client = client or AtlanRESTClient(
        base_url=config.atlan_base_url,
        api_key=config.atlan_api_token,
    )

    connections = _find_connections(client, config)
    if not connections:
        raise SystemExit("No matching connections found; nothing to purge.")

    LOGGER.info("Purging %d connection(s) from Atlan", len(connections))

    for connection in connections:
        guid = connection.get("guid")
        attrs = connection.get("attributes") or {}
        name = attrs.get("name")
        qualified_name = attrs.get("qualifiedName")
        display = name or qualified_name or guid or "unknown"
        if not guid:
            raise SystemExit(
                f"Connection '{display}' does not expose a GUID; cannot delete."
            )

        # Clean up associated assets/domains prior to deleting the connection itself.
        _purge_files_for_connection(
            client,
            connection_qualified_name=qualified_name or "",
            connection_name=display,
            delete_type=delete_type,
            soft_delete_first=soft_delete_first,
        )
        _purge_tables_for_connection(
            client,
            connection_qualified_name=qualified_name or "",
            connection_name=display,
            delete_type=delete_type,
            soft_delete_first=soft_delete_first,
        )
        domain_guids = attrs.get("domainGuids") or attrs.get("domainGUIDs") or []
        _purge_domains(
            client,
            domain_guids=domain_guids,
            delete_type=delete_type,
            soft_delete_first=soft_delete_first,
        )

        if soft_delete_first:
            LOGGER.info("Soft-deleting connection '%s' (guid=%s)", display, guid)
            _delete_asset(client, guid)

        LOGGER.info(
            "Performing %s deletion of connection '%s' (guid=%s)",
            delete_type,
            display,
            guid,
        )
        _purge_asset(client, guid)

    if purge_tags:
        try:
            _purge_tag_typedefs(
                client,
                namespace=config.atlan_tag_namespace,
            )
        except Exception:  # pragma: no cover - defensive logging
            LOGGER.exception("Failed to purge classification typedefs")
            raise


def _find_connections(client: AtlanRESTClient, config: Config) -> List[Dict[str, Any]]:
    payload = {
        "dsl": {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"__typeName.keyword": "Connection"}},
                    ]
                }
            },
            "size": 1000,
        }
    }
    try:
        results = client.search_assets(payload)
    except AtlanRequestError as exc:
        raise SystemExit(f"Failed to search for connections: {exc}") from exc

    entities = _extract_entities(results)
    targets: List[Dict[str, Any]] = []
    for entity in entities:
        attrs = entity.get("attributes") or {}
        name = attrs.get("name")
        if _matches_connection(name, config):
            targets.append(entity)
    return targets


def _purge_files_for_connection(
    client: AtlanRESTClient,
    *,
    connection_qualified_name: str,
    connection_name: str,
    delete_type: str,
    soft_delete_first: bool,
) -> None:
    if not connection_qualified_name:
        return

    payload = {
        "dsl": {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"__typeName.keyword": "File"}},
                        {"term": {"connectionQualifiedName": connection_qualified_name}},
                    ]
                }
            },
            "size": 1000,
        }
    }
    try:
        results = client.search_assets(payload)
    except AtlanRequestError as exc:
        LOGGER.warning(
            "Unable to enumerate files for connection '%s': %s",
            connection_name,
            exc,
        )
        return

    entities = _extract_entities(results)
    if not entities:
        return

    LOGGER.info(
        "Deleting %d file asset(s) under connection '%s'",
        len(entities),
        connection_name,
    )
    for entity in entities:
        guid = entity.get("guid")
        if not guid:
            continue
        if soft_delete_first:
            _delete_asset(client, guid)
        _purge_asset(client, guid)


def _purge_tables_for_connection(
    client: AtlanRESTClient,
    *,
    connection_qualified_name: str,
    connection_name: str,
    delete_type: str,
    soft_delete_first: bool,
) -> None:
    if not connection_qualified_name:
        return

    payload = {
        "dsl": {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"__typeName.keyword": "Table"}},
                        {"term": {"connectionQualifiedName": connection_qualified_name}},
                    ]
                }
            },
            "size": 1000,
        }
    }

    try:
        results = client.search_assets(payload)
    except AtlanRequestError as exc:
        LOGGER.warning(
            "Unable to enumerate tables for connection '%s': %s",
            connection_name,
            exc,
        )
        return

    entities = _extract_entities(results)
    if not entities:
        return

    LOGGER.info(
        "Deleting %d table asset(s) under connection '%s'",
        len(entities),
        connection_name,
    )
    for entity in entities:
        guid = entity.get("guid")
        if not guid:
            continue
        if soft_delete_first:
            _delete_asset(client, guid)
        _purge_asset(client, guid)


def _purge_domains(
    client: AtlanRESTClient,
    *,
    domain_guids: Iterable[str],
    delete_type: str,
    soft_delete_first: bool,
) -> None:
    unique_guids = {str(guid) for guid in domain_guids if guid}
    if not unique_guids:
        return

    LOGGER.info("Purging %d domain(s)", len(unique_guids))
    for guid in unique_guids:
        if soft_delete_first:
            try:
                _delete_asset(client, guid)
            except AtlanRequestError as exc:
                LOGGER.warning(
                    "Unable to soft delete domain %s: %s", guid, exc
                )
        try:
            _purge_asset(client, guid)
        except AtlanRequestError as exc:
            LOGGER.warning("Unable to purge domain %s: %s", guid, exc)


def _purge_tag_typedefs(
    client: AtlanRESTClient,
    *,
    namespace: str,
) -> None:
    prefix = (namespace or "").strip()
    if not prefix:
        LOGGER.info("Tag namespace not configured; skipping classification purge.")
        return

    try:
        typedefs = client.list_classification_typedefs()
    except AtlanRequestError as exc:
        LOGGER.warning("Unable to enumerate classification typedefs: %s", exc)
        return

    display_prefix = f"{prefix} ::"
    to_purge: List[tuple[str, str]] = []
    for typedef in typedefs:
        display = str(typedef.get("displayName") or "")
        hashed = str(typedef.get("name") or "")
        if not hashed:
            continue
        if display.startswith(display_prefix) or hashed.startswith(prefix):
            to_purge.append((hashed, display or hashed))

    if not to_purge:
        LOGGER.info(
            "No classification typedefs found for namespace '%s'; nothing to purge.",
            prefix,
        )
        return

    LOGGER.info(
        "Purging %d classification typedef(s) for namespace '%s'.",
        len(to_purge),
        prefix,
    )
    for hashed, display in to_purge:
        try:
            client.purge_typedef(hashed)
        except AtlanRequestError as exc:
            LOGGER.warning(
                "Unable to purge classification '%s' (%s): %s",
                display,
                hashed,
                exc.details or exc,
            )


def _matches_connection(name: Optional[str], config: Config) -> bool:
    if not name:
        return False
    if name == config.atlan_global_connection_name:
        return True
    prefix = config.atlan_datasource_connection_prefix
    if prefix and name.startswith(prefix):
        return True
    return False


def _extract_entities(response: Any) -> List[Dict[str, Any]]:
    if not isinstance(response, dict):
        return []
    entities = response.get("entities")
    if isinstance(entities, list):
        return [entity for entity in entities if isinstance(entity, dict)]
    entity = response.get("entity")
    if isinstance(entity, dict):
        return [entity]
    return []


def _delete_asset(client: AtlanRESTClient, guid: str) -> None:
    try:
        client.delete_asset(guid)
    except AtlanRequestError as exc:
        LOGGER.warning("Unable to delete asset %s: %s", guid, exc.details or exc)


def _purge_asset(client: AtlanRESTClient, guid: str) -> None:
    try:
        client.purge_asset(guid)
    except AtlanRequestError as exc:
        LOGGER.warning("Unable to purge asset %s: %s", guid, exc.details or exc)


def _parse_delete_type(raw: str) -> str:
    key = raw.lower()
    if key not in _DELETE_TYPE_MAP:
        valid = ", ".join(sorted(_DELETE_TYPE_MAP))
        raise SystemExit(f"Unsupported delete type '{raw}'. Valid options: {valid}.")
    return _DELETE_TYPE_MAP[key]


def purge_environment(
    config: Config,
    *,
    client: Optional[AtlanRESTClient] = None,
) -> None:
    """Convenience helper to hard-delete DXR assets in development environments."""

    purge_connection(
        config,
        delete_type=DeleteType.PURGE,
        soft_delete_first=False,
        purge_tags=True,
        client=client,
    )


def main(argv: Optional[list[str]] = None) -> None:  # pragma: no cover - CLI glue
    parser = argparse.ArgumentParser(
        description="Utility for deleting the configured Atlan connection."
    )
    parser.add_argument(
        "--delete-type",
        default="hard",
        choices=sorted(_DELETE_TYPE_MAP),
        help="Deletion strategy to use (default: hard).",
    )
    parser.add_argument(
        "--skip-soft-delete",
        action="store_true",
        help="Invoke the hard delete directly without issuing a preceding soft delete.",
    )
    parser.add_argument(
        "--purge-tags",
        action="store_true",
        help="Also purge classification typedefs matching the configured namespace.",
    )
    parser.add_argument(
        "--log-level",
        default=None,
        help="Optional logging level override (e.g. DEBUG).",
    )
    args = parser.parse_args(argv)

    config = Config.from_env()
    logging.basicConfig(level=(args.log_level or config.log_level).upper())

    try:
        purge_connection(
            config,
            delete_type=_parse_delete_type(args.delete_type),
            soft_delete_first=not args.skip_soft_delete,
            purge_tags=args.purge_tags,
        )
    except SystemExit:
        raise
    except Exception as exc:  # pragma: no cover - defensive log
        LOGGER.error("Connection purge failed: %s", exc)
        raise SystemExit(1) from exc


if __name__ == "__main__":  # pragma: no cover - CLI glue
    main()


__all__ = ["purge_connection", "purge_environment"]
