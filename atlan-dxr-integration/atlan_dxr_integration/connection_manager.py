"""Utilities for managing Atlan connections during development and testing."""

from __future__ import annotations

import argparse
import logging
from types import SimpleNamespace
from typing import Optional

from pyatlan.client.atlan import AtlanClient
from pyatlan.errors import AtlanError, NotFoundError
from pyatlan.model.assets.connection import Connection
from pyatlan.model.enums import AtlanDeleteType

from .config import Config

LOGGER = logging.getLogger(__name__)

_DELETE_TYPE_MAP = {
    "hard": AtlanDeleteType.HARD,
    "purge": AtlanDeleteType.PURGE,
}


def purge_connection(
    config: Config,
    *,
    delete_type: AtlanDeleteType = AtlanDeleteType.HARD,
    soft_delete_first: bool = True,
    client: Optional[AtlanClient] = None,
) -> None:
    """Hard-delete the configured Atlan connection and (optionally) its assets.

    Parameters align with Atlan's guidance for removing connections during
    automated testing (see https://developer.atlan.com/.../connection-delete/).
    """

    client = client or AtlanClient(
        base_url=config.atlan_base_url,
        api_key=config.atlan_api_token,
    )

    qualified_name = config.atlan_connection_qualified_name
    LOGGER.info(
        "Looking up connection '%s' prior to deletion", qualified_name
    )

    try:
        connection: Connection = client.asset.get_by_qualified_name(
            qualified_name,
            Connection,
            ignore_relationships=True,
        )
    except NotFoundError:
        raise SystemExit(
            f"Connection '{qualified_name}' not found; nothing to purge."
        )
    except AtlanError as exc:
        raise SystemExit(f"Failed to load connection '{qualified_name}': {exc}") from exc

    guid = getattr(connection, "guid", None)
    if not guid:
        raise SystemExit(
            f"Connection '{qualified_name}' does not expose a GUID; cannot delete."
        )

    if soft_delete_first:
        LOGGER.info("Soft-deleting connection '%s' (guid=%s)", qualified_name, guid)
        response = client.asset.delete_by_guid(guid)
        _log_mutation_result("soft delete", response)

    LOGGER.info(
        "Performing %s deletion of connection '%s' (guid=%s)",
        delete_type.value.lower(),
        qualified_name,
        guid,
    )
    response = client.asset.purge_by_guid(guid, delete_type=delete_type)
    _log_mutation_result(f"{delete_type.value.lower()} delete", response)


def _log_mutation_result(operation: str, response) -> None:
    request_id = getattr(response, "request_id", None)
    if request_id:
        LOGGER.info("Atlan acknowledged %s (request_id=%s)", operation, request_id)
    else:
        LOGGER.info("Atlan acknowledged %s", operation)


def _parse_delete_type(raw: str) -> AtlanDeleteType:
    key = raw.lower()
    if key not in _DELETE_TYPE_MAP:
        valid = ", ".join(sorted(_DELETE_TYPE_MAP))
        raise SystemExit(f"Unsupported delete type '{raw}'. Valid options: {valid}.")
    return _DELETE_TYPE_MAP[key]


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
        )
    except SystemExit:
        raise
    except Exception as exc:  # pragma: no cover - defensive log
        LOGGER.error("Connection purge failed: %s", exc)
        raise SystemExit(1) from exc


if __name__ == "__main__":  # pragma: no cover - CLI glue
    main()


__all__ = ["purge_connection"]

