#!/usr/bin/env python3
"""Push Data X-Ray file metadata into a Power BI Push Dataset.

This script streams metadata from the DXR /api/v1/files endpoint, splits
it into five analysis-ready tables, and pushes them into a Power BI workspace
using the Push Datasets API.  Authentication uses MSAL device-code flow
(browser-based, interactive) or client-credentials (service principal).

Usage
-----
    # Interactive sign-in (device code):
    python publish_to_powerbi.py

    # Using environment variables (CI / service principal):
    POWERBI_CLIENT_SECRET=... python publish_to_powerbi.py --non-interactive

    # Filter to a specific datasource:
    python publish_to_powerbi.py --query "datasource.id:42"

Required environment variables (or pass as flags):
    DXR_BASE_URL          – Data X-Ray tenant URL
    DXR_BEARER_TOKEN      – DXR personal access token
    POWERBI_TENANT_ID     – Azure AD tenant ID
    POWERBI_CLIENT_ID     – Azure AD app registration client ID
    POWERBI_WORKSPACE_ID  – Power BI workspace (group) GUID

Optional:
    POWERBI_CLIENT_SECRET – Service-principal secret (skips device-code)
    POWERBI_DATASET_NAME  – Dataset display name (default: "DXR Metadata")
    DXR_QUERY             – KQL filter applied to the /files stream
    DXR_RECORD_CAP        – Max records to export (0 = no cap)
    DXR_NO_VERIFY_SSL     – Set to 1 to disable TLS verification
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from pathlib import Path

# ---------------------------------------------------------------------------
# Path setup — allow running directly without installing the package
# ---------------------------------------------------------------------------
_HERE = Path(__file__).resolve().parent
_SRC = _HERE.parent / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from dxr_to_csv.client import DataXRayClient          # noqa: E402
from dxr_to_csv.powerbi_client import PowerBIClient   # noqa: E402
from dxr_to_csv.splitter import Splitter              # noqa: E402

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Stream DXR file metadata and push to a Power BI Push Dataset.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    # DXR connection
    dxr = p.add_argument_group("Data X-Ray")
    dxr.add_argument("--url", default=os.environ.get("DXR_BASE_URL"), metavar="URL",
                     help="DXR tenant URL  [env: DXR_BASE_URL]")
    dxr.add_argument("--token", default=os.environ.get("DXR_BEARER_TOKEN"), metavar="TOKEN",
                     help="DXR bearer token  [env: DXR_BEARER_TOKEN]")
    dxr.add_argument("--query", default=os.environ.get("DXR_QUERY", ""), metavar="KQL",
                     help="KQL filter for /api/v1/files  [env: DXR_QUERY]")
    dxr.add_argument("--record-cap", type=int,
                     default=int(os.environ.get("DXR_RECORD_CAP", "0")), metavar="N",
                     help="Stop after N records (0 = no cap)  [env: DXR_RECORD_CAP]")
    dxr.add_argument("--no-verify-ssl", action="store_true",
                     default=bool(int(os.environ.get("DXR_NO_VERIFY_SSL", "0"))),
                     help="Disable TLS verification  [env: DXR_NO_VERIFY_SSL=1]")

    # Power BI connection
    pbi = p.add_argument_group("Power BI")
    pbi.add_argument("--tenant-id", default=os.environ.get("POWERBI_TENANT_ID"),
                     help="Azure AD tenant ID  [env: POWERBI_TENANT_ID]")
    pbi.add_argument("--client-id", default=os.environ.get("POWERBI_CLIENT_ID"),
                     help="Azure AD app client ID  [env: POWERBI_CLIENT_ID]")
    pbi.add_argument("--workspace-id", default=os.environ.get("POWERBI_WORKSPACE_ID"),
                     help="Power BI workspace GUID  [env: POWERBI_WORKSPACE_ID]")
    pbi.add_argument("--client-secret", default=os.environ.get("POWERBI_CLIENT_SECRET"),
                     help="Service-principal secret (non-interactive)  [env: POWERBI_CLIENT_SECRET]")
    pbi.add_argument("--dataset-name",
                     default=os.environ.get("POWERBI_DATASET_NAME", "DXR Metadata"),
                     help='Dataset display name  [env: POWERBI_DATASET_NAME]  (default: "DXR Metadata")')
    pbi.add_argument("--no-clear", action="store_true",
                     help="Append rows instead of clearing tables before pushing")

    p.add_argument("--debug", action="store_true", help="Enable debug logging")
    return p


def _validate(args: argparse.Namespace) -> list[str]:
    """Return a list of validation error messages (empty = OK)."""
    errors = []
    if not args.url:
        errors.append("DXR URL is required (--url or DXR_BASE_URL)")
    if not args.token:
        errors.append("DXR bearer token is required (--token or DXR_BEARER_TOKEN)")
    if not args.tenant_id:
        errors.append("Azure AD tenant ID is required (--tenant-id or POWERBI_TENANT_ID)")
    if not args.client_id:
        errors.append("Azure AD client ID is required (--client-id or POWERBI_CLIENT_ID)")
    if not args.workspace_id:
        errors.append("Power BI workspace ID is required (--workspace-id or POWERBI_WORKSPACE_ID)")
    return errors


# ---------------------------------------------------------------------------
# Main logic
# ---------------------------------------------------------------------------

def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    errors = _validate(args)
    if errors:
        for e in errors:
            print(f"  ✗  {e}", file=sys.stderr)
        parser.print_usage(sys.stderr)
        sys.exit(1)

    # ------------------------------------------------------------------
    # 1. Authenticate with Power BI
    # ------------------------------------------------------------------
    print("\n─── Power BI Authentication ───────────────────────────────")
    pbi = PowerBIClient(
        tenant_id=args.tenant_id,
        client_id=args.client_id,
        workspace_id=args.workspace_id,
        client_secret=args.client_secret or None,
    )
    pbi.authenticate()
    print("✓  Authenticated")

    # ------------------------------------------------------------------
    # 2. Resolve / create the Push Dataset
    # ------------------------------------------------------------------
    print(f"\n─── Dataset: {args.dataset_name!r} ─────────────────────────")
    dataset_id = pbi.get_or_create_dataset(args.dataset_name)
    print(f"✓  Dataset ID: {dataset_id}")

    # ------------------------------------------------------------------
    # 3. Stream DXR metadata and split into tables
    # ------------------------------------------------------------------
    print("\n─── Streaming from Data X-Ray ──────────────────────────────")
    dxr = DataXRayClient(
        base_url=args.url,
        bearer_token=args.token,
        verify_ssl=not args.no_verify_ssl,
    )

    splitter = Splitter()
    record_count = 0
    t0 = time.perf_counter()

    cap = args.record_cap if args.record_cap > 0 else None
    query = args.query.strip() or None

    for record in dxr.stream_files(query=query, record_cap=cap):
        splitter.ingest(record)
        record_count += 1
        if record_count % 1_000 == 0:
            elapsed = time.perf_counter() - t0
            print(f"  … {record_count:,} records in {elapsed:.1f}s", end="\r", flush=True)

    elapsed = time.perf_counter() - t0
    print(f"✓  Streamed {record_count:,} file records in {elapsed:.1f}s          ")

    tables = splitter.tables()

    # ------------------------------------------------------------------
    # 4. Push each table to Power BI
    # ------------------------------------------------------------------
    print(f"\n─── Pushing to Power BI  (clear_first={not args.no_clear}) ───────────")
    total_rows = 0
    table_order = ["files", "labels", "annotations", "extracted_metadata", "dlp_labels"]

    for table_name in table_order:
        rows = tables.get(table_name, [])
        pushed = pbi.push_rows(
            dataset_id,
            table_name,
            rows,
            clear_first=not args.no_clear,
        )
        total_rows += pushed
        status = f"{pushed:>8,} rows"
        print(f"  {table_name:<25}  {status}")

    elapsed_total = time.perf_counter() - t0
    print(f"\n✓  Done — {total_rows:,} total rows pushed in {elapsed_total:.1f}s")
    print(f"\n   Open Power BI Service: https://app.powerbi.com/groups/{args.workspace_id}/datasets")
    print()


if __name__ == "__main__":
    main()
