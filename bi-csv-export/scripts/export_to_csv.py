#!/usr/bin/env python3
"""Export Data X-Ray file metadata to analysis-ready CSV tables.

Reads every file record from GET /api/v1/files and writes five normalised
CSV files to an output directory:

    files.csv              — one row per file (scalar fields)
    labels.csv             — one row per (file, smart label) pair
    annotations.csv        — one row per (file, annotator, value) triple
    extracted_metadata.csv — one row per (file, extractor field) pair
    dlp_labels.csv         — one row per (file, DLP label) pair

Configuration is read from environment variables or command-line arguments.
Run with ``--help`` for full usage.

Quickstart::

    export DXR_BASE_URL=https://your-dxr-instance.example.com
    export DXR_BEARER_TOKEN=your-pat-token
    python scripts/export_to_csv.py --output-dir ./output

To filter by datasource or label::

    python scripts/export_to_csv.py \\
        --output-dir ./output \\
        --query "datasource.id:123"
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

# Allow running the script directly from the repo root without pip-installing
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from dxr_to_csv.client import DataXRayClient
from dxr_to_csv.splitter import Splitter
from dxr_to_csv.writer import write_tables


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export Data X-Ray metadata to BI-ready CSV tables",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--url",
        dest="base_url",
        default=os.getenv("DXR_BASE_URL"),
        help="Data X-Ray base URL (env: DXR_BASE_URL)",
    )
    parser.add_argument(
        "--token",
        dest="bearer_token",
        default=os.getenv("DXR_BEARER_TOKEN"),
        help="Bearer token / PAT (env: DXR_BEARER_TOKEN)",
    )
    parser.add_argument(
        "--output-dir",
        default=os.getenv("DXR_OUTPUT_DIR", "./output"),
        help="Directory to write CSV files into (env: DXR_OUTPUT_DIR, default: ./output)",
    )
    parser.add_argument(
        "--query",
        default=os.getenv("DXR_QUERY"),
        help="KQL filter applied to /api/v1/files (env: DXR_QUERY)",
    )
    parser.add_argument(
        "--record-cap",
        type=int,
        default=int(os.getenv("DXR_RECORD_CAP", "0")) or None,
        help="Stop after N records — useful for sampling (env: DXR_RECORD_CAP)",
    )
    parser.add_argument(
        "--no-verify-ssl",
        action="store_true",
        default=os.getenv("DXR_NO_VERIFY_SSL", "").lower() in {"1", "true", "yes"},
        help="Disable TLS certificate verification (env: DXR_NO_VERIFY_SSL)",
    )
    parser.add_argument(
        "--http-timeout",
        type=int,
        default=int(os.getenv("DXR_HTTP_TIMEOUT", "120")),
        help="HTTP connection timeout in seconds (env: DXR_HTTP_TIMEOUT, default: 120)",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable debug logging",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logger = logging.getLogger("export_to_csv")

    if not args.base_url:
        sys.exit("ERROR: DXR base URL is required. Set DXR_BASE_URL or pass --url.")
    if not args.bearer_token:
        sys.exit("ERROR: Bearer token is required. Set DXR_BEARER_TOKEN or pass --token.")

    client = DataXRayClient(
        base_url=args.base_url,
        bearer_token=args.bearer_token,
        http_timeout=args.http_timeout,
        verify_ssl=not args.no_verify_ssl,
    )

    splitter = Splitter()
    logger.info("Streaming file metadata from %s/api/v1/files …", args.base_url)

    for record in client.stream_files(query=args.query, record_cap=args.record_cap):
        splitter.ingest(record)

    logger.info("Processed %d file records — writing CSV tables …", splitter.file_count)

    written = write_tables(splitter.tables(), args.output_dir)

    print(f"\n✓ Export complete — {splitter.file_count} files → {args.output_dir}/")
    for name, path in written.items():
        row_count = sum(1 for _ in open(path, encoding="utf-8")) - 1  # minus header
        print(f"  {path.name:<30} {row_count:>8} rows")


if __name__ == "__main__":
    main()
