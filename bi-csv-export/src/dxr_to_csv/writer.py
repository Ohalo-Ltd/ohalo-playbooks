"""Write the five normalised tables to CSV files in an output directory."""

from __future__ import annotations

import csv
import logging
import os
from pathlib import Path
from typing import Any, Dict, List

logger = logging.getLogger(__name__)

# Column order for each table.  Extra keys are appended at the end.
_COLUMN_ORDER: Dict[str, List[str]] = {
    "files": [
        "file_id",
        "file_name",
        "path",
        "size_bytes",
        "mime_type",
        "created_at",
        "last_modified_at",
        "content_sha256",
        "scan_depth",
        "datasource_id",
        "datasource_name",
        "datasource_connector_type_id",
        "owner_email",
        "created_by_email",
        "modified_by_email",
        "label_count",
        "annotation_count",
    ],
    "labels": [
        "file_id",
        "label_id",
        "label_name",
        "label_hex_color",
        "label_type",
        "label_description",
    ],
    "annotations": [
        "file_id",
        "annotator_id",
        "annotator_name",
        "value",
        "start",
        "end",
        "confidence",
    ],
    "extracted_metadata": [
        "file_id",
        "field_name",
        "value",
        "data_type",
    ],
    "dlp_labels": [
        "file_id",
        "dlp_label",
        "score",
        "category",
    ],
}


def write_tables(
    tables: Dict[str, List[Dict[str, Any]]],
    output_dir: str | Path,
) -> Dict[str, Path]:
    """Write each table to a CSV file in *output_dir*.

    Returns a mapping of table name → written file path.  Empty tables are
    still written (headers only) so downstream scripts can rely on consistent
    file presence.

    Args:
        tables: Dict returned by :meth:`~dxr_to_csv.splitter.Splitter.tables`.
        output_dir: Directory to write CSV files into.  Created if absent.
    """
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    written: Dict[str, Path] = {}
    for table_name, rows in tables.items():
        csv_path = out / f"{table_name}.csv"
        _write_csv(csv_path, rows, _COLUMN_ORDER.get(table_name, []))
        written[table_name] = csv_path
        logger.info("Wrote %d rows → %s", len(rows), csv_path)

    return written


def _write_csv(
    path: Path,
    rows: List[Dict[str, Any]],
    preferred_columns: List[str],
) -> None:
    """Write *rows* to *path* as UTF-8 CSV.

    Column order: preferred columns first (in order), then any extra keys
    found in the rows (sorted).  Empty tables get a header-only file.
    """
    # Collect the superset of all keys present in the data
    extra_keys: List[str] = sorted(
        k for k in ({k for r in rows for k in r} - set(preferred_columns))
    )
    fieldnames = preferred_columns + extra_keys

    # If there are no rows, fall back to the preferred columns as the header
    if not fieldnames:
        fieldnames = preferred_columns

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=fieldnames,
            extrasaction="ignore",
            restval="",
        )
        writer.writeheader()
        writer.writerows(rows)
