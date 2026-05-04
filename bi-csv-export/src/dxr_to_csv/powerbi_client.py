"""Power BI Push Datasets client.

Authenticates via MSAL device-code flow (interactive) or client-credentials
(service principal) and pushes rows into a Power BI Push Dataset.

Typical flow:

    client = PowerBIClient(tenant_id, client_id, workspace_id)
    client.authenticate()          # opens browser / prints device code

    dataset_id = client.get_or_create_dataset("DXR Metadata")
    client.clear_table(dataset_id, "files")
    client.push_rows(dataset_id, "files", rows)     # list[dict]
    ...

Dependencies:
    pip install msal requests

Power BI REST API reference:
    https://learn.microsoft.com/en-us/rest/api/power-bi/
"""

from __future__ import annotations

import logging
import time
from typing import Iterator, Sequence

import requests

logger = logging.getLogger(__name__)

_PBI_API = "https://api.powerbi.com/v1.0/myorg"
_PBI_SCOPE = ["https://analysis.windows.net/powerbi/api/Dataset.ReadWrite.All"]
_PUSH_BATCH = 9_000  # Power BI max is 10 000 rows per call; stay under

# ---------------------------------------------------------------------------
# Dataset schema
# ---------------------------------------------------------------------------

#: Minimal column definitions keyed by table name.  Power BI supports
#: Int64, Double, Boolean, Datetime, String for Push Datasets.
TABLE_SCHEMAS: dict[str, list[dict]] = {
    "files": [
        {"name": "file_id", "dataType": "String"},
        {"name": "file_name", "dataType": "String"},
        {"name": "path", "dataType": "String"},
        {"name": "size_bytes", "dataType": "Int64"},
        {"name": "mime_type", "dataType": "String"},
        {"name": "created_at", "dataType": "Datetime"},
        {"name": "modified_at", "dataType": "Datetime"},
        {"name": "datasource_id", "dataType": "Int64"},
        {"name": "datasource_name", "dataType": "String"},
        {"name": "owner_email", "dataType": "String"},
        {"name": "label_count", "dataType": "Int64"},
        {"name": "annotation_count", "dataType": "Int64"},
    ],
    "labels": [
        {"name": "file_id", "dataType": "String"},
        {"name": "label_id", "dataType": "Int64"},
        {"name": "label_name", "dataType": "String"},
        {"name": "label_hex_color", "dataType": "String"},
        {"name": "label_type", "dataType": "String"},
    ],
    "annotations": [
        {"name": "file_id", "dataType": "String"},
        {"name": "annotator_id", "dataType": "Int64"},
        {"name": "annotator_name", "dataType": "String"},
        {"name": "value", "dataType": "String"},
        {"name": "start", "dataType": "Int64"},
        {"name": "end", "dataType": "Int64"},
    ],
    "extracted_metadata": [
        {"name": "file_id", "dataType": "String"},
        {"name": "extractor_id", "dataType": "Int64"},
        {"name": "extractor_name", "dataType": "String"},
        {"name": "field_name", "dataType": "String"},
        {"name": "value", "dataType": "String"},
        {"name": "data_type", "dataType": "String"},
    ],
    "dlp_labels": [
        {"name": "file_id", "dataType": "String"},
        {"name": "dlp_label", "dataType": "String"},
        {"name": "score", "dataType": "Double"},
        {"name": "category", "dataType": "String"},
    ],
}

#: Relationships that Power BI will create between the five tables.
RELATIONSHIPS: list[dict] = [
    {
        "name": "files_labels",
        "fromTable": "labels",
        "fromColumn": "file_id",
        "toTable": "files",
        "toColumn": "file_id",
        "crossFilteringBehavior": "oneDirection",
    },
    {
        "name": "files_annotations",
        "fromTable": "annotations",
        "fromColumn": "file_id",
        "toTable": "files",
        "toColumn": "file_id",
        "crossFilteringBehavior": "oneDirection",
    },
    {
        "name": "files_extracted_metadata",
        "fromTable": "extracted_metadata",
        "fromColumn": "file_id",
        "toTable": "files",
        "toColumn": "file_id",
        "crossFilteringBehavior": "oneDirection",
    },
    {
        "name": "files_dlp_labels",
        "fromTable": "dlp_labels",
        "fromColumn": "file_id",
        "toTable": "files",
        "toColumn": "file_id",
        "crossFilteringBehavior": "oneDirection",
    },
]


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------


class PowerBIClient:
    """Thin wrapper around the Power BI REST API for Push Datasets.

    Supports two authentication modes:

    * **Device code** (default) — interactive sign-in printed to stdout.
      Suitable for ad-hoc exports and first-time runs.
    * **Client credentials** — non-interactive, for scheduled / CI use.
      Requires ``client_secret`` to be set.

    Args:
        tenant_id:     Azure AD tenant ID.
        client_id:     Azure AD app registration client ID.
        workspace_id:  Power BI workspace (group) ID where the dataset lives.
        client_secret: Service-principal secret for client-credentials flow.
                       Leave ``None`` to use device-code authentication.
    """

    def __init__(
        self,
        tenant_id: str,
        client_id: str,
        workspace_id: str,
        client_secret: str | None = None,
    ) -> None:
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.workspace_id = workspace_id
        self.client_secret = client_secret
        self._access_token: str | None = None

    # ------------------------------------------------------------------
    # Authentication
    # ------------------------------------------------------------------

    def authenticate(self) -> None:
        """Acquire an access token.

        Uses device-code flow when ``client_secret`` is ``None``, otherwise
        uses client-credentials flow (service principal).
        """
        try:
            import msal  # type: ignore[import]
        except ImportError as exc:
            raise ImportError(
                "msal is required for Power BI publishing.  "
                "Install it with: pip install msal"
            ) from exc

        authority = f"https://login.microsoftonline.com/{self.tenant_id}"

        if self.client_secret:
            app = msal.ConfidentialClientApplication(
                self.client_id,
                authority=authority,
                client_credential=self.client_secret,
            )
            result = app.acquire_token_for_client(scopes=_PBI_SCOPE)
        else:
            app = msal.PublicClientApplication(
                self.client_id,
                authority=authority,
            )
            # Try silent acquisition first (cached token)
            accounts = app.get_accounts()
            result = None
            if accounts:
                result = app.acquire_token_silent(_PBI_SCOPE, account=accounts[0])

            if not result:
                flow = app.initiate_device_flow(scopes=_PBI_SCOPE)
                if "user_code" not in flow:
                    raise RuntimeError(f"Failed to initiate device flow: {flow}")
                print(flow["message"])  # noqa: T201  — must be visible to user
                result = app.acquire_token_by_device_flow(flow)

        if "access_token" not in result:
            error = result.get("error_description") or result.get("error")
            raise RuntimeError(f"Authentication failed: {error}")

        self._access_token = result["access_token"]
        logger.info("Power BI authentication successful")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _headers(self) -> dict[str, str]:
        if not self._access_token:
            raise RuntimeError(
                "Not authenticated — call client.authenticate() first"
            )
        return {
            "Authorization": f"Bearer {self._access_token}",
            "Content-Type": "application/json",
        }

    def _url(self, path: str) -> str:
        return f"{_PBI_API}/groups/{self.workspace_id}/{path.lstrip('/')}"

    def _get(self, path: str) -> dict:
        r = requests.get(self._url(path), headers=self._headers(), timeout=60)
        r.raise_for_status()
        return r.json()

    def _post(self, path: str, body: dict) -> dict:
        r = requests.post(
            self._url(path), headers=self._headers(), json=body, timeout=60
        )
        r.raise_for_status()
        return r.json()

    def _delete(self, path: str) -> None:
        r = requests.delete(self._url(path), headers=self._headers(), timeout=60)
        r.raise_for_status()

    # ------------------------------------------------------------------
    # Dataset management
    # ------------------------------------------------------------------

    def list_datasets(self) -> list[dict]:
        """Return all datasets in the workspace."""
        return self._get("datasets").get("value", [])

    def get_or_create_dataset(
        self,
        dataset_name: str,
        *,
        default_mode: str = "Push",
    ) -> str:
        """Return the dataset ID, creating the dataset if it doesn't exist.

        Args:
            dataset_name:  Display name for the dataset in the workspace.
            default_mode:  Typically ``"Push"`` (real-time streaming).

        Returns:
            The Power BI dataset ID (GUID string).
        """
        for ds in self.list_datasets():
            if ds.get("name") == dataset_name:
                logger.info("Using existing dataset '%s' (%s)", dataset_name, ds["id"])
                return ds["id"]

        logger.info("Creating dataset '%s' in workspace %s", dataset_name, self.workspace_id)
        body = {
            "name": dataset_name,
            "defaultMode": default_mode,
            "tables": [
                {"name": name, "columns": cols}
                for name, cols in TABLE_SCHEMAS.items()
            ],
            "relationships": RELATIONSHIPS,
        }
        result = self._post("datasets", body)
        dataset_id: str = result["id"]
        logger.info("Created dataset '%s' (%s)", dataset_name, dataset_id)
        return dataset_id

    # ------------------------------------------------------------------
    # Row operations
    # ------------------------------------------------------------------

    def clear_table(self, dataset_id: str, table_name: str) -> None:
        """Delete all rows in a Push Dataset table (pre-push refresh).

        Args:
            dataset_id:  Dataset GUID.
            table_name:  One of the five table names (e.g. ``"files"``).
        """
        logger.debug("Clearing table '%s' in dataset %s", table_name, dataset_id)
        try:
            self._delete(f"datasets/{dataset_id}/tables/{table_name}/rows")
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 400:
                # Table may have no rows — not an error
                logger.debug("Clear returned 400 (table may be empty) — continuing")
            else:
                raise

    def push_rows(
        self,
        dataset_id: str,
        table_name: str,
        rows: Sequence[dict],
        *,
        clear_first: bool = False,
    ) -> int:
        """Push rows into a Power BI Push Dataset table in batches.

        Args:
            dataset_id:   Dataset GUID.
            table_name:   Target table name.
            rows:         List of row dicts — keys must match column names.
            clear_first:  If ``True``, delete existing rows before pushing.

        Returns:
            Total number of rows pushed.
        """
        if clear_first:
            self.clear_table(dataset_id, table_name)

        if not rows:
            logger.info("No rows for table '%s' — skipping", table_name)
            return 0

        total = 0
        for batch in _batched(rows, _PUSH_BATCH):
            self._post(
                f"datasets/{dataset_id}/tables/{table_name}/rows",
                {"rows": list(batch)},
            )
            total += len(batch)
            logger.debug("  pushed %d rows to '%s' (running total: %d)", len(batch), table_name, total)
            # Brief pause to avoid hitting the 120 requests/min rate limit
            if total < len(rows):
                time.sleep(0.3)

        logger.info("Pushed %d rows to table '%s'", total, table_name)
        return total


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------


def _batched(items: Sequence, size: int) -> Iterator[Sequence]:
    """Yield successive fixed-size slices from ``items``."""
    for start in range(0, len(items), size):
        yield items[start : start + size]
