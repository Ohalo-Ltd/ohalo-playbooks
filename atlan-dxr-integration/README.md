# Atlan ↔ Data X-Ray integration

This playbook ingests Data X-Ray (DXR) metadata into Atlan using a layered connection
model:

- A **global `dxr-unstructured-attributes` connection** stores dataset-style summaries for
  each DXR classification and provisions the custom metadata structures that capture
  DXR-specific context (DLP labels, annotators, entitlements, and so on).
- A **per-datasource connection** is created for every datasource encountered while
  streaming file metadata. File assets belonging to that datasource live beneath the
  dedicated connection and are tagged only with their DXR classification labels, while
  supplementary context is recorded through the `DXR File Metadata` custom metadata set.

The service is inspired by the existing Purview integration but uses the Atlan Application
SDK’s workflow tooling plus direct REST calls (wrapped by a lightweight helper) to
manage metadata in Atlan.

## High-level flow

1. Authenticate against DXR using a personal access token (PAT).
2. Fetch all classification labels (DXR "labels") and populate the global connection with
   dataset summaries plus Atlan tag definitions that mirror DXR attributes.
3. Stream file metadata from DXR. For each datasource, ensure the Atlan connection exists
   (creating it when necessary), build the enriched file asset, keep the DXR label
   classifications attached, and populate the supporting custom metadata fields that
   describe DLP labels, annotators, entitlements, and categories.
4. Aggregate the files under each label to produce dataset summaries and upsert the
   resulting tables into the global connection's database and schema.

Each table created in Atlan uses the following conventions:

- **Qualified name**: `<global-connection-qualified-name>/<database-name>/<schema-name>/<classification-id>`
- **Connector metadata**: `connectionName` and `connectorName` come from configuration.
- **Display name**: DXR label name is preserved as the table's display name, while the
  classification identifier anchors the qualified name for idempotency.
- **Description**: DXR label description plus a generated summary with the file count and
  a short sample of files.
- **Source URL**: Backlink to the DXR search page for that label when available.

Per-datasource `File` assets inherit their connector metadata from the datasource
connection and retain only the DXR classification tags. Ancillary details such as DLP
labels, annotators, extracted metadata pairs, entitlements, and categories are stored in
the `DXR File Metadata` custom metadata set for each file.

## Getting started

1. (Optional) Copy `.env.example` to `.env` if you prefer to populate defaults locally or
   plan to run the sync via the CLI. The Application UI will prompt for any missing values,
   so you can skip storing secrets on disk when running through the frontend.
2. Create and activate a virtual environment managed by `uv`:

   ```bash
   uv venv
   source .venv/bin/activate  # Windows: .venv\Scripts\activate
   uv sync
   ```

3. Bootstrap the Dapr component definitions (skip this step on subsequent runs unless you upgrade the SDK or set `FORCE_REFRESH_COMPONENTS=1`):

   ```bash
   uv run poe download-components
   ```

4. Start the local dependencies required by the Application SDK (Temporal + Dapr):

   ```bash
   uv run poe start-deps
   ```

   Stop the background services when you finish:

   ```bash
   uv run poe stop-deps
   ```

5. Launch the DXR application worker and API:

   ```bash
   uv run main.py
   ```

   The workflow becomes available through the Atlan application runtime, including the
   frontend configuration exposed via `app/frontend/workflow.json`. Open
 <http://localhost:8000> to launch the built-in configuration UI — it mirrors the keys in
  `.env.example`, persists values in the browser, and starts the ingestion once you click
  **Run sync**. The submitted settings are also stored in the Temporal state store, so they
  rehydrate automatically the next time you open the UI. You can trigger workflows directly
  from Temporal/Dapr endpoints or via Atlan's runtime drawer as well.

6. (Optional) When orchestrating the ingestion from automation or CI, you can provision the
   custom metadata definitions explicitly:

   ```bash
   uv run python -m atlan_dxr_integration.custom_metadata provision
   ```

To execute the one-shot sync without the Application SDK (for example in CI), run:

```bash
uv run python -m atlan_dxr_integration
```

The service logs progress and exits once the current batch has been synced. Schedule the
workflow via Atlan’s runtime or any external orchestrator to refresh tables periodically.
When invoking the CLI directly, ensure the required environment variables from
`.env.example` are exported.

## Configuration

Environment variables (see `.env.example`):

| Variable | Description |
|----------|-------------|
| `DXR_BASE_URL` | Base URL for Data X-Ray's API (`/api` suffix included). |
| `DXR_PAT` | Data X-Ray personal access token. |
| `DXR_CLASSIFICATION_TYPES` | Optional comma-separated whitelist of DXR classification `type` values to ingest. Leave blank to ingest everything. |
| `DXR_SAMPLE_FILE_LIMIT` | Number of sample files to include in table summaries. |
| `DXR_FILE_FETCH_LIMIT` | Maximum files to request from DXR per classification (0 for unlimited). |
| `ATLAN_BASE_URL` | Base URL for Atlan. |
| `ATLAN_API_TOKEN` | Atlan API token. |
| `ATLAN_GLOBAL_CONNECTION_QUALIFIED_NAME` | Qualified name of the global connection that owns classification tables and tags. |
| `ATLAN_GLOBAL_CONNECTION_NAME` | Human-readable connection name for the global attributes connection (default: `dxr-unstructured-attributes`). |
| `ATLAN_GLOBAL_CONNECTOR_NAME` | Connector name string for the global connection (for example `custom`). |
| `ATLAN_GLOBAL_DOMAIN` | Optional Atlan domain to assign to the global connection. |
| `ATLAN_DATASOURCE_CONNECTION_PREFIX` | Prefix used when generating datasource connection names (default: `dxr-datasource`). |
| `ATLAN_DATASOURCE_DOMAIN_PREFIX` | Optional domain prefix to assign to datasource connections (for example `DXR`). |
| `ATLAN_DATABASE_NAME` | Name of the Atlan database that will be created (if missing) under the connection. |
| `ATLAN_SCHEMA_NAME` | Name of the Atlan schema that will be created (if missing) inside the database. |
| `ATLAN_BATCH_SIZE` | Maximum number of tables to upsert in a single API call. |
| `ATLAN_DATASET_PATH_PREFIX` | Optional suffix appended to dataset qualified names (default: `dxr`). |
| `ATLAN_TAG_NAMESPACE` | Namespace prefix used when creating Atlan tag definitions (default: `DXR`). |
| `ATLAN_CONNECTION_ADMIN_USER` | Optional Atlan user or service-account ID to assign as a connection admin when provisioning the global connection. Use the token's `service-account-apikey-…` identifier if the API token needs direct connection admin access. |
| `LOG_LEVEL` | Logging verbosity (`INFO`, `DEBUG`, ...). |

## Implementation notes

- `DXR` interactions rely on the documented `/api/vbeta/classifications` and
  `/api/vbeta/files` endpoints.
- Metadata is written to Atlan via REST calls issued by the embedded helper client.
  DXR labels continue to use Atlan classifications, while supplementary context is stored
  via the `DXR File Metadata` and `DXR Classification Metadata` custom metadata sets.
- The module is idempotent: rerunning the service overwrites tables and file assets with
  the same qualified names.
- On startup the uploader verifies the referenced global connection, database, and schema
  and will create them if they do not already exist. Datasource connections are created on
  demand as files are streamed.
- Per-datasource connections are optionally assigned to domains using
  `ATLAN_DATASOURCE_DOMAIN_PREFIX`, making it easy to delegate stewardship or personas per
  source.
- Connector metadata is normalised using Atlan's `AtlanConnectorType` enum, so values such
  as `custom-connector` in configuration are stored as the canonical enum value in Atlan.

### Sanity check

Run a lightweight verification against live DXR and Atlan environments:

```bash
python scripts/sanity_check.py --labels 1 --max-files 200
```

The command pulls a small sample of DXR classifications, pushes them to Atlan, and
confirms the resulting tables exist via `get_by_qualified_name`. It exits with a non-zero
status if no classifications are returned, no tables are generated, or Atlan does not
acknowledge the upsert.

### Cleaning up connections during testing

To hard-delete the configured Atlan connections (for example between integration test
runs), use the dedicated helper:

```bash
python -m atlan_dxr_integration.connection_manager --delete-type purge --skip-soft-delete --purge-tags
```

The helper enumerates every connection whose name matches the configured global
connection or datasource prefix, deletes any file or table assets underneath it, and then
removes the connection itself. **This command is intended for development environments
only**; avoid running it against production tenants. Pass `--skip-soft-delete` to jump
straight to the requested deletion mode (`hard`, `purge`).

Including `--purge-tags` hard-deletes every classification typedef whose display name
starts with your configured `ATLAN_TAG_NAMESPACE` (for example `DXR :: Label :: …`). If
you prefer to keep the tag definitions, simply omit that flag.

You can invoke the same behaviour from Python via
`connection_manager.purge_environment(Config.from_env())`, which defaults to a hard
delete, skips the soft delete, and purges the namespaced classifications.

### Migrating existing deployments

Earlier iterations of this integration stored DLP labels, annotators, entitlements, and
other enrichments as Atlan classifications. Run the migration helper to populate the new
custom metadata fields for legacy assets:

```bash
uv run python -m atlan_dxr_integration.metadata_migration backfill-files
```

Add `--dry-run` to inspect the changes without writing them back to Atlan. The command
can be rerun safely; it only updates assets that still reference the legacy tags.

## Development

Run unit tests from the repository root:

```bash
pytest atlan-dxr-integration/tests -q
```

Feel free to extend the table transformer to attach additional metadata, lineage, or
custom metadata once the relevant Atlan objects are defined.
