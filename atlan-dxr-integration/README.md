# Atlan ↔ Data X-Ray integration

This playbook ingests Data X-Ray (DXR) metadata into Atlan using a layered connection
model:

- A **global `dxr-unstructured-attributes` connection** stores dataset-style summaries for
  each DXR classification and owns the Atlan tag definitions that describe DXR attributes
  (classifications, annotators, DLP labels, entitlements, etc.).
- A **per-datasource connection** is created for every datasource encountered while
  streaming file metadata. File assets belonging to that datasource live beneath the
  dedicated connection and are decorated with the global Atlan tags.

The service is inspired by the existing Purview integration but uses Atlan's Python SDK
(`pyatlan`) to manage metadata directly through Atlan's APIs.

## High-level flow

1. Authenticate against DXR using a personal access token (PAT).
2. Fetch all classification labels (DXR "labels") and populate the global connection with
   dataset summaries plus Atlan tag definitions that mirror DXR attributes.
3. Stream file metadata from DXR. For each datasource, ensure the Atlan connection exists
   (creating it when necessary), build the enriched file asset, and tag it using the
   canonical definitions from the global connection.
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
connection and are tagged with the canonical Atlan tags created under the global
connection. This includes the DXR classification(s) assigned to the file along with
annotator metadata, DLP labels, extracted metadata pairs, entitlements, and categories.

## Getting started

1. Copy `.env.example` to `.env` and populate it with valid credentials.
2. Build and run the container:

   ```bash
   cd atlan-dxr-integration
   docker build -t atlan-dxr-sync .
   docker run --rm --env-file .env atlan-dxr-sync
   ```

   Alternatively, run the module directly with Python:

   ```bash
   pip install -r requirements.txt
   python -m atlan_dxr_integration
   ```

3. The service logs progress and exits once the current batch has been synced.
   Schedule the container (for example through Atlan's embedded runtime, cron, or a
   workflow orchestrator) to refresh tables periodically.

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
| `ATLAN_TAG_NAMESPACE` | Namespace prefix used when creating Atlan tag definitions (default: `DXR`). |
| `LOG_LEVEL` | Logging verbosity (`INFO`, `DEBUG`, ...). |

Legacy variables (`ATLAN_CONNECTION_QUALIFIED_NAME`, `ATLAN_CONNECTION_NAME`,
`ATLAN_CONNECTOR_NAME`) are still recognised for backwards compatibility and map to the
global connection settings above.

## Implementation notes

- `DXR` interactions rely on the documented `/api/vbeta/classifications` and
  `/api/vbeta/files` endpoints.
- Metadata is written to Atlan via `pyatlan`'s synchronous client
  (`AtlanClient.asset.save`). Classification tags are auto-created when missing using the
  namespace configured via `ATLAN_TAG_NAMESPACE`.
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
python -m atlan_dxr_integration.sanity_check --labels 1 --max-files 200
```

The command pulls a small sample of DXR classifications, pushes them to Atlan, and
confirms the resulting tables exist via `get_by_qualified_name`. It exits with a non-zero
status if no classifications are returned, no tables are generated, or Atlan does not
acknowledge the upsert.

### Cleaning up connections during testing

To hard-deleteAfter purging connections you can remove any leftover tables or file assets by running `python scripts/purge_orphan_assets.py` (same env vars apply).

 the configured Atlan connection (for example between integration test
runs), use the dedicated helper:

```bash
python -m atlan_dxr_integration.connection_manager --delete-type purge
```

The helper enumerates every connection whose name matches the configured global
connection or datasource prefix and deletes it (soft delete followed by the requested hard
delete). **This command is intended for development environments only**; avoid running it
against production tenants. Pass `--skip-soft-delete` to jump straight to the requested
deletion mode. Available delete types map to `pyatlan`'s `AtlanDeleteType` values
(`hard`, `purge`).

## Development

Run unit tests from the repository root:

```bash
pytest atlan-dxr-integration/tests -q
```

Feel free to extend the table transformer to attach additional metadata, lineage, or
custom metadata once the relevant Atlan objects are defined.
