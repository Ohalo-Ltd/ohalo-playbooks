# Atlan ↔ Data X-Ray integration

This playbook provides a containerised service that ingests Data X-Ray (DXR) labels and
publishes them to Atlan as relational-style `Table` assets grouped beneath a dedicated
database and schema. The service is inspired by the existing
Purview integration but uses Atlan's Python SDK (`pyatlan`) to manage metadata directly
through Atlan's APIs.

## High-level flow

1. Authenticate against DXR using a personal access token (PAT).
2. Fetch all classification labels (DXR "labels") and stream file metadata.
3. Aggregate the files under each label to produce table summaries.
4. Upsert the resulting tables into Atlan using the configured connection (a database and
   schema are auto-created when missing).

Each table created in Atlan uses the following conventions:

- **Qualified name**: `<connection-qualified-name>/<database-name>/<schema-name>/<classification-id>`
- **Connector metadata**: `connectionName` and `connectorName` come from configuration.
- **Display name**: DXR label name is preserved as the table's display name, while the
  classification identifier anchors the qualified name for idempotency.
- **Description**: DXR label description plus a generated summary with the file count and
  a short sample of files.
- **Source URL**: Backlink to the DXR search page for that label when available.

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
| `ATLAN_CONNECTION_QUALIFIED_NAME` | Qualified name of the Atlan connection that should contain the DXR assets. |
| `ATLAN_CONNECTION_NAME` | Human-readable connection name for the DXR assets. |
| `ATLAN_CONNECTOR_NAME` | Connector name string to persist on assets (for example `custom`). |
| `ATLAN_DATABASE_NAME` | Name of the Atlan database that will be created (if missing) under the connection. |
| `ATLAN_SCHEMA_NAME` | Name of the Atlan schema that will be created (if missing) inside the database. |
| `ATLAN_BATCH_SIZE` | Maximum number of tables to upsert in a single API call. |
| `LOG_LEVEL` | Logging verbosity (`INFO`, `DEBUG`, ...). |

Legacy variable `ATLAN_DATASET_PATH_PREFIX` is still recognised but ignored by the current
implementation; it will be removed once existing deployments migrate to the new
connection/database/schema structure.

## Implementation notes

- `DXR` interactions rely on the documented `/api/vbeta/classifications` and
  `/api/vbeta/files` endpoints.
- Metadata is written to Atlan via `pyatlan`'s synchronous client (`AtlanClient.asset.save`).
- The module is idempotent: rerunning the service overwrites tables with the same
  qualified name.
- On startup the uploader verifies the referenced connection, database, and schema and
  will create them if they do not already exist. The API token therefore needs rights to
  read those assets, resolve the `$admin` role, and create new connection/database/schema
  and table assets. If you prefer to manage the hierarchy manually, create it up-front and
  assign the service principal the
  appropriate persona/purpose.
- Connector metadata is normalised using Atlan's `AtlanConnectorType` enum, so values such
  as `custom-connector` in configuration will be stored as the canonical `custom` in
  Atlan.

### Sanity check

Run a lightweight verification against live DXR and Atlan environments:

```bash
python -m atlan_dxr_integration.sanity_check --labels 1 --max-files 200
```

The command pulls a small sample of DXR classifications, pushes them to Atlan, and
confirms the resulting tables exist via `get_by_qualified_name`. It exits with a non-zero
status if no classifications are returned, no tables are generated, or Atlan does not
acknowledge the upsert.

## Development

Run unit tests from the repository root:

```bash
pytest atlan-dxr-integration/tests -q
```

Feel free to extend the table transformer to attach additional metadata, lineage, or
custom metadata once the relevant Atlan objects are defined.
