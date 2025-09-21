# Atlan ↔ Data X-Ray integration

This playbook provides a containerised service that ingests Data X-Ray (DXR) labels and
publishes them to Atlan as `DataSet` assets. The service is inspired by the existing
Purview integration but uses Atlan's Python SDK (`pyatlan`) to manage metadata directly
through Atlan's APIs.

## High-level flow

1. Authenticate against DXR using a personal access token (PAT).
2. Fetch all classification labels (DXR "labels") and stream file metadata.
3. Aggregate the files under each label to produce dataset summaries.
4. Upsert the resulting datasets into Atlan using the configured connection.

Each dataset created in Atlan uses the following conventions:

- **Qualified name**: `<connection-qualified-name>/<dataset-path-prefix>/<classification-id>`
- **Connector metadata**: `connectionName` and `connectorName` come from configuration.
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
   workflow orchestrator) to refresh datasets periodically.

## Configuration

Environment variables (see `.env.example`):

| Variable | Description |
|----------|-------------|
| `DXR_BASE_URL` | Base URL for Data X-Ray's API (`/api` suffix included). |
| `DXR_PAT` | Data X-Ray personal access token. |
| `DXR_CLASSIFICATION_TYPES` | Optional comma-separated whitelist of DXR classification `type` values to ingest. Leave blank to ingest everything. |
| `DXR_SAMPLE_FILE_LIMIT` | Number of sample files to include in dataset summaries. |
| `ATLAN_BASE_URL` | Base URL for Atlan. |
| `ATLAN_API_TOKEN` | Atlan API token. |
| `ATLAN_CONNECTION_QUALIFIED_NAME` | Qualified name of the Atlan connection that owns the datasets. |
| `ATLAN_CONNECTION_NAME` | Human-readable connection name for the datasets. |
| `ATLAN_CONNECTOR_NAME` | Connector name string to persist on datasets (for example `custom`). |
| `ATLAN_DATASET_PATH_PREFIX` | Path suffix appended to the connection qualified name when generating dataset qualified names. |
| `ATLAN_BATCH_SIZE` | Maximum number of datasets to upsert in a single API call. |
| `LOG_LEVEL` | Logging verbosity (`INFO`, `DEBUG`, ...). |

## Implementation notes

- `DXR` interactions rely on the documented `/api/vbeta/classifications` and
  `/api/vbeta/files` endpoints.
- Metadata is written to Atlan via `pyatlan`'s synchronous client (`AtlanClient.asset.save`).
- The module is idempotent: rerunning the service overwrites datasets with the same
  qualified name.
- Connection management is out of scope — ensure the configured connection already exists
  and grants the API token permission to create assets.

## Development

Run unit tests from the repository root:

```bash
pytest atlan-dxr-integration/tests -q
```

Feel free to extend the dataset transformer to attach additional metadata, lineage, or
custom metadata once the relevant Atlan objects are defined.
