# file-records-management-in-databricks

Daily Databricks workflow that calls Data X-Ray's public API, streams `/api/v1/files` JSONL payloads, and snapshots the metadata into a Delta table so downstream governance jobs can enforce records-management policies (duplicate detection, retention, ROT clean-up, etc.) on every scanned file per day.

## Why records management?

- **Shared source of truth**: Landing the Data X-Ray feed in Delta means every team runs the same policy queries over time-partitioned metadata.
- **Policy flexibility**: Duplicate detection is only one example—once the Delta table exists you can layer other policies (keep-forever, legal hold, stale data, etc.) without redeploying compute.
- **Actionable automation**: Spark + Python is used to export result sets (CSV, JSON) or trigger downstream workflows that archive, quarantine, or delete the flagged files.

### Example policies

| Policy | Sample logic | Outcome |
| --- | --- | --- |
| Duplicate files | `content_sha256` grouping, `count(*) > 1` | Identify redundant files and archive all but one copy |
| Retention gaps | `classification = 'Records' AND last_modified < now() - 7y` | Flag records that have aged past policy windows |
| ROT cleanup | `labels CONTAINS 'ROT'` | Move redundant/obsolete/trivial files to deep storage |

The duplicate-remediation PowerShell script bundled in `scripts/` is a concrete example of how to act on one of these policies, but you can plug in any downstream process that consumes the Delta outputs.

## How it works

1. A lightweight Python job (`query_data_xray.job:main`) authenticates with a JWE bearer token and calls `GET /api/v1/files`.
2. Responses are read as newline-delimited JSON to avoid buffering entire payloads in memory.
3. The driver converts the batch to a Spark DataFrame, tags it with an `ingestion_date`, and writes the result to an append-only Delta table partitioned by day.
4. (Optional) The job can auto-create or update a Unity‑managed table pointing at the Delta path for easy discovery in catalogs.

## Repository layout

```
file-records-management-in-databricks/
├── databricks/
│   └── jobs/
│       └── daily_file_metadata.json   # Sample Databricks Workflow definition
├── pyproject.toml                     # Build + dependency metadata
├── README.md                          # You are here
├── scripts/
│   └── run_daily_snapshot.py          # Entry script used by Databricks Jobs
└── src/query_data_xray/
    ├── __init__.py
    ├── config.py                      # Env/CLI driven configuration loader
    ├── dxr_client.py                  # Thin HTTP client around Data X-Ray
    └── job.py                         # Spark entrypoint that writes to Delta
```

## Configuration

| Variable | Required | Default | Description |
| --- | --- | --- | --- |
| `DXR_BASE_URL` | ✅ | — | Base URL for the Data X-Ray tenant, e.g. `https://app.dataxray.com` |
| `DXR_BEARER_TOKEN` | ✅\* | — | JWE bearer token for local runs. Skip this when `DXR_TOKEN_SCOPE` + `DXR_TOKEN_KEY` are provided. |
| `DXR_TOKEN_SCOPE` | ✅ (Databricks) | — | Databricks secret scope that stores the DXR bearer token |
| `DXR_TOKEN_KEY` | ✅ (Databricks) | — | Databricks secret key within the scope that stores the DXR bearer token |
| `DXR_QUERY` |  | — | Optional KQL-like filter that `/api/v1/files` accepts |
| `DXR_DELTA_PATH` | ✅ | — | Delta Lake location such as `dbfs:/Volumes/dxr/dxr-data/dxr-data-volume/datasets/file_metadata` |
| `DXR_DELTA_TABLE` |  | — | Fully qualified Unity Catalog table name (e.g., `dxr.dxr_data.file_metadata`) to register/refresh |
| `DXR_VERIFY_SSL` |  | `true` | Set to `false` while targeting non-public DXR endpoints |
| `DXR_HTTP_TIMEOUT` |  | `120` | Per-request timeout in seconds |
| `DXR_RECORD_CAP` |  | — | If set, stops streaming after *N* JSONL rows (handy for tests) |
| `DXR_USER_AGENT` |  | `query-data-xray-data-in-databricks/<version>` | Custom user-agent header |

CLI flags (see `python -m query_data_xray.job --help`) can override the same values at runtime.  
Provision Unity Catalog assets (catalog, schema, volume/external location) yourself before running the job; it only writes to the provided Delta path and registers the existing table. If a catalog or schema contains characters like hyphens, wrap that component in backticks when setting `DXR_DELTA_TABLE` (for example `dxr.\`dxr-data\`.files`). When pointing at a UC Volume, pass the `/Volumes/...` path directly—the job preserves that prefix so serverless clusters do not reject the `CREATE TABLE ... LOCATION` statement with a `dbfs:/` scheme.
`*` Provide `DXR_BEARER_TOKEN` for local/dev runs. In Databricks, prefer `DXR_TOKEN_SCOPE` + `DXR_TOKEN_KEY` so the job fetches the secret from the workspace.

Copy `.env.example` to `.env` and fill in your tenant-specific values before running locally. The CLI automatically loads `.env` via `python-dotenv`, so placing the file at the project root is enough. On Databricks set `DXR_TOKEN_SCOPE` and `DXR_TOKEN_KEY` so the script can fetch the bearer token from `dbutils.secrets`; the files API path is fixed at `/api/v1/files`.

Install dependencies with your preferred tool; for example using `uv`:

```bash
uv pip sync requirements.txt            # runtime deps
uv pip sync requirements-dev.txt        # optional dev deps (build/pytest)
```

## Local dry run

You can run the job from your laptop using a local Spark install or Databricks Connect:

```bash
cd file-records-management-in-databricks
python -m venv .venv && source .venv/bin/activate
pip install -e .
export DXR_BASE_URL="https://app.dataxray.com"
export DXR_BEARER_TOKEN="$(databricks secrets get --scope dxr --key bearer-token)"
export DXR_DELTA_PATH="/tmp/dxr_file_metadata"
python -m query_data_xray.job --ingestion-date 2024-01-01 --record-cap 100
```

The dry run writes Parquet + Delta artifacts under the provided path. Replace the token export with your own secret management process.

### Example: handling duplicates off-cluster

After exporting a CSV from a Databricks dashboard (for example, the duplicate detection query that returns `path` and `content_sha256`), download the file and run `scripts/remediate_duplicates.ps1` on a Windows host that has access to the original shares:

```powershell
cd file-records-management-in-databricks
.\scripts\remediate_duplicates.ps1 `
    -CsvPath C:\temp\dxr_duplicates.csv `
    -ArchiveRoot \\fileserver\retention\dxr
```

Each row moves the original file into `$ArchiveRoot/<hash>/filename` and leaves a stub text file in its place. Swap in your own script for other policies (for example, a legal-hold tracker) by consuming the Delta exports.

## Databricks job deployment (Python script workflow)

Follow these steps end-to-end to run the playbook inside Databricks:

1. **Sync the repo**  
   - Navigate to *Repos → Add Repo* and point Databricks at your fork so the code lives at `/Repos/<you>/file-records-management-in-databricks`.  
   - Serverless compute automatically adds the repo root to `PYTHONPATH`, and `scripts/run_daily_snapshot.py` adjusts `sys.path` so `src/` imports resolve—no wheel build required.
2. **Create the secrets**  
   ```bash
   databricks secrets create-scope --scope dxr
   databricks secrets put --scope dxr --key bearer-token   # paste the DXR JWE token
   ```
   - Set two job parameters so the script can read the secret: `DXR_TOKEN_SCOPE = dxr`, `DXR_TOKEN_KEY = bearer-token`.
3. **Define the job parameters**  
   - Required: `DXR_BASE_URL`, `DXR_DELTA_PATH`, `DXR_TOKEN_SCOPE`, `DXR_TOKEN_KEY`, plus either `DXR_DELTA_TABLE` (if you want the Unity Catalog table registered automatically) or handle catalog registration yourself.  
   - Optional overrides: `DXR_QUERY`, `DXR_HTTP_TIMEOUT`, `DXR_RECORD_CAP`, `DXR_INGESTION_DATE`, etc. Every CLI flag accepted by `python -m query_data_xray.job --help` can be expressed as a key/value pair in the Databricks job UI or JSON payload.
4. **Create the job**
   - **UI**: *Workflows → Jobs → Create job* → choose **Python script**.  
     - `Python script path`: `/Repos/<you>/file-records-management-in-databricks/scripts/run_daily_snapshot.py`  
     - Add the parameter table from step 3.  
     - Select a serverless or job compute with access to Unity Catalog/Volumes that back your Delta path.  
   - **CLI**: Update `databricks/jobs/daily_file_metadata.json` with your repo path (`/Repos/<you>/file-records-management-in-databricks/...`) and compute key (`REPLACE_WITH_SERVERLESS_COMPUTE_KEY`), then run:
     ```bash
     databricks jobs create --json @databricks/jobs/daily_file_metadata.json
     ```
5. **Run, validate, schedule**  
   - Trigger a manual run to ensure the job can fetch secrets and write to the Delta location:  
     ```bash
     databricks jobs run-now --job-id <id>
     ```  
   - Inspect the Delta table/volume for the new `ingestion_date` partition.  
   - Enable the default 02:00 UTC schedule (or your own cron) once the validation run succeeds.

## Notes

- `/api/v1/files` can emit large payloads. If you expect millions of rows, keep the cluster as a multi-node job cluster and consider adjusting `spark.databricks.io.cache.enabled` or writing intermediate JSONL chunks to DBFS before loading into Spark.
- The Databricks job is idempotent per `ingestion_date`. Re-running for the same date overwrites only that partition thanks to Delta's `replaceWhere` option.
- Unity Catalog does not allow `CREATE TABLE ... LOCATION 'dbfs:/...'` on serverless compute. If you point `DXR_DELTA_PATH` at a UC Volume, leave `DXR_DELTA_TABLE` unset (or register the table manually via UC external locations) to avoid `UC_FILE_SCHEME_FOR_TABLE_CREATION_NOT_SUPPORTED`.
- Extend `query_data_xray/dxr_client.py` with other endpoints (e.g., `/api/v1/files/{id}`) if you need enriched metadata.
