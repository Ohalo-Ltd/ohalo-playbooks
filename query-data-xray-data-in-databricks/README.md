# query-data-xray-data-in-databricks

Daily Databricks workflow that calls Data X-Ray's public API, streams `/api/v1/files` JSONL payloads, and snapshots the metadata into a Delta table so downstream governance jobs can query the state of every scanned file per day.

## How it works

1. A lightweight Python job (`query_data_xray.job:main`) authenticates with a JWE bearer token and calls `GET /api/v1/files`.
2. Responses are read as newline-delimited JSON to avoid buffering entire payloads in memory.
3. The driver converts the batch to a Spark DataFrame, tags it with an `ingestion_date`, and writes the result to an append-only Delta table partitioned by day.
4. (Optional) The job can auto-create or update a Unity‑managed table pointing at the Delta path for easy discovery in catalogs.

## Repository layout

```
query-data-xray-data-in-databricks/
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
| `DXR_DELTA_PATH` | ✅ | — | Delta Lake location such as `dbfs:/Volumes/workspace/dxr-data/dxr-data-volume/datasets/file_metadata` |
| `DXR_DELTA_TABLE` |  | — | Fully qualified Unity Catalog table name (e.g., `workspace.dxr_data.file_metadata`) to register/refresh |
| `DXR_VERIFY_SSL` |  | `true` | Set to `false` while targeting non-public DXR endpoints |
| `DXR_HTTP_TIMEOUT` |  | `120` | Per-request timeout in seconds |
| `DXR_RECORD_CAP` |  | — | If set, stops streaming after *N* JSONL rows (handy for tests) |
| `DXR_USER_AGENT` |  | `query-data-xray-data-in-databricks/<version>` | Custom user-agent header |

CLI flags (see `python -m query_data_xray.job --help`) can override the same values at runtime.
`*` Provide `DXR_BEARER_TOKEN` for local/dev runs. In Databricks, prefer `DXR_TOKEN_SCOPE` + `DXR_TOKEN_KEY` so the job fetches the secret from the workspace.

Copy `.env.example` to `.env` and fill in your tenant-specific values before running locally. The CLI automatically loads `.env` via `python-dotenv`, so placing the file at the project root is enough. On Databricks set `DXR_TOKEN_SCOPE` and `DXR_TOKEN_KEY` so the script can fetch the bearer token from `dbutils.secrets`; the files API path is fixed at `/api/v1/files`.

Install dependencies with your preferred tool; for example using `uv`:

```bash
uv pip sync requirements.txt             # runtime deps
uv pip sync requirements-dev.txt        # optional dev deps (build/pytest)
```

## Local dry run

You can run the job from your laptop using a local Spark install or Databricks Connect:

```bash
cd query-data-xray-data-in-databricks
python -m venv .venv && source .venv/bin/activate
pip install -e .
export DXR_BASE_URL="https://app.dataxray.com"
export DXR_BEARER_TOKEN="$(databricks secrets get --scope dxr --key bearer-token)"
export DXR_DELTA_PATH="/tmp/dxr_file_metadata"
python -m query_data_xray.job --ingestion-date 2024-01-01 --record-cap 100
```

The dry run writes Parquet + Delta artifacts under the provided path. Replace the token export with your own secret management process.

## Databricks job deployment (Python script workflow)

1. **Sync or upload the repo**  
   In *Repos → Add Repo*, point Databricks at your GitHub fork so that the scripts live under a path such as `/Repos/you/query-data-xray-data-in-databricks`. Serverless compute automatically adds the repo root to `PYTHONPATH`, and `scripts/run_daily_snapshot.py` adds the `src/` folder so imports resolve without installing wheels.
2. **Secrets + parameters**  
   Put the bearer token into a secret scope (e.g., `dxr/dxr-bearer-token`). The job will pass `--bearer-token {{secrets/dxr/dxr-bearer-token}}`. All other CLI flags can be specified as Databricks job parameters (see the JSON template below).
3. **Create the job**  
   - In the UI: *Workflows → Jobs → Create job → Task type = Python script*. Point `Python script path` at `/Repos/you/query-data-xray-data-in-databricks/scripts/run_daily_snapshot.py`. Add parameter pairs (for example: `DXR_BASE_URL`, `https://...`; `DXR_TOKEN_SCOPE`, `dxr`; `DXR_TOKEN_KEY`, `dxr-bearer-token`; `DXR_DELTA_PATH`, `dbfs:/Volumes/workspace/dxr-data/dxr-data-volume/datasets/file_metadata`) and select the desired serverless compute tier.  
   - Via CLI: update `databricks/jobs/daily_file_metadata.json` by replacing `/Repos/REPLACE_WITH_REPO_OWNER/...` and `REPLACE_WITH_SERVERLESS_COMPUTE_KEY`, then run `databricks jobs create --json @databricks/jobs/daily_file_metadata.json`.
4. **Run & schedule**  
   Kick off a run (`databricks jobs run-now --job-id <id>`) to validate connectivity, then enable the cron schedule (default 02:00 UTC) once the Delta path populates.

## Notes

- `/api/v1/files` can emit large payloads. If you expect millions of rows, keep the cluster as a multi-node job cluster and consider adjusting `spark.databricks.io.cache.enabled` or writing intermediate JSONL chunks to DBFS before loading into Spark.
- The Databricks job is idempotent per `ingestion_date`. Re-running for the same date overwrites only that partition thanks to Delta's `replaceWhere` option.
- Unity Catalog does not allow `CREATE TABLE ... LOCATION 'dbfs:/...'` on serverless compute. If you point `DXR_DELTA_PATH` at a UC Volume, leave `DXR_DELTA_TABLE` unset (or register the table manually via UC external locations) to avoid `UC_FILE_SCHEME_FOR_TABLE_CREATION_NOT_SUPPORTED`.
- Extend `query_data_xray/dxr_client.py` with other endpoints (e.g., `/api/v1/files/{id}`) if you need enriched metadata.
