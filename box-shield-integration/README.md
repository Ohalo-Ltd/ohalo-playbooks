# DXR → Box Shield Label Connector (Prototype)

A simple Python tool that imitates the Data X-Ray `dlp-syncer` behaviour, but in the
opposite direction: it takes the labels that DXR has assigned to documents and pushes
the corresponding Box Shield classifications back into Box.

The connector is intentionally lightweight and can run in an offline `--dry-run`
mode using the fixtures under `sample_data/`. It writes human- and machine-readable
outputs into `dxr-box-shield/output` so the sync can be audited or piped into other
DXR components.

## Features

- Enumerates files from the DXR API, fetches tag metadata for each file, and maps those labels to Box Shield classification keys (or uses the built-in dry-run fixtures).
- Uses Box JWT auth to mint short-lived access tokens and apply classifications;
  falls back to dry-run mode to avoid network calls during development.
- Batches items, retries transient failures, and records a detailed operations log.
- Writes a plain-text log (`sync_run.log`) with per-file actions and summary stats.

## Installation

```bash
cd dxr-box-shield
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Usage

1. Copy and update the configuration:
   ```bash
   cp config.example.yml config.yml
   # edit config.yml with your Box JWT config.json path and DXR API details
   ```
   Ensure `box.jwt_config_file` points to the JWT configuration JSON generated
   from the Box developer console (see Box docs linked below).

2. Run a dry-run against the bundled sample data (no API calls):
   ```bash
   python dxr_box_shield_connector.py --config config.example.yml --dry-run
   ```

3. To hit the live DXR and Box APIs, supply a config with valid Box JWT credentials
   and `dxr.api_token`, then run without the `--dry-run` flag:
   ```bash
   python dxr_box_shield_connector.py --config config.yml
   ```

4. Inspect `dxr-box-shield/output/sync_run.log` for the run details and summary.

## Configuration

| Field | Description |
| --- | --- |
| `box.base_url` | Base API URL (defaults to `https://api.box.com/2.0`) |
| `box.classification_template_key` | The Shield classification template key |
| `box.oauth_url` | OAuth token endpoint (defaults to Box cloud URL) |
| `box.jwt_config_file` | Path to the Box JWT JSON config (e.g., `config.json`) |
| `dxr.base_url` | Base URL of the Data X-Ray API |
| `dxr.api_token` | DXR API token with permission to read label metadata |
| `dxr.datasource_id` | Datasource identifier used in DXR endpoints |
| `dxr.files_endpoint` | Relative path that returns files for a datasource |
| `dxr.labels_endpoint` | Relative path that returns labels for a given file |
| `dxr.page_size` | Page size when pulling files from DXR |
| `labels.mapping` | Map from DXR label keys to Box classification keys |
| `dry_run_fixture.files` | Offline JSON fixture of datasource files |
| `dry_run_fixture.labels` | Offline JSON fixture mapping file IDs to labels |
| `sync.output_dir` | Output destination (defaults to `./output`) |
| `sync.batch_size` | Number of items to push per batch |
| `sync.retry_attempts` | Number of retries for failed Box calls |
| `logging.level` | Log verbosity (`INFO`, `DEBUG`, ...) |

## Relationship to `dlp-syncer`

| DLP Syncer | DXR → Box Shield Connector |
| --- | --- |
| Pulls 3rd-party classifications into DXR | Pushes DXR classifications into Box |
| Emits RabbitMQ request/response events | Emits JSON logs for downstream DXR consumers |
| Assigns sensitivity via Microsoft Graph | Assigns classifications via Box metadata API |

The connector keeps the high-level workflow: load configuration, interpret DXR
labels, call the external DLP provider, and publish a report of what happened.
