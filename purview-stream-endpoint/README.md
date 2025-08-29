# Purview Stream Endpoint

Synchronizes Data X-Ray (DXR) classifications (labels) and datasources into Microsoft Purview Data Map. It creates/updates custom entities, organizes them into Collections, and builds lineage so you can trace which DXR datasource produced each label asset.

## What It Does
- Creates/updates `unstructured_dataset` entities for DXR labels (with labelId/type/subtype/timestamps and a `dxrSearchLink`).
- Creates/updates `unstructured_datasource` entities for DXR datasources.
- Creates lineage using a `dxr_process` (subtype of Process) with clickable `dxrSearchLink` and fields: labelType, labelSubtype, createdAt, updatedAt, datasourceId/Name, connectorTypeName/Id/Name.
- Ensures Purview typedefs and the `unstructured_datasource_has_unstructured_dataset` relationship.
- Organizes assets under a Domain: single “Unstructured Datasets” collection for labels plus optional child collections per datasource.
- Optional delete sync of stale label entities when `SYNC_DELETE=1`.

## Prerequisites
- Python 3.10+ (tested with 3.12).
- A DXR instance and a Personal Access Token (PAT).
- An Entra ID (Azure AD) App Registration and Client Secret.
- Purview roles for that app:
  - Data Curator on the target collection(s) (required for upserts/lineage).
  - Collection Admin on the domain/collections (only if you want this tool to create collections). Otherwise pre-create them and use `PURVIEW_COLLECTION_ID`.

### Create the App Registration (Azure Portal)
1) Azure Portal → Microsoft Entra ID → App registrations → New registration
   - Name: “DXR Purview Sync” (any)
   - Supported account types: Single tenant
   - Redirect URI: leave empty
   - Click Register
2) On the app page, record:
   - Application (client) ID → `AZURE_CLIENT_ID`
   - Directory (tenant) ID → `AZURE_TENANT_ID`
3) Certificates & secrets → New client secret
   - Record the secret value → `AZURE_CLIENT_SECRET`

### Assign Purview Roles to the App
In Purview Studio (or Azure Portal Purview blade):
- Collections → Access control → Add role assignment
  - Select the parent domain or target collection(s)
  - Add the app principal as Data Curator
- If you want the tool to create collections:
  - Add the app principal as Collection Admin at the domain level
- If using Domains feature: ensure the domain exists and the app can see it (Domain Administrator or higher if creation is needed).

## Install
```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\\Scripts\\activate
pip install --upgrade pip
pip install azure-identity azure-purview-datamap requests python-dotenv
```

Container image (optional):
```bash
cd purview-stream-endpoint
docker build -t purview-dxr-sync:latest .
# One-time run using your .env
docker run --rm --env-file ./.env purview-dxr-sync:latest
# Continuous run (unset RUN_ONCE); add -d to run detached
docker run --rm -e RUN_ONCE=0 --env-file ./.env purview-dxr-sync:latest
```

Docker Compose (recommended for env-file driven runs):
```bash
cd purview-stream-endpoint
# One-time cycle (RUN_ONCE=1 in image). Override in .env if you want continuous.
docker compose up --build
# Continuous run: set RUN_ONCE=0 in .env and re-up
docker compose up -d --build
# Stop
docker compose down
```

## Configuration
Place a `.env` file in `purview-stream-endpoint/` (this folder). The tool auto-loads it. A template is provided at `.env.example`.
Note: If a value contains spaces, wrap it in quotes, e.g., `PURVIEW_DOMAIN_NAME="Data X-Ray Integration"`.

Required:
- `DXR_APP_URL`: Base URL, e.g. `https://<account>.dataxray.io`
- `DXR_PAT_TOKEN`: DXR Personal Access Token (Bearer)
- `PURVIEW_ENDPOINT`: Purview account endpoint, e.g. `https://<account>.purview.azure.com`
- `AZURE_TENANT_ID`: Entra tenant ID
- `AZURE_CLIENT_ID`: App registration (client) ID
- `AZURE_CLIENT_SECRET`: Client secret
- `PURVIEW_DOMAIN_NAME`: Purview domain to target (must exist or be creatable/visible)

 Optional:
 - `PURVIEW_COLLECTION_ID`: Collection referenceName for direct upserts (use with `DISABLE_GOVERNANCE=1`)
 - `PURVIEW_DOMAIN_ID`: Domain referenceName (6-char) as parent for child collections
- `PURVIEW_PARENT_COLLECTION_ID`: Parent collection referenceName; if set, takes precedence over `PURVIEW_DOMAIN_ID`
- `UNSTRUCTURED_DATASETS_COLLECTION_NAME`: Friendly name for the single collection holding all label assets (default "Unstructured Datasets"). The referenceName is derived automatically per parent collection and no longer configurable.
- `DXR_CLASSIFICATIONS_PATH`: Defaults to `/api/vbeta/classifications`
- `DXR_FILES_PATH`: Defaults to `/api/vbeta/files`
- `DXR_TENANT`: Tenant label used in qualified names (default `default`)
- `POLL_SECONDS`: Poll interval (default `60`)
- `RUN_ONCE`: Set to `1` to run one cycle and exit
- `SYNC_DELETE`: `1/true` to delete Purview label entities not present in current DXR set
- `PURVIEW_GOV_RESOURCE`: OAuth resource override for governance APIs (advanced)
- `PURVIEW_GOV_BASE`: Base URL override for governance APIs (advanced)
- `DISABLE_GOVERNANCE`: `1/true` to skip domain/collection API calls and moves; entities are written to `PURVIEW_COLLECTION_ID` if set

Example `.env` (do not commit real secrets):
```dotenv
DXR_APP_URL=https://demo.dataxray.io
DXR_PAT_TOKEN=eyJ...your-token...
PURVIEW_ENDPOINT=https://contoso.purview.azure.com
AZURE_TENANT_ID=00000000-0000-0000-0000-000000000000
AZURE_CLIENT_ID=11111111-1111-1111-1111-111111111111
AZURE_CLIENT_SECRET=your-secret
PURVIEW_DOMAIN_NAME=Data X-Ray Integration
POLL_SECONDS=60
RUN_ONCE=1
```

Note: If you previously had a `.env` at the repo root, move it into `purview-stream-endpoint/.env` so settings only apply to this project.

## Run (Quick Start)
- Smoke run (quick sanity check with small limits):
```bash
cd purview-stream-endpoint
./run_once.sh           # uses .env here; add --full to disable limits
```
- Continuous polling service:
```bash
cd purview-stream-endpoint
python purview_dxr_integration.py
```

Run in Docker:
```bash
cd purview-stream-endpoint
docker build -t purview-dxr-sync:latest .
docker run --rm --env-file ./.env purview-dxr-sync:latest
# Continuous: drop RUN_ONCE or set RUN_ONCE=0
docker run --rm -e RUN_ONCE=0 --env-file ./.env purview-dxr-sync:latest
```

What you should see in Purview
- Entities of type `unstructured_dataset` (labels) with attributes including `dxrSearchLink`.
- Entities of type `unstructured_datasource` (datasources).
- Lineage: a process node (`dxr_process`) between datasource → label with clickable `dxrSearchLink`, label metadata, and datasource connector details.

## Developer Modules
For easier comprehension and extension, core functionality has been extracted into small modules under `pvlib/`:
- `pvlib/config.py`: Env loading, logging, HTTP session helpers
- `pvlib/dxr.py`: DXR API calls (classifications, files streaming for datasource hits)
- `pvlib/atlas.py`: Purview data-plane auth, REST helpers, entity lookups and utilities
- `pvlib/typedefs.py`: Ensures custom entity/relationship types and merges new attributes
- `pvlib/governance.py`: Governance headers/base and Collections API helpers (shared-service and account-host)
- `pvlib/collections.py`: Collection helpers (slug/ref builders, ensure collections/UD collection)
- `pvlib/entities.py`: Upsert helpers for datasets and datasources
- `pvlib/relationships.py`: Create/update/delete relationships, mapping helpers
  (stats module removed; relationships are created from files hit discovery)

The top-level `purview_dxr_integration.py` will gradually be refactored to use these modules directly.

## Pre‑flight Checks
Use the helper to validate Purview connectivity and basic permissions:
```bash
cd purview-stream-endpoint
./sanity-check.sh
```
This acquires tokens, attempts to list collections, checks typedef visibility, and probes domain access.

## Notes on Collections and Domain
- The code checks your domain by slug (`/catalog/api/domains/{slug}`). If it’s not found, create it in Purview Studio and grant your service principal Domain Administrator (or higher) and Data Curator on the target hierarchy.
- When permitted, the tool creates:
  - A parent collection from the domain slug.
  - Child collections per DXR datasource and moves entities accordingly.

 Reduced-permissions mode:
 - Set `DISABLE_GOVERNANCE=1` and `PURVIEW_COLLECTION_ID=<collectionRefName>` to avoid governance calls entirely (governance calls require a higher level of permission in Azure). Upserts go straight into the specified collection; move/collection creation is skipped.
- Note: `PURVIEW_DOMAIN_ID` is not valid for direct upserts; the data-plane upsert expects a collection referenceName.

Collection naming rules
- Account-host collections require `collectionName` length 3–36 chars. The tool constructs datasource child collection refs as `<slug(name)>-<id>` and truncates the slug to fit. For very long IDs, it uses an 8-char hash suffix.

- Desired structure example:
- Provide `PURVIEW_DOMAIN_ID` as your domain’s referenceName (e.g., `hjdqay`).
- The tool will:
  - Create child collections per DXR datasource under that parent.
  - Ensure a single `UNSTRUCTURED_DATASETS_COLLECTION_NAME` under that parent and write all tag assets there.

## Testing (optional)
- Unit tests (offline):
```bash
pytest -m unit purview-stream-endpoint/purview_dxr_integration_test.py -q
```
- Integration tests (hit live services; require env vars and permissions):
```bash
pytest -m "integration" purview-stream-endpoint/purview_dxr_integration_test.py -q
```
Some tests are marked `destructive` and may create or modify cloud resources.

## Troubleshooting
- 401/403 from Purview Atlas endpoints: ensure Data Curator on the target collection(s).
- 401/403 from Collections endpoints: ensure Collection Admin (or create collections manually).
- `PURVIEW_ENDPOINT` must be the account host (e.g. `https://<account>.purview.azure.com`), not `purview.azure.net`.
- SDK upsert responses can be sparse; the code resolves entity GUIDs by qualifiedName when needed.
- 403 while creating lineage Process: ensure we pass a `collection_id` and that the app has Data Curator on that collection (fixed in the code), or provide `PURVIEW_COLLECTION_ID`.

## Run as a Service (systemd)

Example native service (virtualenv at /opt/purview-dxr/.venv, repo at /opt/purview-dxr):
```
[Unit]
Description=Purview DXR Sync
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=/opt/purview-dxr/purview-stream-endpoint
EnvironmentFile=/opt/purview-dxr/purview-stream-endpoint/.env
ExecStart=/opt/purview-dxr/.venv/bin/python purview_dxr_integration.py
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

Example Dockerized service:
```
[Unit]
Description=Purview DXR Sync (Docker)
After=network-online.target docker.service
Requires=docker.service

[Service]
Type=simple
WorkingDirectory=/opt/purview-dxr/purview-stream-endpoint
EnvironmentFile=/opt/purview-dxr/purview-stream-endpoint/.env
ExecStartPre=/usr/bin/docker build -t purview-dxr-sync:latest /opt/purview-dxr/purview-stream-endpoint
ExecStart=/usr/bin/docker run --rm --env-file /opt/purview-dxr/purview-stream-endpoint/.env purview-dxr-sync:latest
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

Enable and start:
```bash
sudo systemctl daemon-reload
sudo systemctl enable purview-dxr.service
sudo systemctl start purview-dxr.service
```

## Security
Treat the `.env` as sensitive. Do not commit secrets. Prefer using a secret manager in production.
