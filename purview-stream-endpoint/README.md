# Purview Stream Endpoint

Synchronizes Data X-Ray (DXR) tags and datasources into Microsoft Purview Data Map. It creates/updates custom Purview entities, organizes them into Collections under a Domain, and links datasources to the tags that reference them. Can run once (for CI) or as a polling service.

## What It Does
- Maps DXR tags to custom Purview entities (`unstructured_dataset`).
- Maps DXR datasources to custom Purview entities (`unstructured_datasource`).
- Ensures custom Atlas typedefs and a relationship exist.
- Ensures a Domain exists and creates Collections: one parent (domain) and children per datasource.
- Moves upserted entities into the correct Collections and creates datasource→tag relationships.
- Optional pruning of stale tag entities when `SYNC_DELETE` is enabled.

## Prerequisites
- Python 3.10+ (tested with 3.12).
- Azure AD app registration (client credentials) with:
  - Purview Data Curator on the target collections (for entity/type/relationship ops).
  - Collection Admin on the target collection hierarchy (to create/move collections), or pre-create the domain and collections manually.
- A DXR instance and a Personal Access Token (PAT) that can call its APIs.

## Install
```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\\Scripts\\activate
pip install --upgrade pip
pip install azure-identity azure-purview-datamap requests python-dotenv
```

## Configuration
Set environment variables (a local `.env` is supported via python-dotenv):

Required:
- `DXR_APP_URL`: Base URL, e.g. `https://demo.dataxray.io`
- `DXR_PAT_TOKEN`: DXR Personal Access Token (Bearer)
- `PURVIEW_ENDPOINT`: Purview account endpoint, e.g. `https://<account>.purview.azure.com`
- `AZURE_TENANT_ID`: Entra tenant ID
- `AZURE_CLIENT_ID`: App registration (client) ID
- `AZURE_CLIENT_SECRET`: Client secret
- `PURVIEW_DOMAIN_NAME`: Purview domain to target (must exist or be creatable/visible)

 Optional:
 - `PURVIEW_COLLECTION_ID`: Collection referenceName for direct upserts (used mainly when `DISABLE_GOVERNANCE=1`)
 - `PURVIEW_DOMAIN_ID`: Domain referenceName (6-char) used as the parent for child collections
- `PURVIEW_PARENT_COLLECTION_ID`: Parent collection referenceName; if set, takes precedence over `PURVIEW_DOMAIN_ID`
- `UNSTRUCTURED_DATASETS_COLLECTION_NAME`: Friendly name for the single collection holding all tag assets (default "Unstructured Datasets"). The referenceName is derived automatically per parent collection and no longer configurable.
- `DXR_TAGS_PATH`: Defaults to `/api/tags`
- `DXR_SEARCHABLE_DATASOURCES_PATH`: Defaults to `/api/datasources/searchable`
- `DXR_TENANT`: Tenant label used in qualified names (default `default`)
- `POLL_SECONDS`: Poll interval (default `60`)
- `RUN_ONCE`: Set to `1` to run one cycle and exit
- `SYNC_DELETE`: `1/true` to delete Purview tag entities not present in current DXR set
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

## Run
- Single cycle (good for CI or manual runs):
```bash
cd purview-stream-endpoint
./run_once.sh           # uses .env; add --fast to limit scope/timeouts
```
- Continuous polling service:
```bash
cd purview-stream-endpoint
python purview_dxr_integration.py
```

## Developer Modules
For easier comprehension and extension, core functionality has been extracted into small modules under `pvlib/`:
- `pvlib/config.py`: Env loading, logging, HTTP session helpers
- `pvlib/dxr.py`: DXR API calls (tags, searchable datasources, label statistics)
- `pvlib/atlas.py`: Purview data-plane auth, REST helpers, entity lookups and utilities
- `pvlib/typedefs.py`: Ensures custom entity/relationship types and merges new attributes
- `pvlib/governance.py`: Governance headers/base and Collections API helpers (shared-service and account-host)
- `pvlib/collections.py`: Collection helpers (slug/ref builders, ensure collections/UD collection)
- `pvlib/entities.py`: Upsert helpers for datasets and datasources
- `pvlib/relationships.py`: Create/update/delete relationships, mapping helpers
- `pvlib/stats.py`: Datasource/label hit statistics upserts and hit-based relationship wiring

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
 - Set `DISABLE_GOVERNANCE=1` and `PURVIEW_COLLECTION_ID=<collectionRefName>` to avoid governance calls entirely. Upserts go straight into the specified collection; move/collection creation is skipped.
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

## Security
Treat the `.env` as sensitive. Do not commit secrets. Prefer using a secret manager in production.
