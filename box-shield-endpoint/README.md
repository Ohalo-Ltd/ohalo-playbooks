# Box Shield Stream Endpoint

Integrates Data X-Ray (DXR) results with Box Shield by mapping DXR classifications to Box Security Classifications and (optionally) managing Shield policies such as Information Barriers. This project mirrors the structure and DevEx of `purview-stream-endpoint` for consistency.

## What It Does (initial scope)
- Fetches DXR classifications and basic file hits.
- Maps DXR labels to Box Security Classifications (e.g., Public, Internal, Confidential) via a configurable mapping.
- Applies the mapped classification to Box files using Box APIs.
- Provides hooks to extend into Shield Information Barriers and access policies in later iterations.

## Best Practices Considered
- Use a Box JWT app with an App Service Account for server-to-server automation; use As-User only when necessary.
- Scope to least privilege (classifications, manage enterprise properties, and specific Shield scopes as required).
- Respect rate limits (HTTP 429) with exponential backoff; inspect `X-Rate-Limit-*` headers.
- Idempotent updates: read existing classification and update only when needed.
- Store mapping config in env (or a file) to keep policy separate from code.
- Support `RUN_ONCE` for batch runs and polling service mode with `POLL_SECONDS`.

## Prerequisites
- Python 3.10+ (tested with 3.12)
- A DXR instance and a Personal Access Token (PAT)
- A Box app (JWT recommended) with appropriate enterprise scopes; for local testing you can use a short-lived Developer Token

### Box App (JWT) quick notes
- Create a Custom App (JWT) in Box Developer Console
- Enable access to enterprise and request needed scopes (Security Classification, Shield features if required)
- Generate and securely store the App’s public/private key config (JSON)
- Authorize the app in the enterprise and add it to your Box tenant

## Install
```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\\Scripts\\activate
pip install --upgrade pip
pip install -r box-shield-endpoint/requirements.txt
```

Container image (optional):
```bash
cd box-shield-endpoint
docker build -t box-shield-dxr-sync:latest .
docker run --rm --env-file ./.env box-shield-dxr-sync:latest
# Continuous run: set RUN_ONCE=0
```

Docker Compose:
```bash
cd box-shield-endpoint
docker compose up --build
# Continuous: set RUN_ONCE=0 in .env and re-up
```

## Configuration
Place a `.env` file in `box-shield-endpoint/`. A template is provided at `.env.example`.

Required (DXR):
- `DXR_APP_URL`: Base URL, e.g. `https://<account>.dataxray.io`
- `DXR_PAT_TOKEN`: DXR Personal Access Token (Bearer)

Auth (choose one):
- Developer Token (for quick testing only):
  - `BOX_DEVELOPER_TOKEN` (short-lived token from Developer Console)
- JWT App (recommended for server-to-server):
  - `BOX_CONFIG_JSON_PATH`: Path to Box JWT app config JSON
  - `BOX_ENTERPRISE_ID`: Your Box enterprise ID

Classification mapping:
- `DXR_TO_BOX_CLASSIFICATIONS_JSON`: JSON dict mapping DXR label types/subtypes to Box classification keys; see example below.

Operational:
- `POLL_SECONDS`: Poll interval (default `60`)
- `RUN_ONCE`: Set to `1` to run once and exit
- `HTTP_TIMEOUT_SECONDS`: Per-request timeout (default `20`)
- `DXR_TAGS_LIMIT`: Optional limit for smoke runs

Example `.env` (do not commit real secrets):
```dotenv
DXR_APP_URL=https://demo.dataxray.io
DXR_PAT_TOKEN=eyJ...your-token...
# Dev token for quick start (remove in production)
BOX_DEVELOPER_TOKEN=your-box-dev-token
# JWT alternative (recommended for production)
# BOX_CONFIG_JSON_PATH=./box_config.json
# BOX_ENTERPRISE_ID=123456
RUN_ONCE=1
POLL_SECONDS=60
DXR_TO_BOX_CLASSIFICATIONS_JSON={
  "default": "Internal",
  "pii": "Confidential",
  "phi": "Highly Confidential"
}
```

## Run (Quick Start)
- Smoke run (limits DXR tags and uses short timeouts):
```bash
cd box-shield-endpoint
./run_once.sh --smoke
```
- Continuous polling service:
```bash
cd box-shield-endpoint
python box_shield_dxr_integration.py
```

## Sanity Check
```bash
cd box-shield-endpoint
./sanity-check.sh
```
This validates env, attempts Box auth, and probes the Security Classification metadata template.

## Developer Modules
- `boxlib/config.py`: Env loading, logging, HTTP session helpers
- `boxlib/dxr.py`: DXR API calls (classifications fetch)
- `boxlib/box_client.py`: Box auth (Developer Token or JWT) and raw REST helpers
- `boxlib/shield.py`: Classification apply helpers; future: IB segments/policies
- `boxlib/mapping.py`: DXR→Box classification mapping helpers

## Roadmap
- Ensure classification values exist and create missing ones
- Add Information Barrier segments and policies support
- Add unit tests and integration smoke tests
- Persist last-seen DXR sync cursor to reduce work per cycle
