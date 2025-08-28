# Repository Guidelines

## Project Structure & Modules
- Root: Multi-playbook repo. Key folders:
  - `rag-app/`: Full-stack RAG demo (`backend/` FastAPI + Postgres, `frontend/` Next.js).
  - `scan-email-attachments/`: SMTP proxy playbook with tests under `tests/` and app code in `src/smtp_proxy/`.
  - `purview-stream-endpoint/`: Purview integration scripts (`pvlib/` helpers, integration + test files).
  - `export-stream-endpoint/`: Terraform + Lambda under `terraform/` (Python in `terraform/lambda/`).
  - Root config: `pytest.ini` (markers), `.env` (local vars, do not commit secrets).

## Build, Test, and Dev Commands
- RAG app (both services): `cd rag-app && ./start.sh` (add `--reset-db` to drop Postgres volume).
- RAG backend only: `cd rag-app/backend && ./start.sh` (requires Docker running; serves on `:8000`).
- RAG frontend only: `cd rag-app/frontend && npm ci && npm run dev` (serves on `:3000`).
- Email attachments playbook: `cd scan-email-attachments && ./start_playbook.sh`.
- Purview integration smoke runs: `cd purview-stream-endpoint && ./run_once.sh` or `./sanity-check.sh`.
- Export stream build/deploy: `cd export-stream-endpoint/terraform/lambda && ./build.sh`; then `cd .. && terraform init && terraform apply`.
- Tests:
  - Root markers: `pytest -m unit`, `-m integration`, or `-m destructive`.
  - Email playbook: `cd scan-email-attachments && pytest -q`.
  - Purview: `cd purview-stream-endpoint && pytest -q`.

## Coding Style & Naming
- Python: 4-space indent, `snake_case` for files/functions, `PascalCase` classes.
  - Tools (email playbook): Black (88), isort (profile=black), mypy. Run: `black . && isort . && mypy`.
- TypeScript/React (frontend): 2-space indent, ESLint + Next.js defaults; components `PascalCase`, files typically `kebab-case.tsx`.
- Terraform: keep modules and variables in `terraform/*`; prefer descriptive resource names.

## Testing Guidelines
- Pytest conventions: files `test_*.py`, classes `Test*`, functions `test_*` (see `scan-email-attachments/pytest.ini`).
- Use markers from root `pytest.ini` for scope; avoid running `destructive` by default.
- Provide a simple repro command in PRs (e.g., `cd purview-stream-endpoint && pytest -k dxr`).

## Commit & PR Guidelines
- Commits: present tense, concise scope (e.g., "Add Purview dataset stats"). Reference issues/PRs when relevant.
- PRs must include:
  - Purpose and summary, linked issue(s).
  - Test plan and commands run; screenshots for UI changes.
  - Notes on configs/secrets touched (never include secrets; use `.env` and commit `.env.example`).

## Security & Config
- Never commit secrets. Use `.env` files locally; provide safe `.env.example` when adding new variables.
- Validate cloud-impacting changes with `-m integration` in a sandbox account; gate `-m destructive` behind explicit approval.
