# Backend

## Setup

```bash
# Install dependencies with uv
uv sync

# Run development server
uv run uvicorn api.app:app --reload --host 0.0.0.0 --port 8000

# Run tests
uv run pytest

# Format code
uv run black .
uv run ruff check --fix .

# Type check
uv run mypy .
```

## Environment Variables

Copy `.env.example` to `.env` and configure:

```bash
cp ../.env.example .env
```
