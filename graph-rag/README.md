# Graph RAG

A knowledge graph-powered RAG system that combines vector search with graph reasoning for intelligent document retrieval and question answering.

## Features

- **DXR Integration**: Ingests documents from Data X-Ray with pre-extracted entities
- **Graph Storage**: Neo4j knowledge graph with vector embeddings
- **AI Agent**: Pydantic AI-powered agent with multiple tools (vector search, graph traversal, Cypher queries)
- **Modern Stack**: FastAPI backend, Next.js frontend, PostgreSQL metadata storage

## Quick Start

### Prerequisites

- Docker & Docker Compose
- Python 3.11+ with [uv](https://github.com/astral-sh/uv)
- Node.js 20+
- API keys for OpenAI and Data X-Ray

### Setup

```bash
# Run setup script
chmod +x scripts/setup_dev.sh
./scripts/setup_dev.sh

# Edit .env with your API keys
nano .env
```

### Development

```bash
# Start all services with Docker Compose
docker-compose up -d

# Or run services individually:

# Backend (in backend/ directory)
uv run uvicorn api.app:app --reload

# Frontend (in frontend/ directory)
npm run dev
```

### Access

- **Frontend**: http://localhost:3000
- **Backend API**: http://localhost:8000
- **API Docs**: http://localhost:8000/docs
- **Neo4j Browser**: http://localhost:7474 (neo4j/graphrag123)
- **PostgreSQL**: localhost:5432 (graphrag/graphrag)

## Project Structure

```
graph-rag/
├── backend/          # FastAPI backend
├── frontend/         # Next.js frontend
├── infrastructure/   # Docker & K8s configs
├── docs/            # Documentation
└── scripts/         # Utility scripts
```

## Documentation

- [Implementation Plan](../IMPLEMENTATION.md)
- [Architecture](../ARCHITECTURE.md)
- [Vision](../VISION.md)

## License

MIT
