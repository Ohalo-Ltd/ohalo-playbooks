# Graph RAG - Knowledge Graph Enhanced Retrieval

A full-stack Graph-based Retrieval Augmented Generation (RAG) system that combines vector search with knowledge graph relationships for enhanced document understanding and Q&A.

## 🎯 Current Status: Phase 1 Complete ✅

### Implemented Features
- ✅ **Document Ingestion**: Fetch documents from Data X-Ray API with retry logic
- ✅ **Entity Extraction**: Parse entities and relationships from DXR metadata
- ✅ **Knowledge Graph**: Store entities/relationships in Neo4j with constraints
- ✅ **Vector Search**: OpenAI embeddings with batching and similarity search
- ✅ **AI Agent**: Pydantic AI agent with vector search tool
- ✅ **Chat Interface**: Modern React UI with real-time Q&A
- ✅ **Job Tracking**: PostgreSQL-based ingestion job monitoring

### Coming Next
- **Phase 2**: Graph-Augmented Retrieval (entity-aware context expansion)
- **Phase 3**: Multi-Hop Reasoning (relationship traversal)
- **Phase 4**: Production Polish (auth, monitoring, optimization)

## Quick Start

### Prerequisites

- **Docker** (for Neo4j + PostgreSQL)
- **Python 3.11+** with [uv](https://github.com/astral-sh/uv) package manager
- **Node.js 18+** with npm
- **OpenAI API Key**
- **Data X-Ray API credentials**

### 1. Configure Environment

```bash
cd graph-rag
cp backend/.env.example backend/.env
```

Edit `backend/.env` with your credentials:
```bash
OPENAI_API_KEY=sk-...
DXR_API_URL=https://your-dxr-instance.com
DXR_API_KEY=your-key
```

### 2. Start All Services

```bash
./start.sh
```

This single command will:
- Start Neo4j and PostgreSQL in Docker
- Install Python dependencies with `uv`
- Start FastAPI backend on `:8000`
- Install Node.js dependencies
- Start Next.js frontend on `:3000`

### 3. Access the Application

- **Frontend UI**: http://localhost:3000
- **Backend API Docs**: http://localhost:8000/docs
- **Neo4j Browser**: http://localhost:7474 (no auth required)

## 🏗️ Architecture

```
┌─────────────────┐
│   Next.js UI    │ ← React + TanStack Query + shadcn/ui
└────────┬────────┘
         │
         ↓
┌─────────────────┐
│  FastAPI REST   │ ← Python + Pydantic AI
└────────┬────────┘
         │
    ┌────┴─────┬──────────────┐
    ↓          ↓              ↓
┌────────┐ ┌────────┐ ┌──────────────┐
│ Neo4j  │ │ OpenAI │ │ PostgreSQL   │
│ Graph  │ │  API   │ │ Job Tracking │
└────────┘ └────────┘ └──────────────┘
    ↑
    │
┌────────────┐
│ DXR API    │ ← Source documents
└────────────┘
```

## Project Structure

```
graph-rag/
├── backend/              # FastAPI backend
│   ├── api/             # REST endpoints
│   ├── ingestion/       # DXR client, parsers, embeddings
│   ├── database/        # Neo4j & PostgreSQL clients
│   ├── agent/           # Pydantic AI query agent
│   └── core/            # Configuration
├── frontend/            # Next.js frontend
│   └── src/
│       ├── app/         # Next.js pages
│       ├── components/  # React components
│       └── lib/         # API client
├── infrastructure/      # Docker configs & SQL
├── scripts/            # Setup & utility scripts
└── docs/               # Documentation
```

## 📝 API Examples

### Query Documents
```bash
POST /api/chat/query
Content-Type: application/json

{
  "query": "What entities are mentioned in the Paris documents?",
  "conversation_id": "optional-session-id"
}
```

Response:
```json
{
  "answer": "The Paris documents mention several entities including...",
  "conversation_id": "abc-123",
  "sources": [
    {
      "file_name": "paris-guide.pdf",
      "similarity": 0.95,
      "text": "Paris is the capital city..."
    }
  ],
  "entities_mentioned": ["Paris", "France", "Eiffel Tower"]
}
```

### Start Document Ingestion
```bash
POST /api/ingestion/start
Content-Type: application/json

{
  "datasource_name": "my-datasource"
}
```

### Check Ingestion Status
```bash
GET /api/ingestion/status/{job_id}
```

## 🔧 Development

### Running Tests
```bash
# Backend (when tests are added)
cd backend && uv run pytest

# Frontend (when tests are added)
cd frontend && npm test
```

### Exploring the Graph
Open Neo4j Browser at http://localhost:7474 and run Cypher queries:

```cypher
// View all entities
MATCH (e:Entity) RETURN e LIMIT 25;

// View entity relationships
MATCH (e1:Entity)-[r]->(e2:Entity) RETURN e1, r, e2 LIMIT 25;

// View document chunks
MATCH (d:Document)-[:HAS_CHUNK]->(c:Chunk) RETURN d, c LIMIT 10;
```

## Documentation

- [Implementation Plan](../IMPLEMENTATION.md)
- [Architecture](../ARCHITECTURE.md)
- [Vision](../VISION.md)

## License

MIT
