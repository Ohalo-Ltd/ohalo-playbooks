# Graph RAG - Knowledge Graph Enhanced Retrieval

A full-stack Graph-based Retrieval Augmented Generation (RAG) system that combines vector search with knowledge graph relationships for enhanced document understanding and Q&A.

## 🎯 Current Status: Phase 2 Complete ✅

### Implemented Features

**Phase 1: The Searchable Graph** ✅
- ✅ Document ingestion from Data X-Ray API with retry logic
- ✅ Entity extraction and relationship parsing from DXR metadata
- ✅ Knowledge graph storage in Neo4j with constraints
- ✅ OpenAI embeddings with batching and vector similarity search
- ✅ Pydantic AI agent with basic Q&A
- ✅ Modern React chat UI with real-time responses
- ✅ PostgreSQL-based job tracking

**Phase 2: Graph-Augmented Retrieval** ✅
- ✅ Multi-tool agent (4 tools: vector search, entity lookup, graph neighbors, Cypher queries)
- ✅ Hybrid search combining vector similarity + graph expansion
- ✅ Dynamic schema discovery and analysis
- ✅ Graph traversal methods (neighbors, relationships, paths)
- ✅ Safe Cypher query execution (read-only)
- ✅ Enhanced UI with entity and relationship display
- ✅ Schema API endpoints for graph introspection

### Coming Next
- **Phase 3**: Interactive graph visualization and advanced reasoning
- **Phase 4**: Production polish (auth, monitoring, optimization)

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

### Query with Graph Context
```bash
POST /api/chat/query
Content-Type: application/json

{
  "question": "What entities are related to Paris?",
  "project_id": "my-project",
  "top_k": 5,
  "include_graph_context": true
}
```

Response:
```json
{
  "answer": "Based on the knowledge graph, Paris is related to several entities including France (as its location), the Eiffel Tower (a landmark within Paris), and the Louvre Museum...",
  "sources": [
    {
      "id": "chunk-123",
      "text": "Paris is the capital city of France...",
      "score": 0.95,
      "chunk_index": 0
    }
  ],
  "related_entities": [
    {
      "id": "paris-1",
      "name": "Paris",
      "type": "City"
    },
    {
      "id": "france-1",
      "name": "France",
      "type": "Country"
    },
    {
      "id": "eiffel-1",
      "name": "Eiffel Tower",
      "type": "Landmark"
    }
  ],
  "relationships": [
    {
      "from_entity": "Paris",
      "to_entity": "France",
      "relationship_type": "LOCATED_IN"
    },
    {
      "from_entity": "Eiffel Tower",
      "to_entity": "Paris",
      "relationship_type": "LOCATED_IN"
    }
  ],
  "conversation_id": "my-project"
}
```

### Get Graph Schema
```bash
GET /api/chat/schema
```

Response:
```json
{
  "entity_types": [
    {
      "type": "City",
      "count": 150,
      "properties": ["id", "name", "type", "population"]
    },
    {
      "type": "Country",
      "count": 50,
      "properties": ["id", "name", "type", "capital"]
    }
  ],
  "relationship_types": [
    {
      "type": "LOCATED_IN",
      "from_type": "City",
      "to_type": "Country",
      "count": 150
    }
  ],
  "statistics": {
    "Entity": 200,
    "Document": 100,
    "Chunk": 500,
    "relationships": 300
  }
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
