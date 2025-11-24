# Changelog - Graph RAG

All notable changes to the Graph RAG project are documented in this file.

## [Phase 2.0] - 2024-11-24

### 🎉 Major Features

#### Graph-Augmented Retrieval
- **Hybrid Search Service**: Combines vector similarity with graph expansion for richer context
  - Automatic entity and relationship deduplication
  - Configurable expansion depth (1-3 hops)
  - Entity-focused search variant
- **Enhanced Query Responses**: Now includes related entities and relationships alongside source chunks

#### Multi-Tool AI Agent
- **4 Agent Tools** (up from 1 in Phase 1):
  1. `vector_search`: Semantic similarity search (existing, enhanced)
  2. `entity_lookup`: Find entities by name (exact or fuzzy matching)
  3. `graph_neighbors`: Explore entity relationships and neighbors
  4. `graph_query`: Execute safe read-only Cypher queries
- **Enhanced System Prompt**: Multi-step reasoning strategy with tool usage guidelines

#### Schema Discovery
- **Dynamic Schema Inference**: Automatically discovers entity types, relationship patterns, and statistics
- **Quality Analysis**: Identifies duplicates, orphaned nodes, and schema anomalies
- **Sample Retrieval**: Get example entities and relationships by type

#### Graph Traversal
- **Neo4j Client Extensions**: 7 new methods for graph operations
  - `entity_lookup()`: Name-based entity search
  - `get_neighbors()`: Multi-hop relationship traversal
  - `traverse_relationships()`: Pattern-based path following
  - `expand_context_from_chunks()`: Enrich chunks with graph context
  - `execute_safe_cypher()`: Safe query execution with write protection
- **Safety Features**: Automatic LIMIT injection, write operation blocking

### 🔧 API Enhancements

#### New Endpoints
- `GET /api/chat/schema`: Get current knowledge graph schema
- `GET /api/chat/schema/suggestions`: Get schema improvement suggestions

#### Enhanced Endpoints
- `POST /api/chat/query`: Now returns `related_entities` and `relationships` in addition to `sources`
  - New request parameter: `include_graph_context` (default: true)
  - Enhanced response models: `SourceChunk`, `RelatedEntity`, `EntityRelationship`

### 🎨 Frontend Improvements

#### Enhanced Chat UI
- **Related Entities Card**: Visual display of entities discovered in the graph
  - Network icon header
  - Entity type labels
  - Wrapped badge layout
- **Relationships Card**: Visual representation of entity connections
  - Arrow indicators for directionality
  - Relationship type badges
  - Smart truncation (shows first 5, then count)
- **Enhanced Sources Card**: Improved readability with better styling

#### API Client Updates
- New TypeScript interfaces for all Phase 2 types
- Schema fetching methods (`getSchema()`, `getSchemaSuggestions()`)
- Updated request/response types with proper typing

### 📦 New Files

**Backend**:
- `backend/agent/hybrid_search.py`: Hybrid vector + graph search service
- `backend/ingestion/schema_discovery.py`: Schema inference and analysis

**Frontend**: (Modified existing files only)

**Documentation**:
- `docs/phase2-summary.md`: Complete Phase 2 implementation summary
- `docs/phase2-testing-guide.md`: Comprehensive testing scenarios

### 🔄 Modified Files

**Backend**:
- `backend/database/neo4j_client.py`: Added 7 graph traversal methods (~400 lines)
- `backend/agent/query_agent.py`: Added 3 new tools, enhanced system prompt (~120 lines)
- `backend/api/chat.py`: Enhanced query endpoint, added schema endpoints (~100 lines)

**Frontend**:
- `frontend/src/lib/api.ts`: Updated types and added schema methods
- `frontend/src/components/chat-message.tsx`: Enhanced with graph context display
- `frontend/src/components/chat-interface.tsx`: Updated data flow for new response types

**Documentation**:
- `README.md`: Updated to reflect Phase 2 completion
- `docs/testing-guide.md`: Remains focused on Phase 1 basics

### 🚀 Performance Improvements
- Graph queries limited to prevent expensive operations (max depth: 3)
- Automatic LIMIT injection for unbounded Cypher queries
- Entity/relationship deduplication using efficient dict/set operations

### 🛡️ Security Enhancements
- Safe Cypher execution blocks all write operations (CREATE, DELETE, MERGE, SET, REMOVE, DROP)
- Parameterized queries prevent Cypher injection
- Read-only agent tools by design

### 📊 Metrics
- **Code Added**: ~950 lines (800 backend, 150 frontend)
- **Agent Tools**: 1 → 4 (4x increase)
- **Response Context**: 3x richer (chunks + entities + relationships)
- **API Endpoints**: 3 → 5 (chat, ingestion, schema, suggestions)

---

## [Phase 1.0] - 2024-11-23

### 🎉 Initial Release

#### Core Features
- **Document Ingestion Pipeline**: DXR API integration with pagination and retry logic
- **Entity Extraction**: Parse entities and relationships from DXR metadata
- **Knowledge Graph Storage**: Neo4j with vector indexes and constraints
- **Vector Embeddings**: OpenAI text-embedding-3-small with batching and caching
- **Basic AI Agent**: Pydantic AI agent with vector search tool
- **Chat Interface**: Modern Next.js UI with real-time Q&A
- **Job Tracking**: PostgreSQL-based ingestion monitoring

#### Files Created
**Backend**:
- `backend/core/config.py`: Pydantic Settings configuration
- `backend/database/neo4j_client.py`: Neo4j client with vector search
- `backend/database/postgres_client.py`: Async PostgreSQL client
- `backend/ingestion/dxr_client.py`: Data X-Ray API client
- `backend/ingestion/metadata_parser.py`: Entity/relationship parser
- `backend/ingestion/embedder.py`: OpenAI embedding service
- `backend/ingestion/chunker.py`: Text chunking utility
- `backend/ingestion/graph_writer.py`: Neo4j write operations
- `backend/ingestion/service.py`: Ingestion orchestrator
- `backend/agent/query_agent.py`: Pydantic AI agent
- `backend/api/app.py`: FastAPI application
- `backend/api/chat.py`: Chat/query endpoints
- `backend/api/ingestion.py`: Ingestion endpoints

**Frontend**:
- `frontend/src/app/page.tsx`: Main chat page
- `frontend/src/components/chat-interface.tsx`: Chat UI component
- `frontend/src/components/chat-message.tsx`: Message display component
- `frontend/src/components/chat-input.tsx`: Input component
- `frontend/src/lib/api.ts`: API client

**Infrastructure**:
- `docker-compose.yml`: Neo4j + PostgreSQL services
- `infrastructure/init-postgres.sql`: Database schema
- `scripts/setup_dev.sh`: Development environment setup
- `start.sh`: One-command startup script

**Documentation**:
- `README.md`: Project overview and quick start
- `docs/phase1-summary.md`: Phase 1 implementation details
- `docs/testing-guide.md`: Testing scenarios and troubleshooting

### 🛠️ Tech Stack
- **Backend**: Python 3.11+, FastAPI, Pydantic AI 0.0.14, Neo4j driver, asyncpg, OpenAI SDK, httpx, uv
- **Frontend**: Next.js 16, TypeScript, TanStack Query, shadcn/ui, Tailwind CSS
- **Infrastructure**: Docker Compose, Neo4j 5.16, PostgreSQL 16 with pgcrypto

---

## Version History

- **Phase 2.0** (2024-11-24): Graph-augmented retrieval with multi-tool agent
- **Phase 1.0** (2024-11-23): Basic searchable graph with vector search
- **Phase 0.0** (2024-11-22): Project scaffolding and setup

---

## Roadmap

### Phase 3 (Planned)
- Interactive graph visualization (force-directed layout)
- Visual schema editor
- Logfire observability integration
- Advanced query patterns (multi-hop reasoning, path finding)

### Phase 4 (Planned)
- Authentication and authorization
- Multi-tenancy support
- Performance optimization and caching
- Kubernetes deployment manifests
- Production monitoring and alerting
