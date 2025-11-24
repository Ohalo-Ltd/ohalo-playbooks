# Graph RAG System - Architecture Document

## System Overview

The Graph RAG platform bridges Data X-Ray's structured extractions with queryable knowledge graphs, enabling intelligent semantic and structural queries through a Pydantic AI agent with specialized tools.

```
┌─────────────────────────────────────────────────────────────────────┐
│                         User Interface Layer                        │
│                           (Next.js + shadcn/ui)                     │
│  ┌──────────────┐  ┌──────────────┐  ┌─────────────────────────┐  │
│  │ Chat UI      │  │ Graph        │  │ Project Config UI       │  │
│  │              │  │ Browser      │  │ (DXR + Extractor)       │  │
│  └──────────────┘  └──────────────┘  └─────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
                              ▲
                              │ HTTP/SSE
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      API Gateway (FastAPI)                          │
│  ┌──────────────┐  ┌──────────────┐  ┌─────────────────────────┐  │
│  │ Chat API     │  │ Graph API    │  │ Admin API               │  │
│  │ /chat        │  │ /graph       │  │ /projects /ingestion    │  │
│  └──────────────┘  └──────────────┘  └─────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
                              ▲
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    Query Agent (Pydantic AI)                        │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │ Single Agent with System Prompt                               │  │
│  │ - Understands project schema and graph structure              │  │
│  │ - Decides which tools to use and in what order                │  │
│  │ - Combines results and generates responses                    │  │
│  └──────────────────────────────────────────────────────────────┘  │
│                                                                      │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │ Tools (Functions callable by agent)                           │  │
│  │ ┌────────────────┐ ┌────────────────┐ ┌──────────────────┐  │  │
│  │ │ graph_query    │ │ vector_search  │ │ get_entity       │  │  │
│  │ │ (Cypher exec)  │ │ (similarity)   │ │ (by ID)          │  │  │
│  │ └────────────────┘ └────────────────┘ └──────────────────┘  │  │
│  │ ┌────────────────┐ ┌────────────────┐ ┌──────────────────┐  │  │
│  │ │ traverse_graph │ │ get_neighbors  │ │ fulltext_search  │  │  │
│  │ │ (paths)        │ │ (expand node)  │ │ (keyword)        │  │  │
│  │ └────────────────┘ └────────────────┘ └──────────────────┘  │  │
│  └──────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
                              ▲
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      Storage & Indexing Layer                       │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │ Neo4j Graph Database + Vector Index (per project)            │  │
│  │ - Nodes: Document, Entity (from DXR extractor)               │  │
│  │ - Edges: HAS_ENTITY, relationships from extractor JSON       │  │
│  │ - Vector: Chunks with embeddings linked to documents         │  │
│  │ - Indexes: Full-text, vector similarity, property            │  │
│  └──────────────────────────────────────────────────────────────┘  │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │ Metadata Database (PostgreSQL + pgcrypto)                    │  │
│  │ - Project configurations                                      │  │
│  │ - Encrypted API keys (DXR, OpenAI)                           │  │
│  │ - Extractor ID mappings                                       │  │
│  │ - Ingestion job status                                        │  │
│  └──────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
                              ▲
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      Ingestion Pipeline Layer                       │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │ DXR Client                                                    │  │
│  │ - Fetches /api/v1/files with all metadata                    │  │
│  │ - Reads extracted_metadata#EXTRACTOR_ID field                │  │
│  │ - Parses nodes/relationships JSON                             │  │
│  └──────────────────────────────────────────────────────────────┘  │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │ Chunking & Embedding Service                                  │  │
│  │ - Chunks document text (overlap for context)                  │  │
│  │ - Generates embeddings via OpenAI API                         │  │
│  │ - Links chunks to source document                             │  │
│  └──────────────────────────────────────────────────────────────┘  │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │ Graph Writer                                                  │  │
│  │ - Creates Document nodes                                      │  │
│  │ - Creates Entity nodes from extractor JSON                    │  │
│  │ - Creates relationships from extractor JSON                   │  │
│  │ - Links document → entities, document → chunks               │  │
│  └──────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
                              ▲
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     External Services Layer                         │
│  ┌──────────────────────┐  ┌──────────────────────────────────┐   │
│  │ Data X-Ray API       │  │ OpenAI API                       │   │
│  │ /api/v1/files        │  │ (Embeddings + Chat completion)   │   │
│  └──────────────────────┘  └──────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
```

## Core Components

### 1. Frontend (Next.js 14+ App Router)

#### Chat Interface
**Location**: `frontend/src/app/chat/`

**Responsibilities**:
- Real-time chat with streaming responses
- Display source citations with graph links
- Message history persistence
- Project selector

**Key Technologies**:
- Next.js App Router
- TanStack Query for state management
- Server-Sent Events (SSE) for streaming
- **shadcn/ui components exclusively** (no custom UI components)

#### Graph Browser
**Location**: `frontend/src/app/graph/`

**Responsibilities**:
- Interactive graph visualization
- Filter by entity/relationship types
- Expand/collapse subgraphs
- Search nodes
- Export subgraphs

**Options**:
1. **Neo4j Bloom** - Embedded iframe (requires Aura/Enterprise)
2. **react-force-graph** - Custom D3-based visualization
3. **Cytoscape.js** - Advanced graph analytics
4. **vis.js** - Lightweight alternative

**Recommendation**: Start with `react-force-graph` for flexibility, integrate Bloom later for enterprise clients.

#### Project Configuration UI
**Location**: `frontend/src/app/admin/`

**Responsibilities**:
- Create/edit projects
- Configure DXR connection (base URL, datasource ID, API key)
- Specify LLM Extractor ID for extracting nodes/relationships
- Configure OpenAI API key for embeddings
- Set custom system prompt per project
- Ingestion job monitoring

**Features**:
- Simple form-based project setup (shadcn/ui Form components)
- Connection testing to DXR API
- Real-time ingestion logs
- Helper text explaining extractor JSON format requirement

### 2. Backend API (FastAPI)

#### Project Structure
```
backend/
├── api/
│   ├── __init__.py
│   ├── main.py              # FastAPI app, middleware, CORS
│   ├── chat.py              # Chat endpoints (SSE streaming)
│   ├── graph.py             # Graph query endpoints
│   ├── projects.py          # Project CRUD
│   └── ingestion.py         # Trigger ingestion jobs
├── agent/
│   ├── __init__.py
│   ├── query_agent.py       # Single Pydantic AI agent
│   └── tools.py             # Agent tools (graph_query, vector_search, etc.)
├── core/
│   ├── __init__.py
│   ├── config.py            # Settings (Pydantic BaseSettings)
│   └── dependencies.py      # Dependency injection
├── database/
│   ├── __init__.py
│   ├── neo4j_client.py      # Neo4j connection pool
│   ├── postgres_client.py   # Metadata DB client (with pgcrypto)
│   └── models.py            # Pydantic models for DB
├── ingestion/
│   ├── __init__.py
│   ├── dxr_client.py        # Data X-Ray /api/v1/files client
│   ├── embedder.py          # Embedding generation
│   └── graph_writer.py      # Neo4j write operations
├── services/
│   ├── __init__.py
│   ├── project_service.py   # Project management
│   ├── secrets_service.py   # Encrypted API key storage/retrieval
│   └── query_service.py     # Query orchestration
└── pyproject.toml
```

#### Key Endpoints

**Chat API** (`/api/chat`)
```python
POST /api/chat/query
{
  "project_id": "uuid",
  "message": "string",
  "conversation_id": "uuid?",
  "stream": bool
}

Response (SSE stream):
event: thought
data: {"agent": "query_agent", "content": "Breaking down query..."}

event: graph_query
data: {"cypher": "MATCH (p:Person)...", "results": [...]}

event: vector_results
data: {"documents": [...], "scores": [...]}

event: response
data: {"content": "Based on the graph...", "citations": [...]}

event: done
data: {}
```

**Graph API** (`/api/graph`)
```python
GET /api/graph/explore
?project_id=uuid&entity_id=uuid&depth=2

POST /api/graph/query
{
  "project_id": "uuid",
  "cypher": "MATCH (n:Contract) RETURN n LIMIT 10"
}

GET /api/graph/neighbors
?project_id=uuid&node_id=uuid&relationship_types=[]
```

**Project API** (`/api/projects`)
```python
POST /api/projects
{
  "name": "Contract Intelligence",
  "description": "Vendor contract analysis",
  "dxr_base_url": "https://api.ohalo.co",
  "dxr_datasource_id": "ds_abc123",
  "dxr_api_key": "dxr_key_...",  # Encrypted in DB with pgcrypto
  "extractor_id": "ext_xyz789",  # LLM Extractor ID for nodes/relationships
  "openai_api_key": "sk-...",    # Encrypted in DB with pgcrypto
  "system_prompt": "You are an expert contract analyst..."
}

GET /api/projects/{project_id}
PUT /api/projects/{project_id}
DELETE /api/projects/{project_id}
```

**Ingestion API** (`/api/ingestion`)
```python
POST /api/ingestion/start
{
  "project_id": "uuid",
  "full_refresh": bool
}

GET /api/ingestion/status/{job_id}
```

### 3. Agent Layer (Pydantic AI)

#### Single Agent Architecture

The system uses **one Pydantic AI agent** with multiple tools, not multiple agents. The agent is the "brain" that uses a project-specific system prompt and dynamically decides which tools to call based on the user's query.

**Agent Configuration**:
- **Model**: OpenAI GPT-4o (or configurable per project)
- **System Prompt**: Customizable per project, includes:
  - Graph structure understanding (inferred from data)
  - Available entity and relationship types
  - Query strategies (when to use graph traversal vs vector search)
  - Response formatting guidelines
- **Dependencies**: Project config, Neo4j client, graph schema metadata
- **Output**: Structured response with citations and sources

#### Agent Tools

Tools are deterministic Python functions that the agent can call. Each tool has a specific, single purpose:

**Available Tools**:
1. **`graph_query`** - Execute read-only Cypher queries
2. **`vector_search`** - Find semantically similar chunks/documents
3. **`get_entity`** - Retrieve entity by ID with all properties
4. **`get_neighbors`** - Get connected nodes for a given entity
5. **`traverse_graph`** - Find paths between entities
6. **`fulltext_search`** - Keyword search across nodes
7. **`get_document`** - Retrieve full document content and metadata

**Tool Design Principles**:
- Each tool is a single-purpose function
- Tools are read-only (no writes during queries)
- Tools return structured data (Pydantic models)
- Tools handle errors gracefully and return empty results vs exceptions
- Tools are instrumented for observability (Logfire)

**Agent Workflow**:
1. User submits query
2. Agent receives query + system prompt + project context
3. Agent reasons about which tools to use
4. Agent calls tools (can call multiple in sequence)
5. Agent synthesizes results into coherent response
6. Response includes citations to source documents/entities

### 4. Storage Layer

#### Neo4j Graph Database

**Database Per Project**: Each project gets its own Neo4j database for complete isolation.

**Node Types**:
```cypher
// Document node - source file from DXR
(:Document {
  id: 'file_abc123',
  title: 'Master Service Agreement.pdf',
  source_url: 's3://...',
  dxr_categories: ['Contract', 'Legal'],
  ingested_at: datetime()
})

// Entity nodes - extracted by DXR Extractor
(:Entity {
  id: 'entity_xyz',
  type: 'Contract',  // From extractor JSON
  properties: {...}, // All extracted properties as JSON
  source_document_id: 'file_abc123'
})

// Chunk nodes - text segments with embeddings
(:Chunk {
  id: 'chunk_001',
  text: '...',
  embedding: [0.123, 0.456, ...],  // Vector embedding
  start_char: 0,
  end_char: 500,
  document_id: 'file_abc123'
})
```

**Relationships**:
```cypher
// Document to entities (from extractor JSON)
(doc:Document)-[:HAS_ENTITY]->(entity:Entity)

// Document to chunks
(doc:Document)-[:HAS_CHUNK]->(chunk:Chunk)

// Entity relationships (from extractor JSON)
// Relationship types are dynamic based on extractor output
(entity1:Entity)-[:SIGNED_BY|REFERENCES|DEPENDS_ON|...]->(entity2:Entity)
```

**Indexes**:
```cypher
// Property indexes
CREATE INDEX entity_id FOR (n:Entity) ON (n.id);
CREATE INDEX document_id FOR (n:Document) ON (n.id);
CREATE INDEX entity_type FOR (n:Entity) ON (n.type);

// Vector index for semantic search
CREATE VECTOR INDEX chunk_embeddings FOR (n:Chunk) ON (n.embedding)
OPTIONS {indexConfig: {
  `vector.dimensions`: 1536,
  `vector.similarity_function`: 'cosine'
}};

// Full-text search
CREATE FULLTEXT INDEX entity_fulltext FOR (n:Entity) ON EACH [n.properties];
```

#### PostgreSQL Metadata Database

**Extensions Required**:
```sql
CREATE EXTENSION IF NOT EXISTS pgcrypto;  -- For API key encryption
```

**Schema**:
```sql
-- Projects
CREATE TABLE projects (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    description TEXT,
    
    -- DXR Configuration
    dxr_base_url VARCHAR(255) NOT NULL,
    dxr_datasource_id VARCHAR(255) NOT NULL,
    dxr_api_key_encrypted BYTEA NOT NULL,  -- Encrypted with pgcrypto
    extractor_id VARCHAR(255) NOT NULL,     -- LLM Extractor ID for nodes/relationships
    
    -- OpenAI Configuration  
    openai_api_key_encrypted BYTEA NOT NULL,  -- Encrypted with pgcrypto
    
    -- Agent Configuration
    system_prompt TEXT,
    
    -- Neo4j Database
    neo4j_database VARCHAR(255) NOT NULL,  -- project_{id}
    
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Ingestion Jobs
CREATE TABLE ingestion_jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID REFERENCES projects(id) ON DELETE CASCADE,
    status VARCHAR(50) NOT NULL,  -- 'pending', 'running', 'completed', 'failed'
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    documents_processed INT DEFAULT 0,
    entities_extracted INT DEFAULT 0,
    relationships_created INT DEFAULT 0,
    chunks_created INT DEFAULT 0,
    error_log TEXT,
    stats JSONB  -- Detailed stats (entity types, relationship types, etc.)
);

-- Schema Cache (inferred from ingested data)
CREATE TABLE graph_schemas (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID REFERENCES projects(id) ON DELETE CASCADE,
    entity_types JSONB,        -- Array of discovered entity types
    relationship_types JSONB,   -- Array of discovered relationship types
    sample_properties JSONB,    -- Sample properties per entity type
    last_updated TIMESTAMP DEFAULT NOW(),
    UNIQUE(project_id)
);
```

**API Key Encryption/Decryption**:
```sql
-- Encrypt API key on insert
INSERT INTO projects (name, dxr_api_key_encrypted) 
VALUES ('My Project', pgp_sym_encrypt('dxr_key_123', 'encryption_password'));

-- Decrypt API key on read
SELECT pgp_sym_decrypt(dxr_api_key_encrypted, 'encryption_password') AS dxr_api_key
FROM projects WHERE id = 'project_id';
```

### 5. Ingestion Pipeline

#### Data Flow

```
1. Fetch Files from DXR
   ├─> DXR API: GET /api/v1/files?datasource_id={id}
   ├─> Parse file metadata for each file:
   │   ├─> file_id, title, source_url, categories
   │   └─> extracted_metadata#EXTRACTOR_ID field
   └─> Filter files that have the required extractor metadata

2. Parse Extractor JSON
   ├─> Read extracted_metadata#EXTRACTOR_ID field (JSON string)
   ├─> Parse JSON structure:
   │   {
   │     "nodes": [
   │       {"id": "...", "type": "Contract", "properties": {...}},
   │       {"id": "...", "type": "Party", "properties": {...}}
   │     ],
   │     "relationships": [
   │       {"from": "node_id_1", "to": "node_id_2", "type": "SIGNED_BY", "properties": {...}}
   │     ]
   │   }
   └─> Validate JSON structure (has nodes and relationships arrays)

3. Chunk Documents & Generate Embeddings
   ├─> Download document content from DXR
   ├─> Chunk text (500 chars with 50 char overlap)
   ├─> Generate embeddings for each chunk (OpenAI API)
   └─> Associate chunks with document ID

4. Write to Graph
   ├─> Create Document node
   ├─> Create Entity nodes from extractor JSON "nodes" array
   ├─> Create relationships from extractor JSON "relationships" array  
   ├─> Create Chunk nodes with embeddings
   ├─> Link Document → Entities (HAS_ENTITY)
   ├─> Link Document → Chunks (HAS_CHUNK)
   └─> Update graph schema cache (discovered entity/relationship types)

5. Update Job Status
   └─> Record stats in Postgres (docs processed, entities created, etc.)
```

#### DXR Client

**Responsibilities**:
- Fetch files from `/api/v1/files` endpoint
- Parse `extracted_metadata#EXTRACTOR_ID` field
- Handle pagination (DXR returns max 1000 files per request)
- Download document content for chunking
- Retry on failures with exponential backoff

#### Embedding Service

**Responsibilities**:
- Generate embeddings for document chunks using OpenAI API
- Batch processing to minimize API calls (up to 100 texts per request)
- Cache embeddings to avoid re-generating
- Track token usage and costs

#### Graph Writer

**Responsibilities**:
- Create Document nodes with metadata
- Create Entity nodes from extractor JSON
- Create dynamic relationships (types come from extractor)
- Create Chunk nodes with vector embeddings
- Link nodes with appropriate relationships
- Handle duplicate entities (merge by ID)
- Update graph schema cache with discovered types

### 6. Schema Discovery

Instead of requiring users to define schemas upfront, the system **discovers schemas** from the ingested data:

**Schema Discovery Process**:
1. During ingestion, track all entity types found in extractor JSONs
2. Track all relationship types found in extractor JSONs
3. Track sample properties for each entity type
4. Store in `graph_schemas` table in Postgres
5. Update dynamically as new documents are ingested

**Schema Usage**:
- **Agent System Prompt**: Include discovered entity/relationship types so agent knows what exists
- **UI Autocomplete**: Show available entity types in filters
- **Graph Browser**: Color-code nodes by entity type
- **Query Suggestions**: Suggest queries based on available relationships

**Schema Cache Example**:
```json
{
  "entity_types": ["Contract", "Party", "Obligation", "Deliverable"],
  "relationship_types": ["SIGNED_BY", "OBLIGATES", "REFERENCES"],
  "sample_properties": {
    "Contract": {
      "title": "string",
      "effective_date": "date",
      "value": "number"
    },
    "Party": {
      "name": "string",
      "type": "enum[person,company,government]"
    }
  }
}

### 7. Query Execution

#### Hybrid Query Flow

1. **User submits query** via chat UI
2. **Agent receives context**:
   - User query text
   - Project system prompt
   - Discovered schema (entity/relationship types)
   - Conversation history
3. **Agent decides strategy** and calls tools:
   - `graph_query` for relationship-based queries ("Find contracts signed by X")
   - `vector_search` for semantic similarity ("Find similar documents")
   - `traverse_graph` for multi-hop queries ("Path from A to B")
   - Multiple tools can be called in sequence
4. **Agent synthesizes response**:
   - Combines results from tools
   - Generates natural language response
   - Includes citations to source documents/entities
5. **Response streamed to UI** via Server-Sent Events

#### Vector Search in Neo4j

- Uses Neo4j's native vector index on `Chunk.embedding`
- Returns top-K similar chunks based on cosine similarity
- Can filter by document properties (categories, dates)
- Each chunk links back to source document and entities

## Technology Stack

### Backend
- **Python**: 3.11+
- **Framework**: FastAPI 0.100+
- **Agent Framework**: Pydantic AI 1.22+
- **Async**: asyncio, httpx, aiofiles
- **Validation**: Pydantic V2
- **Package Manager**: uv

### Database
- **Graph DB**: Neo4j 5.13+ (Community or Aura)
- **Vector Support**: Native Neo4j vector indexes
- **Metadata DB**: PostgreSQL 15+
- **Drivers**: neo4j-driver (async), asyncpg

### LLM & Embeddings
- **Chat Model**: OpenAI GPT-4o (configurable)
- **Embeddings**: OpenAI text-embedding-3-large (1536 dimensions)
- **Observability**: Pydantic Logfire

### Frontend
- **Framework**: Next.js 14+ (App Router)
- **Language**: TypeScript 5+
- **UI Library**: React 18+
- **Components**: shadcn/ui (Radix UI + Tailwind)
- **Graph Viz**: react-force-graph or Neo4j Bloom
- **State**: React Query (TanStack Query)
- **Package Manager**: npm or pnpm

### Infrastructure
- **Containerization**: Docker, Docker Compose
- **Orchestration**: Kubernetes (production)
- **CI/CD**: GitHub Actions
- **Secrets**: AWS Secrets Manager or Vault
- **Monitoring**: Pydantic Logfire + Prometheus + Grafana

## Security Architecture

### API Key Encryption
- **PostgreSQL pgcrypto extension** encrypts all API keys at rest
- Encryption key stored in environment variable (never in code/DB)
- Keys decrypted only when needed for API calls
- Backend service holds encryption key, not exposed to frontend

### Data Isolation
- **Separate Neo4j database per project** ensures complete isolation
- No cross-project queries possible
- Database names follow pattern: `project_{uuid}`
- Metadata DB enforces project ownership

### API Security
- No authentication required initially (single-user deployment)
- All API endpoints validate project existence
- Read-only Cypher queries enforced in agent tools
- Rate limiting on chat endpoints

### Secrets Management
- API keys encrypted in PostgreSQL with pgcrypto
- Encryption password from environment variable
- No secrets in code, logs, or frontend
- Secrets never returned in API responses (masked in UI)

## Observability

### Instrumentation
- **Pydantic Logfire** (optional but recommended)
  - Automatic tracing of all agent calls
  - Token usage tracking per query
  - Request/response logging
  - Performance metrics

### Metrics
- Query latency (P50, P95, P99)
- Agent tool call frequency
- Ingestion throughput (docs/hour)
- Graph size (nodes, relationships per project)
- API error rates
- OpenAI API costs per project

### Logging
- Structured JSON logs
- Log levels: DEBUG, INFO, WARNING, ERROR
- Request IDs for tracing
- Agent reasoning logged for debugging

## Deployment Architecture

### Development (Docker Compose)
- **Neo4j** container with APOC plugin
- **PostgreSQL** container with pgcrypto
- **Backend** FastAPI service
- **Frontend** Next.js dev server
- All services networked together
- Volume mounts for data persistence

### Production
- **Neo4j Aura** or self-hosted Neo4j cluster
- **PostgreSQL** (RDS, managed service, or containerized)
- **Backend**: Containerized FastAPI with multiple replicas
- **Frontend**: Next.js static export or SSR deployment
- **Load balancer** for backend API
- **Container orchestration**: Docker Compose (simple) or Kubernetes (enterprise)

## Performance Considerations

### Caching Strategy
- **Query results**: Redis cache (5min TTL)
- **Embeddings**: Cache in Neo4j (avoid regenerating)
- **Schema**: In-memory cache (invalidate on update)

### Optimization
- **Batch operations**: Process 100 docs at a time
- **Parallel ingestion**: Multiple workers per project
- **Index tuning**: Composite indexes on frequently queried properties
- **Query optimization**: Use `EXPLAIN` to tune Cypher
- **Connection pooling**: Reuse Neo4j driver sessions

### Scalability Limits (v1)
- **Projects**: 100 concurrent projects
- **Documents/Project**: 100K documents
- **Entities/Project**: 1M entities
- **Query latency**: <2s for P95
- **Ingestion**: 1K docs/hour

---

**Document Owner**: AI Development Team  
**Last Updated**: November 24, 2025  
**Status**: Draft for Review  
**Related**: VISION.md, IMPLEMENTATION.md
