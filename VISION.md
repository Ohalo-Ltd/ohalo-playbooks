# Graph RAG System - Vision Document

## Executive Summary

We are building a **configurable Graph RAG platform** that transforms unstructured data into queryable knowledge graphs, enabling semantic search, relationship discovery, and intelligent querying across diverse enterprise datasets. The system will serve as a foundation for multiple client implementations, each with unique data sources and schemas, while maintaining a consistent core engine.

## Problem Statement

Traditional RAG systems treat documents as isolated chunks, missing the rich relationships and structure inherent in enterprise data. While vector similarity retrieves relevant content, it fails to:

- **Capture entity relationships** - People, projects, organizations, and concepts are interconnected
- **Leverage structural patterns** - Documents have inherent schemas (e.g., contracts, reports, compliance filings)
- **Enable graph-based reasoning** - Complex queries require traversing relationships, not just similarity matching
- **Adapt to different domains** - Each client has unique entity types and relationship patterns

Current limitations with our RAG implementations:
- Row-level security is enforced but relationships between documents are lost
- Schema extraction is manual and inflexible
- Each new use case requires rebuilding the entire application
- No visual exploration of how entities connect

## Vision

Create a **two-layer architecture** that separates concerns:

### 1. Foundation Layer (Universal)
A production-ready Graph RAG engine that:
- Ingests documents from Data X-Ray
- Extracts entities and relationships based on configurable schemas
- Stores both graph structure (nodes/edges) and vector embeddings in a unified database
- Provides intelligent agents that can reason over both graph topology and semantic similarity
- Offers a visual graph browser for exploration and debugging
- Enforces access controls and security policies
- Scales to enterprise workloads with observability and monitoring

### 2. Customization Layer (Project-Specific)
Lightweight configuration that defines:
- **DXR Connection** - Base URL, datasource ID, API key (encrypted)
- **LLM Extractor ID** - Which extractor provides nodes/relationships JSON
- **OpenAI API Key** - For embeddings and chat (encrypted)
- **System Prompt** - Domain-specific instructions for the query agent
- **Graph Schema** - Automatically discovered from ingested data

## Core Capabilities

### DXR-Driven Entity Extraction
- DXR LLM Extractors handle entity and relationship extraction
- Extracted metadata stored in `extracted_metadata#EXTRACTOR_ID` field as JSON
- JSON structure: `{"nodes": [...], "relationships": [...]}`
- System ingests pre-extracted structured data (no extraction during ingestion)
- Schema discovered dynamically from ingested entities
- Validation ensures extractor output matches expected format

### Hybrid Graph-Vector Storage
- Store entities as graph nodes with properties
- Store relationships as typed edges with metadata
- Embed text content as vectors attached to nodes
- Enable queries that combine:
  - **Graph traversal** - "Find all contracts related to Project X through suppliers"
  - **Vector similarity** - "Find documents semantically similar to this query"
  - **Hybrid** - "Find projects connected to Entity Y and semantically related to 'risk assessment'"

### Query Agent with Specialized Tools
- **Single Pydantic AI agent** that understands graph structure and user intent
- **Agent tools** for specific actions:
  - Graph query execution (Cypher)
  - Vector similarity search
  - Entity retrieval and traversal
  - Full-text search
- Agent dynamically decides which tools to use based on query
- Tools are deterministic Python functions, agent is the intelligent decision-maker
- Combines results from multiple tool calls into coherent responses

### Visual Graph Exploration
- Interactive graph browser (e.g., Neo4j Bloom, Graphistry, or custom)
- Filter by entity type, relationship type, metadata
- Highlight paths between entities
- Overlay vector similarity as visual weights
- Export subgraphs for presentations

### Project Management System
- **Multi-tenancy** - Isolate graphs per project/client
- **Configuration UI** - Setup wizard for new projects
- **Schema designer** - Visual or code-based schema definition
- **Data source connector** - OAuth/API key management for DXR
- **Monitoring dashboard** - Ingestion status, query performance, agent traces

## Use Cases

### Use Case 1: Contract Intelligence
**Client**: Legal department analyzing vendor contracts

**Schema**:
- Entities: `Contract`, `Party`, `Obligation`, `Deliverable`, `PaymentTerm`, `RiskClause`
- Relationships: `SIGNED_BY`, `OBLIGATES`, `REFERENCES`, `SUPERSEDES`

**Queries**:
- "Which contracts obligate Party X to deliver software?"
- "Find all payment terms similar to net-90 across contracts signed in 2024"
- "Show me the dependency chain if Supplier Y defaults"

### Use Case 2: Research Publication Network
**Client**: University tracking research output and collaborations

**Schema**:
- Entities: `Paper`, `Author`, `Institution`, `Grant`, `Topic`, `Dataset`
- Relationships: `AUTHORED_BY`, `FUNDED_BY`, `CITES`, `USES_DATASET`, `AFFILIATED_WITH`

**Queries**:
- "Which authors collaborate most frequently on AI ethics?"
- "Find datasets used in papers similar to my research proposal"
- "Trace citation paths from this paper to Nobel Prize winners"

### Use Case 3: Compliance & Governance
**Client**: Financial services tracking regulatory requirements

**Schema**:
- Entities: `Regulation`, `Requirement`, `Control`, `Evidence`, `System`, `Risk`
- Relationships: `MANDATES`, `IMPLEMENTS`, `MITIGATES`, `EVIDENCED_BY`

**Queries**:
- "What controls do we have for GDPR Article 25?"
- "Find systems not evidencing required SOC 2 controls"
- "Show me similar compliance frameworks to CCPA"

### Use Case 4: Program Element Tracking (Leidos-style)
**Client**: Government contractor managing program elements

**Schema**:
- Entities: `ProgramElement`, `Project`, `Resource`, `Milestone`, `Risk`, `Budget`
- Relationships: `FUNDED_BY`, `DEPENDS_ON`, `ALLOCATED_TO`, `CONTRIBUTES_TO`

**Queries**:
- "Which projects are funded by PE 12345 and delayed?"
- "Find budget allocations similar to historical patterns"
- "Show dependency chain for critical milestones"

## Success Metrics

### Technical Metrics
- **Ingestion throughput** - Documents processed per hour
- **Query latency** - P50/P95/P99 for hybrid queries
- **Graph quality** - Entity extraction accuracy, relationship precision/recall
- **Agent effectiveness** - Query success rate, multi-hop reasoning accuracy

### Business Metrics
- **Time to deployment** - New project setup in <1 day
- **Reusability** - 80%+ code shared across projects
- **User satisfaction** - Query relevance scores, NPS
- **Scalability** - Support 100K+ documents, 1M+ entities per project

## Design Principles

1. **Configuration over Code** - New projects should require minimal programming
2. **Schema First** - Explicit schemas enable better extraction and querying
3. **Observability Built-In** - Every agent action, query, and ingestion step is traced
4. **Security by Default** - Inherit DXR permissions, enforce RLS where applicable
5. **Local-First Development** - Developers can run the full stack on their machine
6. **Cloud-Ready Deployment** - Docker containers, Kubernetes manifests, IaC templates
7. **Incremental Adoption** - Start with graph, add vector search, enable agents progressively

## Non-Goals (Out of Scope for v1)

- **Real-time ingestion** - Focus on batch processing initially
- **Custom LLM training** - Use foundation models with prompt engineering
- **Mobile applications** - Web UI only
- **Blockchain/provenance** - Trust DXR's audit trail
- **Multi-language support** - English only for v1

## Technology Choices (Tentative)

### Core Stack
- **Backend** - Python 3.11+, FastAPI, Pydantic AI v1.22+
- **Graph Database** - Neo4j Community (with vector index) or Neo4j Aura
  - Native vector search (since Neo4j 5.13+)
  - Cypher query language
  - APOC procedures for graph algorithms
  - Neo4j Bloom for visualization
- **Vector Strategy** - Unified in Neo4j (nodes have vector properties)
- **Embeddings** - OpenAI `text-embedding-3-large` or similar
- **Agent Framework** - Pydantic AI with graph support
- **Observability** - Pydantic Logfire (OpenTelemetry compatible)

### Infrastructure
- **Orchestration** - Docker Compose (dev), Kubernetes (prod)
- **Data Ingestion** - Async workers with retry logic
- **Schema Management** - JSON Schema + custom validators
- **Frontend** - Next.js 14+ (App Router), React, TypeScript, shadcn/ui
- **Graph Viz** - Neo4j Bloom or react-force-graph
- **Package Manager** - uv for Python, npm for Node.js

## Risks & Mitigations

| Risk                                  | Impact   | Likelihood | Mitigation                                             |
| ------------------------------------- | -------- | ---------- | ------------------------------------------------------ |
| Schema complexity overwhelms users    | High     | Medium     | Provide templates, auto-inference tools, wizard UI     |
| Graph queries too slow at scale       | High     | Medium     | Query optimization, caching, incremental indexing      |
| LLM extraction errors degrade quality | High     | High       | Validation layers, human-in-the-loop review, evals     |
| Neo4j licensing costs                 | Medium   | Low        | Use Community Edition, plan for Aura if needed         |
| Multi-tenancy isolation failures      | Critical | Low        | Separate databases per project, strict access controls |
| Agent hallucinations                  | High     | Medium     | Structured outputs, validation, source citations       |

## Timeline & Phases

### Phase 0: Research & Design (1 week)
- Finalize technology stack
- Design schema definition format
- Prototype graph extraction pipeline
- Validate Neo4j vector capabilities

### Phase 1: Foundation MVP (3-4 weeks)
- Core ingestion pipeline (DXR → Graph)
- Schema-driven entity extraction
- Basic Cypher query generation
- Simple chat interface
- Local development environment

### Phase 2: Agent Intelligence (2-3 weeks)
- Multi-agent architecture with Pydantic AI
- Query decomposition and planning
- Hybrid graph-vector search
- Logfire observability integration

### Phase 3: Visualization & UX (2 weeks)
- Graph browser integration
- Interactive schema designer
- Project configuration UI
- Query history and favorites

### Phase 4: Production Readiness (2 weeks)
- Multi-tenancy support
- Security hardening
- Performance optimization
- Deployment automation
- Documentation

**Total: ~10-12 weeks to production-ready v1**

## Open Questions

1. **Schema format** - JSON Schema? Custom DSL? GraphQL SDL?
2. **Graph browser** - Build custom or integrate Neo4j Bloom?
3. **Relationship extraction** - LLM-based or rule-based or hybrid?
4. **Project isolation** - Separate Neo4j databases or graph namespaces?
5. **Schema evolution** - How to handle breaking changes?
6. **Prompt management** - Version control? UI editor? External CMS?
7. **Access control** - Replicate DXR permissions or independent RBAC?

## Next Steps

1. Create **ARCHITECTURE.md** - Technical design and component details
2. Create **IMPLEMENTATION.md** - Phased task breakdown and dependencies
3. Set up project repository structure
4. Prototype schema extraction with Pydantic AI
5. Validate Neo4j vector search performance
6. Build hello-world Graph RAG example

---

**Document Owner**: AI Development Team  
**Last Updated**: November 24, 2025  
**Status**: Draft for Review
