Here’s a reworked implementation plan that reflects your requested changes. The language is cleaner, the structure is clearer, and the plan now includes the updated frontend, start.sh, backend streaming, and RLS-based access controls.

⸻


# RAG with pgvector and Data X-Ray – Implementation Plan

## Executive Summary

This playbook outlines how to implement a Retrieval-Augmented Generation (RAG) system enhanced with Data X-Ray (DXR) metadata and entitlements. The goal is to deliver a secure, compliant, and role-aware chat experience where users only retrieve data they are authorized to access, enforced via row-level security (RLS).

## Target Users

- **Chief Data Officer (CDO):** Drives secure and compliant data usage.
- **Compliance Officer:** Ensures adherence to data access and auditability standards.

## Business Impact

By integrating DXR metadata and entitlements into a RAG pipeline, this solution:
- Restricts access to documents based on departmental permissions
- Guarantees security and regulatory compliance
- Demonstrates how secure RAG can be applied to real-world enterprise use cases

---

## Architecture Overview

### Folder Structure

project-root/
├── backend/                 # Python RAG backend
│   ├── api/                 # Streaming endpoint
│   ├── rag/                 # Embedding, retrieval, generation
│   └── auth/                # Identity and entitlement checks
├── frontend/                # Minimal Next.js chat UI
│   └── components/          # Tailwind + shadcn UI components
├── scripts/
│   └── start.sh             # Project startup + DXR ingestion
└── .env                     # Shared environment config

### Core Components

1. **Data X-Ray Ingestion Layer** – Pulls and enriches documents with entitlements.
2. **PostgreSQL + pgvector** – Stores document chunks and metadata with RLS.
3. **Pydantic-AI Backend** – Handles embedding search and streaming response generation.
4. **Minimalist Next.js Frontend** – Modern chat interface with user switching.
5. **Row-Level Security (RLS)** – Restricts access based on document category.

---

## Technology Stack

- **Backend:** Python, `pydantic-ai`, `dxrpy`, `pgvector`, FastAPI
- **Frontend:** Next.js, Tailwind CSS, shadcn/ui
- **Database:** PostgreSQL + pgvector, RLS enabled
- **Startup Script:** `start.sh` for full setup and data sync
- **Authentication:** Simple identity switching via dropdown (for demo)

---

## Implementation Plan

### Phase 1: Project Bootstrap (Days 1–2) ✅ **COMPLETE**

**Tasks**
- ✅ Initialize monorepo with `frontend` and `backend`
- ✅ Set up virtual environments and dependency management
- ✅ Create `start.sh` script

**Deliverables**
- ✅ Folder structure
- ✅ Working dev environments
- ✅ Bootstrap script with DXR ingestion logic

**Acceptance**
- ✅ `./scripts/start.sh` sets up everything from scratch
- ✅ DB setup triggers DXR fetch, chunk, and vectorize when empty

---

### Phase 2: Backend Services (Days 2–4) 🔄 **IN PROGRESS**

**Tasks**
- ✅ Implement FastAPI app structure with health endpoints
- ✅ Build vector search infrastructure using OpenAI embeddings and `pgvector`
- 🔄 **IN PROGRESS**: Add `/chat` streaming endpoint with `pydantic-ai`

**Deliverables**
- ✅ Secure search backend with RLS enforcement
- ✅ RLS policies: general users see HR policies only, HR staff can also see performance reviews
- ✅ Department-to-category access mapping with mock IdP
- ✅ Complete vector search and database layer
- 🔄 **NEXT**: Streaming RAG pipeline implementation

**Acceptance**
- ✅ Querying returns only documents the current user is entitled to
- 🔄 **NEXT**: Responses stream token-by-token to the frontend

---

### Phase 3: Frontend Chat App (Days 4–6) 🔄 **READY TO START**

**Tasks**
- 🔄 **NEXT**: Build minimal chat interface (like Next.js AI chatbot)
- 🔄 **NEXT**: Add user dropdown to switch identities
**Deliverables**
- 🔄 **NEXT**: Fully styled, responsive chat UI
- 🔄 **NEXT**: Smooth animations and copy-to-clipboard for responses
- 🔄 **NEXT**: Demo users (e.g. alice@corp.com, hr-bob@corp.com)

**Acceptance**
- 🔄 **NEXT**: Switching users affects what documents are returned
- 🔄 **NEXT**: Chat UI is clean, usable, and production-ready in style

---

### Phase 4: Data X-Ray Integration (Days 6–8) ✅ **COMPLETE**

**Tasks**
- ✅ Use `dxrpy` to fetch documents and metadata
- ✅ Parse entitlements and categorize documents (e.g., "HR Policies", "Performance Reviews")
- ✅ Chunk and vectorize content

**Deliverables**
- ✅ Document ingestion service
- ✅ Metadata tagging + entitlement mapping
- ✅ Integration into `start.sh` for cold-start DB population

**Acceptance**
- ✅ Documents are loaded with category metadata

---

### Phase 5: End-to-End Security (Days 8–10) ✅ **COMPLETE**

**Tasks**
- ✅ Audit logs for all document accesses (via RLS policies)
- ✅ Ensure chat respects access boundaries throughout

**Deliverables**
- ✅ Middleware enforcing security context
- ✅ Row-level security implementation
- ✅ User context and group-based access control

**Acceptance**
- ✅ Unauthorized access is blocked
- ✅ All accesses are traceable by user, document, and timestamp

---

### Phase 6: Testing & Scenarios (Days 10–12)

**Tasks**
- Load test with synthetic HR data
- Verify access edge cases (e.g. marketing user, HR user, guest)
- Performance testing

**Deliverables**
- Synthetic documents with mixed entitlements
- Automated test suite
- RLS test coverage

**Acceptance**
- Only correct users can retrieve protected content
- Search results are performant (<500ms)
- No RLS bypasses

---

### Phase 7: Docs & Demo (Days 12–14)

**Tasks**
- Write clear how-to guides for CDOs and developers
- Document entitlements + security mapping
- Script out demo interactions

**Deliverables**
- Technical deployment doc
- End-user demo instructions
- Sample audit logs

**Acceptance**
- Demo shows value of DXR metadata for secure retrieval
- Users can explore the entitlement-aware system with confidence

