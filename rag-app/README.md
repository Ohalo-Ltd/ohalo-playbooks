# RAG App Demo

A simple demonstration of Retrieval-Augmented Generation (RAG) with Data X-Ray integration and row-level security.

To get started with the demo:

```bash
./start.sh
```

## Purpose

This is a **demo application** designed to showcase:
- Secure document retrieval based on user permissions
- Simple RAG implementation with OpenAI
- Data X-Ray integration for document metadata

This will set up the environment with a FastAPI backend, Postgres database with PGVector and RLS policies, and a Next.js frontend for querying documents.

## Overview

Retrieval-Augmented Generation (RAG) applications are transforming how enterprises access and utilize their data. However, without robust security measures, these applications risk exposing sensitive information. By integrating Data X-Ray and employing row-level security (RLS), you can ensure governed access to company data while extracting key metadata to enforce strong security policies.

This demo showcases a secure RAG application for HR, where employees can access HR policies, their own performance reviews, and documents they are entitled to—while preventing unauthorized access to sensitive information. The application uses a mock identity provider (IdP) to simulate user groups and permissions, and Postgres with PGVector extension for embedding storage and RLS enforcement.

## Architecture

```
rag-app/
├── backend/                # Python FastAPI backend
│   ├── api/               # REST API endpoints (auth, chat, docs)
│   ├── auth/              # Mock IdP for user/group simulation
│   ├── core/              # Config and utilities
│   ├── database/          # RLS-aware DB manager, schema, migrations
│   ├── ingestion/         # DXR ingestion pipeline (DXR → Postgres)
│   ├── rag_service.py     # RAG orchestration (OpenAI, DB, agent)
├── frontend/               # Next.js chat interface (user switching)
└── scripts/                # Setup and utility scripts
```

## How It Works

### Data Ingestion

1. Data is ingested through the Data X-Ray API, extracting file content, categories, entitlements, and metadata.
2. The data is processed into embeddings and stored in the Postgres database.

### User Query

1. A user submits a query, which is embedded using OpenAI and sent to the Postgres database.
2. Row-level security policies are applied to the query to determine access.
3. The database retrieves the allowed data based on the policies.
4. The retrieved chunks are synthesized into a response and sent to the user.

### RLS Policy Example

```sql
CREATE POLICY secure_document_access ON documents
FOR SELECT
USING (
    -- Grant access ONLY if one of these specific conditions is met:
    (
        -- 1. User is explicitly entitled via email list
        entitled_emails @> ARRAY[current_setting('app.user_email', true)]::text[]
    ) OR (
        -- 2. Performance Review access: ONLY if it's their own review OR they're HR with permission
        category = 'Performance Review' AND (
            extracted_employee_id = current_setting('app.employee_id', true) OR
            current_setting('app.groups', true) LIKE '%can_see_all_reviews%'
        )
    ) OR (
        -- 3. Public policies: Only for specific public categories
        category IN ('Code of Conduct', 'Leave and Time-Off Policy')
    )
    -- Note: If none of these conditions match, access is DENIED by default
);
```

This policy ensures that employees can only access their own performance reviews, while HR managers can access reviews for all employees.

## Why Postgres with PGVector and RLS?

We chose Postgres with PGVector and Row-Level Security (RLS) because it allows us to enforce security policies directly on the data. This minimizes the risk of the application exposing incorrect or unauthorized data, as the policies are applied at the database level.

While Postgres is our choice for this demo, any vector database could be used for embedding storage. However, in such cases, the security policies would likely need to be defined and enforced in the application layer, which may introduce additional complexity and potential risks.

## Environment Setup

There are two environment files required for this project:

### Backend: `.env` (see `.env.example` for template)

Copy the env variables for backend and fill in the required values:

```sh
cp backend/.env.example backend/.env && cp frontend/.env.local.example frontend/.env.local
```

Configure both with your environment information.

## Demo Users

- `sarah.mitchell@enterprise.com` - Project Manager (can only see her own reviews and entitled docs)
- `charlie@corp.com` - HR Manager (can see all reviews and HR docs)

---

**Note**: This is a demonstration application optimized for clarity and simplicity, not production use.
