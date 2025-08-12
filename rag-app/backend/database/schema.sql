-- Initialize RAG database with pgvector extension
CREATE EXTENSION IF NOT EXISTS vector;

-- Create tables (these will also be created by the Python app, but good to have here)
CREATE TABLE IF NOT EXISTS documents (
    id SERIAL PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    content TEXT NOT NULL,
    category VARCHAR(100) DEFAULT NULL, -- 'HR Policy', 'Performance Review' etc.
    entitled_emails TEXT[] DEFAULT ARRAY[]::TEXT[], -- emails entitled to access
    extracted_employee_id VARCHAR(200) DEFAULT NULL, -- from extracted_metadata#DXR_EXTRACTOR_ID
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS document_chunks (
    id SERIAL PRIMARY KEY,
    document_id INTEGER REFERENCES documents(id) ON DELETE CASCADE,
    content TEXT NOT NULL,
    embedding vector(1536), -- OpenAI embeddings are 1536 dimensions
    chunk_index INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index for vector similarity search
CREATE INDEX IF NOT EXISTS document_chunks_embedding_idx ON document_chunks 
USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);

-- Enable Row Level Security (RLS) on documents table
ALTER TABLE documents ENABLE ROW LEVEL SECURITY;

-- Enable RLS on document_chunks and join with documents for policy enforcement
ALTER TABLE document_chunks ENABLE ROW LEVEL SECURITY;

-- Always drop only the policies we actually create below
DROP POLICY IF EXISTS secure_document_access ON documents;
DROP POLICY IF EXISTS secure_chunk_access ON document_chunks;

-- SECURE RLS DESIGN: Single comprehensive policy with explicit access control
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

-- Document chunks: Inherit access control from parent document
CREATE POLICY secure_chunk_access ON document_chunks
FOR SELECT
USING (
    -- Access chunks ONLY if the user can access the parent document
    EXISTS (
        SELECT 1 
        FROM documents d 
        WHERE d.id = document_chunks.document_id
        -- Use the same logic as the main document policy
        AND (
            (
                d.entitled_emails @> ARRAY[current_setting('app.user_email', true)]::text[]
            ) OR (
                d.category = 'Performance Review' AND (
                    d.extracted_employee_id = current_setting('app.employee_id', true) OR
                    current_setting('app.groups', true) LIKE '%can_see_all_reviews%'
                )
            ) OR (
                d.category IN ('Code of Conduct', 'Leave and Time-Off Policy')
            )
        )
    )
);

-- Re-apply application user grants after any schema recreation
-- Note: ragapp_user only needs SELECT permissions for RLS-controlled access
DO $$
BEGIN
    IF EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'ragapp_user') THEN
        GRANT USAGE ON SCHEMA public TO ragapp_user;
        GRANT SELECT ON documents TO ragapp_user;
        GRANT SELECT ON document_chunks TO ragapp_user;
        -- No INSERT/UPDATE/DELETE permissions - data loading uses superuser
    END IF;
END$$;
