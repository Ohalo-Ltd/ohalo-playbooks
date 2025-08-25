-- Initialize RAG database with pgvector extension
CREATE EXTENSION IF NOT EXISTS vector;

-- Create tables (these will also be created by the Python app, but good to have here)
CREATE TABLE IF NOT EXISTS documents (
    id SERIAL PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    content TEXT NOT NULL,
    category VARCHAR(100) DEFAULT NULL, -- 'HR Policy', 'Performance Review' etc.
    entitled_emails TEXT[] DEFAULT ARRAY[]::TEXT[], -- emails entitled to access
    extracted_employee_id VARCHAR(200) DEFAULT NULL, -- from extracted_metadata#DXR_EMPLOYEE_ID_EXTRACTOR_ID
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
