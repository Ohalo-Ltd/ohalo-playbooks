# RAG App Backend

A secure RAG (Retrieval-Augmented Generation) system with Data X-Ray integration, now reorganized for better maintainability.

## 🏗️ New Architecture

The backend has been **reorganized into logical modules**:

- **`core/`** - Shared configuration and utilities
- **`database/`** - Database management and initialization  
- **`ingestion/`** - Data ingestion from DXR (superuser access)
- **`api/`** - FastAPI endpoints (non-privileged access)
- **`auth/`** - Authentication and authorization

📖 **See [ARCHITECTURE.md](ARCHITECTURE.md) for detailed documentation of the new structure.**

## Features

- 🔄 **Automatic Document Ingestion**: Fetches documents from DXR datasources
- 🧩 **Smart Chunking**: Intelligently splits documents into overlapping chunks
- 🔍 **Vector Search**: Uses OpenAI embeddings for semantic search
- � **Row Level Security**: Department-based access control with RLS
- ⚡ **Secure API**: Non-privileged database access for production safety

## Quick Start

### 1. Environment Setup

Copy the environment template and fill in your credentials:

```bash
cp .env.example .env
```

Edit `.env` with your actual values:
- `DXR_API_KEY`: Your Data X-Ray API key
- `DXR_DATASOURCE_ID`: The datasource ID to fetch documents from
- `OPENAI_API_KEY`: Your OpenAI API key for embeddings

### 2. Install Dependencies

Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Database Setup

Make sure PostgreSQL is running with the pgvector extension:

```bash
# Create database
createdb ragapp

# Connect and enable pgvector (this is also done automatically)
psql ragapp -c "CREATE EXTENSION IF NOT EXISTS vector;"
```

**Initialize the database schema and users:**
```bash
python init_database.py
```

### 4. Ingest Documents

**Load documents from DXR:**
```bash
python ingest_documents.py
```

This will:
- Connect to your DXR datasource using superuser privileges
- Fetch and process all documents  
- Create embeddings and store them in the database
- Set up proper access controls via Row Level Security

### 5. Start the Complete RAG App

**To start both backend and frontend together (Recommended):**
```bash
# From the rag-app root directory (not backend/)
cd /Users/darko/projects/ohalo-playbooks/rag-app
./start.sh
```

This will start:
- PostgreSQL database (via Docker)
- Backend API server on `http://localhost:8000`
- Next.js frontend on `http://localhost:3000`
- Automatic database initialization and document ingestion

**To start only the backend API server:**
```bash
# From the backend directory
cd /Users/darko/projects/ohalo-playbooks/rag-app/backend
./venv/bin/python -m api.main
```

The complete app will be available at:
- **Frontend**: http://localhost:3000 (main app interface)
- **Backend API**: http://localhost:8000 (REST API)
- **API Documentation**: http://localhost:8000/docs (Swagger UI)
source venv/bin/activate

# Then start the server
python api/main.py
```

The API will be available at `http://localhost:8000`

## API Endpoints

- `GET /` - API status
- `GET /health` - Health check
- `POST /initialize` - Initialize documents (streaming response)
- `POST /chat` - Chat endpoint (coming in Phase 2)

## Document Ingestion Process

1. **Fetch from DXR**: Retrieves all documents from the specified datasource
2. **Content Validation**: Ensures documents have `dxr#raw_text` metadata
3. **Chunking**: Splits documents into overlapping chunks (1000 chars with 200 char overlap)
4. **Vectorization**: Generates embeddings using OpenAI's `text-embedding-3-small`
5. **Storage**: Saves documents and chunks to PostgreSQL with pgvector

## Error Handling

- **Missing `dxr#raw_text`**: If documents don't have raw text, you need to rescan your DXR datasource with "Store file content on Data X-Ray" enabled
- **API Key Issues**: Check that all environment variables are set correctly
- **Database Errors**: Ensure PostgreSQL is running and pgvector is installed

## Configuration

All configuration is managed through environment variables. See `.env.example` for the complete list.

Key settings:
- `DXR_DATASOURCE_ID`: Must be set to fetch documents from a specific datasource
- `OPENAI_API_KEY`: Required for generating embeddings
- `DXR_API_KEY`: Required for accessing DXR API

## Progress Tracking

The ingestion process provides detailed progress information:
- Total documents and processing status
- Chunk creation progress
- Processing rate and ETA estimates
- Error reporting
- Real-time updates via terminal or streaming API

## Development

### Project Structure
```
backend/
├── api/                 # FastAPI application
├── config.py           # Configuration management
├── database.py         # Database manager with connection pooling and RLS
├── documents.py        # Document processing and CLI commands
└── requirements.txt    # Python dependencies
```

### Adding New Features

The document service is modular and can be extended:
- Custom chunking strategies
- Different embedding models
- Additional metadata extraction
- Custom progress callbacks
