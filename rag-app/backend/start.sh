#!/bin/bash

# RAG App Startup Script - Updated for new architecture
set -e

echo "🚀 Starting RAG App Backend (New Architecture)"
echo "=============================================="
echo "ℹ️  Note: This starts only the backend API server."
echo "   To start the complete app (backend + frontend), run '../start.sh' from the rag-app root directory."
echo ""

# Cleanup function
cleanup() {
    echo ""
    echo "🛑 Shutting down RAG App..."
    echo "🐘 Stopping PostgreSQL database..."
    docker compose down
    echo "✅ RAG App stopped"
    exit 0
}

# Set trap to cleanup on interrupts (Ctrl+C, etc.)
trap cleanup SIGINT SIGTERM

BACKEND_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$BACKEND_DIR"

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker first."
    exit 1
fi

# Start PostgreSQL with pgvector
echo "🐘 Starting PostgreSQL database..."
docker compose up -d postgres

# Wait for database to be ready
echo "⏳ Waiting for database to be ready..."
for i in {1..30}; do
    if docker compose exec -T postgres pg_isready -U postgres -d ragapp > /dev/null 2>&1; then
        echo "✅ Database is ready"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "❌ Database failed to start within 30 seconds"
        exit 1
    fi
    sleep 1
done

# Setup virtual environment
if [ ! -d "venv" ]; then
    echo "📦 Creating virtual environment..."
    uv venv .venv
    source .venv/bin/activate
    uv sync
else
    echo "✅ Activating virtual environment..."
    source .venv/bin/activate
    # Quick dependency check
    uv run python -c "import tiktoken, openai, dxrpy, psycopg2" 2>/dev/null || uv sync
fi

# Load environment
if [ -f ".env" ]; then
    export $(cat .env | grep -v '^#' | xargs)
else
    echo "⚠️  No .env file found. Please copy .env.example to .env and configure it."
fi

RESET_DB=false
for arg in "$@"; do
  if [ "$arg" == "--reset-db" ]; then
    RESET_DB=true
  fi
  shift
  set -- "$@"
done

# If reset flag is set, drop the database volume and restart database
if [ "$RESET_DB" = true ]; then
  echo "🗑️  Dropping PostgreSQL Docker volume and resetting database..."
  docker compose down -v
  docker compose up -d postgres
  # Wait for database to be ready again
  echo "⏳ Waiting for database to be ready after reset..."
  for i in {1..30}; do
      if docker compose exec -T postgres pg_isready -U postgres -d ragapp > /dev/null 2>&1; then
          echo "✅ Database is ready"
          break
      fi
      if [ $i -eq 30 ]; then
          echo "❌ Database failed to start within 30 seconds after reset"
          exit 1
      fi
      sleep 1
  done
fi

# Initialize database using new architecture
echo "🔍 Checking database setup..."
uv run init_database.py || {
    echo "❌ Database initialization failed"
    exit 1
}

# Check if documents exist, if not prompt user
echo "📊 Checking document status..."
DOC_COUNT=$(docker compose exec -T postgres psql -U postgres -d ragapp -c "SELECT COUNT(*) FROM documents;" -t 2>/dev/null | tr -d ' ' | head -1 || echo "0")

if [ "$DOC_COUNT" = "0" ]; then
    echo "📥 No documents found. Ingesting documents automatically..."
    uv run ingest_documents.py || {
        echo "⚠️  Document ingestion failed, but continuing to start API..."
    }
fi

# Start the server
echo "🌟 Starting FastAPI server with new architecture..."
echo "📍 API will be available at: http://localhost:8000"
echo "🔒 Using Row Level Security for document access"
echo "🛑 Press Ctrl+C to stop everything"
echo ""
uv run -m api.main
