#!/bin/bash

set -e

echo "🚀 Setting up Graph RAG Development Environment..."

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker and try again."
    exit 1
fi

# Check if uv is installed
if ! command -v uv &> /dev/null; then
    echo "📦 Installing uv..."
    curl -LsSf https://astral.sh/uv/install.sh | sh
    export PATH="$HOME/.cargo/bin:$PATH"
fi

# Create .env file if it doesn't exist
if [ ! -f .env ]; then
    echo "📝 Creating .env file from .env.example..."
    cp .env.example .env
    echo "⚠️  Please edit .env and add your API keys (OPENAI_API_KEY, DXR_API_KEY)"
fi

# Install backend dependencies
echo "📦 Installing backend dependencies..."
cd backend
uv sync
cd ..

# Install frontend dependencies
echo "📦 Installing frontend dependencies..."
cd frontend
npm install
cd ..

# Start Docker services
echo "🐳 Starting Docker services..."
docker-compose up -d postgres neo4j

# Wait for services to be healthy
echo "⏳ Waiting for services to be ready..."
timeout 60 bash -c 'until docker-compose ps | grep -q "healthy.*postgres"; do sleep 2; done' || {
    echo "❌ Postgres failed to start"
    exit 1
}
timeout 60 bash -c 'until docker-compose ps | grep -q "healthy.*neo4j"; do sleep 2; done' || {
    echo "❌ Neo4j failed to start"
    exit 1
}

# Initialize database
echo "🗄️  Initializing database..."
docker exec graphrag-postgres psql -U graphrag -d graphrag -f /docker-entrypoint-initdb.d/init.sql || true

echo "✅ Development environment is ready!"
echo ""
echo "📚 Next steps:"
echo "  1. Edit .env and add your API keys"
echo "  2. Start backend: cd backend && uv run uvicorn api.app:app --reload"
echo "  3. Start frontend: cd frontend && npm run dev"
echo ""
echo "🌐 Services:"
echo "  - Backend API: http://localhost:8000"
echo "  - Frontend: http://localhost:3000"
echo "  - Neo4j Browser: http://localhost:7474 (neo4j/graphrag123)"
echo "  - PostgreSQL: localhost:5432 (graphrag/graphrag)"
