#!/bin/bash

# Graph RAG - Quick Start Script
# Starts Docker services, backend, and frontend

set -e

echo "🚀 Starting Graph RAG..."

# Check if ports are already in use
if lsof -ti:8000 > /dev/null 2>&1; then
    echo "⚠️  Port 8000 is already in use. Stopping existing backend..."
    lsof -ti:8000 | xargs kill -9 2>/dev/null || true
    sleep 1
fi

if lsof -ti:3000 > /dev/null 2>&1; then
    echo "⚠️  Port 3000 is already in use. Stopping existing frontend..."
    lsof -ti:3000 | xargs kill -9 2>/dev/null || true
    sleep 1
fi

# Start Docker services
echo "📦 Starting Neo4j and PostgreSQL..."
docker compose up -d postgres neo4j

# Wait for services to be ready
echo "⏳ Waiting for databases to be ready..."
sleep 5

# Start backend
echo "🐍 Starting backend..."
cd backend
uv sync
uv run uvicorn api.app:app --reload --env-file ../.env &
BACKEND_PID=$!
cd ..

# Start frontend
echo "⚛️  Starting frontend..."
cd frontend
npm ci
npm run dev &
FRONTEND_PID=$!
cd ..

echo "✅ All services started!"
echo ""
echo "Backend:  http://localhost:8000"
echo "Frontend: http://localhost:3000"
echo "Neo4j:    http://localhost:7474"
echo ""
echo "Press Ctrl+C to stop all services"
echo "Or run ./stop.sh to stop services"

# Cleanup function
cleanup() {
    echo ""
    echo "🛑 Stopping services..."
    
    # Kill backend
    if kill -0 $BACKEND_PID 2>/dev/null; then
        kill $BACKEND_PID 2>/dev/null || true
    fi
    
    # Kill frontend
    if kill -0 $FRONTEND_PID 2>/dev/null; then
        kill $FRONTEND_PID 2>/dev/null || true
    fi
    
    # Kill any remaining processes on ports
    lsof -ti:8000 | xargs kill -9 2>/dev/null || true
    lsof -ti:3000 | xargs kill -9 2>/dev/null || true
    
    # Stop Docker services
    docker compose down
    
    echo "✅ All services stopped"
    exit 0
}

# Wait for Ctrl+C
trap cleanup INT TERM
wait
