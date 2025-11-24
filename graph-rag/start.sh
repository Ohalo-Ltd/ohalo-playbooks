#!/bin/bash

# Graph RAG - Quick Start Script
# Starts Docker services, backend, and frontend

set -e

echo "🚀 Starting Graph RAG..."

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

# Wait for Ctrl+C
trap "echo '🛑 Stopping services...'; docker compose down; kill $BACKEND_PID $FRONTEND_PID; exit" INT
wait
