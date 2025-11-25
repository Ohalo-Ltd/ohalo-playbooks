#!/bin/bash

# Graph RAG - Stop Script
# Stops all running services

echo "🛑 Stopping Graph RAG services..."

# Kill backend (port 8000)
if lsof -ti:8000 > /dev/null 2>&1; then
    echo "  └─ Stopping backend..."
    lsof -ti:8000 | xargs kill -9 2>/dev/null
    echo "     ✓ Backend stopped"
else
    echo "  └─ Backend not running"
fi

# Kill frontend (port 3000)
if lsof -ti:3000 > /dev/null 2>&1; then
    echo "  └─ Stopping frontend..."
    lsof -ti:3000 | xargs kill -9 2>/dev/null
    echo "     ✓ Frontend stopped"
else
    echo "  └─ Frontend not running"
fi

# Stop Docker services
echo "  └─ Stopping Docker services..."
docker compose down
echo "     ✓ Docker services stopped"

echo ""
echo "✅ All services stopped"
