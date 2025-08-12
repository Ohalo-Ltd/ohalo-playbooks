#!/bin/bash

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

# Global variables for PIDs
BACKEND_PID=""
FRONTEND_PID=""

# Cleanup function
cleanup() {
    echo -e "\n${YELLOW}Shutting down services...${NC}"
    
    if [ ! -z "$FRONTEND_PID" ]; then
        echo -e "${YELLOW}Stopping frontend...${NC}"
        kill $FRONTEND_PID 2>/dev/null
        wait $FRONTEND_PID 2>/dev/null
    fi
    
    if [ ! -z "$BACKEND_PID" ]; then
        echo -e "${YELLOW}Stopping backend...${NC}"
        kill $BACKEND_PID 2>/dev/null
        wait $BACKEND_PID 2>/dev/null
    fi
    
    # Force kill any remaining processes on our ports
    lsof -ti :8000 | xargs kill -9 2>/dev/null
    lsof -ti :3000 | xargs kill -9 2>/dev/null
    
    echo -e "${GREEN}All services stopped${NC}"
    exit 0
}

# Trap exit signals
trap cleanup SIGINT SIGTERM EXIT

echo -e "${BLUE}🚀 Starting RAG App Services...${NC}"

# Check if we're in the right directory
if [ ! -d "backend" ] || [ ! -d "frontend" ]; then
    echo -e "${RED}Error: Please run from the RAG app root directory${NC}"
    exit 1
fi

# Create environment files if missing
if [ ! -f "backend/.env" ] && [ -f "backend/.env.example" ]; then
    echo -e "${YELLOW}Creating backend/.env from example...${NC}"
    cp backend/.env.example backend/.env
fi

if [ ! -f "frontend/.env.local" ]; then
    echo -e "${YELLOW}Creating frontend/.env.local...${NC}"
    cat > frontend/.env.local << EOF
NEXT_PUBLIC_BACKEND_URL=http://localhost:8000
NEXT_PUBLIC_APP_NAME=RAG App Demo
NEXT_PUBLIC_ENVIRONMENT=development
EOF
fi

# Parse arguments
RESET_DB=false
for arg in "$@"; do
  if [ "$arg" == "--reset-db" ]; then
    RESET_DB=true
  fi
  # Remove the flag from positional parameters
  shift
  set -- "$@"
done

# Start backend
if [ "$RESET_DB" = true ]; then
  echo -e "${YELLOW}Resetting database and starting backend server...${NC}"
  cd backend
  ./start.sh --reset-db &
  BACKEND_PID=$!
  cd ..
else
  echo -e "${GREEN}Starting backend server...${NC}"
  cd backend
  ./start.sh &
  BACKEND_PID=$!
  cd ..
fi

# Wait for backend to be ready
echo -e "${YELLOW}Waiting for backend...${NC}"
for i in {1..90}; do
    if curl -s http://localhost:8000/health >/dev/null 2>&1; then
        break
    fi
    if [ $i -eq 90 ]; then
        echo -e "${RED}Backend failed to start${NC}"
        exit 1
    fi
    sleep 1
done

# Start frontend
echo -e "${GREEN}Starting frontend server...${NC}"
cd frontend
npm ci
npm run dev &
FRONTEND_PID=$!
cd ..

# Wait for frontend to be ready
echo -e "${YELLOW}Waiting for frontend...${NC}"
for i in {1..30}; do
    if curl -s http://localhost:3000 >/dev/null 2>&1; then
        break
    fi
    if [ $i -eq 30 ]; then
        echo -e "${RED}Frontend failed to start${NC}"
        exit 1
    fi
    sleep 1
done

# Success message
echo -e "\n${GREEN}✅ RAG App Started Successfully!${NC}"
echo -e "${BLUE}Backend API:${NC}    http://localhost:8000"
echo -e "${BLUE}Frontend App:${NC}   http://localhost:3000"
echo -e "${BLUE}API Docs:${NC}       http://localhost:8000/docs"
echo -e "\n${YELLOW}Press Ctrl+C to stop all services${NC}\n"

# Keep running and wait for exit signal
wait
