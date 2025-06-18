#!/bin/bash
set -e

echo "🚀 Starting DataXray Email Security Demo"
echo "========================================"

# Check if .env file exists
if [ ! -f .env ]; then
    echo "❌ Error: .env file not found"
    echo "Please copy .env.example to .env and configure your DataXray credentials"
    exit 1
fi

# Load environment variables
source .env

# Validate required environment variables
if [ -z "$DATAXRAY_API_KEY" ] || [ "$DATAXRAY_API_KEY" = "your_api_key_here" ]; then
    echo "❌ Error: DATAXRAY_API_KEY not configured"
    echo "Please update .env with your actual DataXray API key"
    exit 1
fi

if [ -z "$DATAXRAY_BASE_URL" ]; then
    echo "❌ Error: DATAXRAY_BASE_URL not configured"
    exit 1
fi

echo "✅ Environment configuration validated"

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Error: Docker is not running"
    echo "Please start Docker Desktop and try again"
    exit 1
fi

echo "✅ Docker is running"

# Build and start services
echo "🏗️  Building and starting services..."
docker compose up --build -d

# Wait for services to be ready
echo "⏳ Waiting for services to start..."
sleep 10

# Check if services are running
if docker compose ps | grep -q "Up"; then
    echo "✅ Services started successfully!"
    echo ""
    echo "🌐 MailHog Web UI: http://localhost:8025"
    echo "📧 SMTP Proxy: localhost:1025"
    echo ""
    echo "📋 Next steps:"
    echo "1. Configure Thunderbird (see THUNDERBIRD_SETUP.md)"
    echo "2. Send test emails to the SMTP Proxy"
    echo ""
    echo "🛑 To stop: docker compose down"
    echo "📊 View logs: docker compose logs -f"
else
    echo "❌ Error: Some services failed to start"
    echo "Check logs with: docker compose logs"
    exit 1
fi
