"""Test configuration."""

import os
import pytest
from httpx import AsyncClient, ASGITransport
from uuid import uuid4


@pytest.fixture
def sample_data() -> dict[str, str]:
    """Sample test data."""
    return {"test": "data"}


@pytest.fixture
async def client():
    """Create a test client for the API."""
    # Set required environment variables if not already set
    if "OPENAI_API_KEY" not in os.environ:
        os.environ["OPENAI_API_KEY"] = "sk-test-key"
    if "POSTGRES_HOST" not in os.environ:
        os.environ["POSTGRES_HOST"] = "localhost"
    if "POSTGRES_USER" not in os.environ:
        os.environ["POSTGRES_USER"] = "graphrag"
    if "POSTGRES_PASSWORD" not in os.environ:
        os.environ["POSTGRES_PASSWORD"] = "graphrag"
    if "POSTGRES_DB" not in os.environ:
        os.environ["POSTGRES_DB"] = "graphrag"
    if "NEO4J_URI" not in os.environ:
        os.environ["NEO4J_URI"] = "bolt://localhost:7687"
    if "NEO4J_USER" not in os.environ:
        os.environ["NEO4J_USER"] = "neo4j"
    if "NEO4J_PASSWORD" not in os.environ:
        os.environ["NEO4J_PASSWORD"] = "graphrag123"

    from api.app import app

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as ac:
        yield ac


@pytest.fixture
async def test_project(client):
    """Create a test project for entitlements testing."""
    # Create a project
    response = await client.post(
        "/api/projects",
        json={
            "name": f"Test Project {uuid4()}",
            "description": "Project for testing entitlements",
        },
    )
    assert response.status_code == 201
    project = response.json()

    yield project

    # Cleanup: delete the project (if delete endpoint exists)
    # await client.delete(f"/api/projects/{project['id']}")
