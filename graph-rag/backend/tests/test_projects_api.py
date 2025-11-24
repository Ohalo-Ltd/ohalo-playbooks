import pytest
from unittest.mock import AsyncMock
from fastapi.testclient import TestClient
from datetime import datetime
from api.app import app
from api.projects import get_postgres_client

@pytest.fixture
def mock_pg_client():
    client = AsyncMock()
    client.connect = AsyncMock()
    client.close = AsyncMock()
    return client

@pytest.fixture
def client(mock_pg_client):
    async def override_get_pg_client():
        yield mock_pg_client
    
    app.dependency_overrides[get_postgres_client] = override_get_pg_client
    return TestClient(app)

def test_list_projects(client, mock_pg_client):
    mock_row = {
        "id": "123e4567-e89b-12d3-a456-426614174000",
        "name": "Test Project",
        "description": "Desc",
        "system_prompt": "Prompt",
        "created_at": datetime.now(),
        "updated_at": datetime.now()
    }
    mock_pg_client.fetch = AsyncMock(return_value=[mock_row])
    
    response = client.get("/api/projects")
    
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["name"] == "Test Project"

def test_create_project(client, mock_pg_client):
    mock_row = {
        "id": "123e4567-e89b-12d3-a456-426614174000",
        "name": "New Project",
        "description": "Desc",
        "system_prompt": "Prompt",
        "created_at": datetime.now(),
        "updated_at": datetime.now()
    }
    mock_pg_client.fetchrow = AsyncMock(return_value=mock_row)
    
    response = client.post("/api/projects", json={"name": "New Project"})
    
    assert response.status_code == 201
    data = response.json()
    assert data["name"] == "New Project"

def test_get_project_not_found(client, mock_pg_client):
    mock_pg_client.fetchrow = AsyncMock(return_value=None)
    response = client.get("/api/projects/123e4567-e89b-12d3-a456-426614174000")
    assert response.status_code == 404

def test_update_project(client, mock_pg_client):
    mock_pg_client.fetchrow = AsyncMock(side_effect=[
        {"id": "123e4567-e89b-12d3-a456-426614174000"}, # Check exists
        { # Return updated
            "id": "123e4567-e89b-12d3-a456-426614174000",
            "name": "Updated Project",
            "description": "Desc",
            "system_prompt": "Prompt",
            "created_at": datetime.now(),
            "updated_at": datetime.now()
        }
    ])
    
    response = client.patch("/api/projects/123e4567-e89b-12d3-a456-426614174000", json={"name": "Updated Project"})
    
    assert response.status_code == 200
    data = response.json()
    assert data["name"] == "Updated Project"

def test_delete_project(client, mock_pg_client):
    mock_pg_client.execute = AsyncMock(return_value="DELETE 1")
    
    response = client.delete("/api/projects/123e4567-e89b-12d3-a456-426614174000")
    
    assert response.status_code == 204

def test_delete_project_not_found(client, mock_pg_client):
    mock_pg_client.execute = AsyncMock(return_value="DELETE 0")
    
    response = client.delete("/api/projects/123e4567-e89b-12d3-a456-426614174000")
    
    assert response.status_code == 404
