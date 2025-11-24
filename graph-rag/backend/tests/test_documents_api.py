import pytest
from unittest.mock import AsyncMock
from fastapi.testclient import TestClient
from api.app import app
from api.documents import get_neo4j_client

@pytest.fixture
def mock_neo4j_client():
    client = AsyncMock()
    client.connect = AsyncMock()
    client.close = AsyncMock()
    return client

@pytest.fixture
def client(mock_neo4j_client):
    async def override_get_neo4j_client():
        yield mock_neo4j_client
    
    app.dependency_overrides[get_neo4j_client] = override_get_neo4j_client
    return TestClient(app)

def test_get_document_success(client, mock_neo4j_client):
    document_id = "doc-1"
    mock_node = {"name": "Test Doc", "size": 100}
    mock_neo4j_client.execute_query = AsyncMock(return_value=[{"d": mock_node}])
    
    response = client.get(f"/api/documents/{document_id}")
    
    assert response.status_code == 200
    data = response.json()
    assert data["id"] == document_id
    assert data["name"] == "Test Doc"
    assert data["properties"]["size"] == 100

def test_get_document_not_found(client, mock_neo4j_client):
    document_id = "doc-missing"
    mock_neo4j_client.execute_query = AsyncMock(return_value=[])
    
    response = client.get(f"/api/documents/{document_id}")
    
    assert response.status_code == 404
