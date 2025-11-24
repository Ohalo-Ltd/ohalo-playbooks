import pytest
from unittest.mock import AsyncMock, patch
from fastapi.testclient import TestClient
from api.app import app

@pytest.fixture
def client():
    return TestClient(app)

@patch("api.chat.query")
@patch("api.chat.Neo4jClient")
@patch("api.chat.PostgresClient")
@patch("api.chat.EmbeddingService")
def test_query_endpoint(
    MockEmbeddingService,
    MockPostgresClient,
    MockNeo4jClient,
    mock_query,
    client
):
    # Setup mocks
    mock_neo4j_instance = AsyncMock()
    mock_neo4j_instance.connect = AsyncMock()
    mock_neo4j_instance.close = AsyncMock()
    MockNeo4jClient.return_value = mock_neo4j_instance

    mock_pg_instance = AsyncMock()
    mock_pg_instance.connect = AsyncMock()
    mock_pg_instance.close = AsyncMock()
    mock_pg_instance.fetchrow = AsyncMock(return_value=None) # No custom prompt
    MockPostgresClient.return_value = mock_pg_instance

    mock_query.return_value = "Test answer"

    response = client.post("/api/chat/query", json={
        "question": "Test question",
        "project_id": "default"
    })

    assert response.status_code == 200
    data = response.json()
    assert data["answer"] == "Test answer"

@patch("api.chat.query_with_steps")
@patch("api.chat.Neo4jClient")
@patch("api.chat.PostgresClient")
@patch("api.chat.EmbeddingService")
def test_query_stream_endpoint(
    MockEmbeddingService,
    MockPostgresClient,
    MockNeo4jClient,
    mock_query_with_steps,
    client
):
    # Setup mocks
    mock_neo4j_instance = AsyncMock()
    mock_neo4j_instance.connect = AsyncMock()
    mock_neo4j_instance.close = AsyncMock()
    MockNeo4jClient.return_value = mock_neo4j_instance

    mock_pg_instance = AsyncMock()
    mock_pg_instance.connect = AsyncMock()
    mock_pg_instance.close = AsyncMock()
    mock_pg_instance.fetchrow = AsyncMock(return_value=None)
    MockPostgresClient.return_value = mock_pg_instance

    async def mock_generator(*args, **kwargs):
        yield {"type": "thinking", "content": "Step 1"}
        yield {"type": "answer", "content": "Final Answer"}

    mock_query_with_steps.return_value = mock_generator()

    response = client.post("/api/chat/query/stream", json={
        "question": "Test question",
        "project_id": "default"
    })

    assert response.status_code == 200
    content = response.content.decode()
    assert "Step 1" in content
    assert "Final Answer" in content
    assert "[DONE]" in content
