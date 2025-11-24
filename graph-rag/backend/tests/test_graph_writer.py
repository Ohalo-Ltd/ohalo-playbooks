import pytest
from unittest.mock import AsyncMock
from ingestion.graph_writer import GraphWriter
from ingestion.metadata_parser import ExtractedEntity

@pytest.fixture
def mock_neo4j_client():
    client = AsyncMock()
    client.execute_write = AsyncMock(return_value=[{"node_id": "node-123"}])
    return client

@pytest.mark.asyncio
async def test_link_entity_to_document(mock_neo4j_client):
    writer = GraphWriter(mock_neo4j_client)
    entity_id = "entity-1"
    document_id = "doc-1"
    
    await writer.link_entity_to_document(entity_id, document_id)
    
    mock_neo4j_client.execute_write.assert_called_once()
    call_args = mock_neo4j_client.execute_write.call_args
    query = call_args[0][0]
    params = call_args[0][1]
    
    assert "MERGE (e)-[:MENTIONED_IN]->(d)" in query
    assert "SET e.source_file_ids" in query
    assert params["entity_id"] == entity_id
    assert params["document_id"] == document_id

@pytest.mark.asyncio
async def test_merge_entity(mock_neo4j_client):
    writer = GraphWriter(mock_neo4j_client)
    entity = ExtractedEntity(
        id="e1",
        type="Person",
        properties={"name": "John Doe"}
    )
    
    node_id = await writer.merge_entity(entity)
    
    assert node_id == "node-123"
    mock_neo4j_client.execute_write.assert_called_once()
