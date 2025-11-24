import pytest
from unittest.mock import AsyncMock, MagicMock
from ingestion.service import IngestionService
from ingestion.dxr_client import DXRFile
from ingestion.metadata_parser import ParsedMetadata, ExtractedEntity

class AnyString:
    def __eq__(self, other):
        return isinstance(other, str)

@pytest.fixture
def mock_deps():
    return {
        "dxr_client": AsyncMock(),
        "neo4j_client": AsyncMock(),
        "postgres_client": AsyncMock(),
        "embedding_service": AsyncMock(),
        "metadata_parser": MagicMock(),
    }

@pytest.mark.asyncio
async def test_ingest_document(mock_deps):
    service = IngestionService(**mock_deps)
    
    dxr_file = DXRFile(
        id="file-1",
        name="test.pdf",
        path="/path/to/test.pdf",
        size=100,
        mime_type="application/pdf",
        categories=["test"],
        extracted_metadata={"some": "metadata"},
        createdAt="2023-01-01T00:00:00Z",
        updatedAt="2023-01-01T00:00:00Z"
    )
    
    mock_deps["metadata_parser"].parse_from_dxr_file.return_value = ParsedMetadata(
        entities=[ExtractedEntity(id="e1", type="Person", properties={})],
        relationships=[]
    )
    
    mock_deps["dxr_client"].fetch_document_content.return_value = "content"
    mock_deps["embedding_service"].get_embedding.return_value = [0.1, 0.2]
    
    # Mock execute_write to return a list (simulating result)
    mock_deps["neo4j_client"].execute_write.return_value = [{"node_id": "123"}]
    
    await service.ingest_document(dxr_file, project_id="proj-1")
    
    # Verify calls
    assert mock_deps["neo4j_client"].execute_write.called
    assert mock_deps["dxr_client"].fetch_document_content.called

@pytest.mark.asyncio
async def test_ingest_document_no_content(mock_deps):
    service = IngestionService(**mock_deps)
    
    dxr_file = DXRFile(
        id="file-2",
        name="test2.pdf",
        path="/path/to/test2.pdf",
        size=100,
        mime_type="application/pdf",
        categories=["test"],
        extracted_metadata={},
        createdAt="2023-01-01T00:00:00Z",
        updatedAt="2023-01-01T00:00:00Z"
    )
    
    await service.ingest_document(dxr_file, project_id="proj-1", fetch_content=False)
    
    assert mock_deps["neo4j_client"].execute_write.called
    assert not mock_deps["dxr_client"].fetch_document_content.called
    assert not mock_deps["metadata_parser"].parse_from_dxr_file.called

@pytest.mark.asyncio
async def test_ingest_document_with_metadata_only(mock_deps):
    service = IngestionService(**mock_deps)
    
    dxr_file = DXRFile(
        id="file-3",
        name="test3.pdf",
        path="/path/to/test3.pdf",
        size=100,
        mime_type="application/pdf",
        categories=["test"],
        extracted_metadata={"some": "metadata"},
        createdAt="2023-01-01T00:00:00Z",
        updatedAt="2023-01-01T00:00:00Z"
    )
    
    mock_deps["metadata_parser"].parse_from_dxr_file.return_value = ParsedMetadata(
        entities=[ExtractedEntity(id="e1", type="Person", properties={})],
        relationships=[]
    )
    
    await service.ingest_document(dxr_file, project_id="proj-1", fetch_content=False)
    
    assert mock_deps["neo4j_client"].execute_write.called
    assert mock_deps["metadata_parser"].parse_from_dxr_file.called
    assert not mock_deps["dxr_client"].fetch_document_content.called
