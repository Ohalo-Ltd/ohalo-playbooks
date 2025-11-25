"""Tests for DXR client."""

import pytest
from unittest.mock import AsyncMock, MagicMock
from ingestion.dxr_client import DXRClient, DXRFile


@pytest.fixture
def mock_httpx_client(monkeypatch):
    """Mock httpx.AsyncClient."""
    mock_client = AsyncMock()
    mock_client.aclose = AsyncMock()
    
    async def mock_init(self, **kwargs):
        return mock_client
    
    monkeypatch.setattr("httpx.AsyncClient", lambda **kwargs: mock_client)
    return mock_client


@pytest.mark.asyncio
async def test_fetch_documents_ndjson_format(mock_httpx_client):
    """Test fetching documents when DXR returns NDJSON format."""
    # Simulate NDJSON response (one JSON object per line)
    ndjson_response = """{"fileId":"0KQUMpgBVo-c9i0dUnJ8","fileName":"doc1.pdf","path":"/docs/doc1.pdf","size":1024,"mimeType":"application/pdf","createdAt":"2023-01-01T00:00:00Z","lastModifiedAt":"2023-01-02T00:00:00Z","extractedMetadata":[]}
{"fileId":"1LRVNqhCWp-d0j1eVoK9","fileName":"doc2.pdf","path":"/docs/doc2.pdf","size":2048,"mimeType":"application/pdf","createdAt":"2023-01-01T00:00:00Z","lastModifiedAt":"2023-01-02T00:00:00Z","extractedMetadata":[]}"""
    
    mock_response = MagicMock()
    mock_response.text = ndjson_response
    mock_response.raise_for_status = MagicMock()
    
    mock_httpx_client.request = AsyncMock(return_value=mock_response)
    
    client = DXRClient(api_key="test-key", base_url="https://api.test.com")
    
    documents = await client.fetch_documents(datasource_id="ds_123")
    
    # Should parse NDJSON and return list of documents
    assert len(documents) == 2
    assert documents[0].name == "doc1.pdf"
    assert documents[1].name == "doc2.pdf"


@pytest.mark.asyncio
async def test_fetch_documents_single_object_format(mock_httpx_client):
    """Test fetching documents when DXR returns a single JSON object."""
    # Single object response (as shown in the user's example)
    json_response = {
        "datasource": {"id": "c3d4e5f6-a7b8-4012-8def-345678901234"},
        "fileName": "2011_audited_financial_statement_msword.doc",
        "fileId": "0KQUMpgBVo-c9i0dUnJ8",
        "path": "Documents/Confidential Folder/2011_audited_financial_statement_msword.doc",
        "size": 1048576,
        "mimeType": "application/pdf",
        "createdAt": "2023-01-01T00:00:00Z",
        "lastModifiedAt": "2023-01-02T00:00:00Z",
        "extractedMetadata": []
    }
    
    mock_response = MagicMock()
    mock_response.json = MagicMock(return_value=json_response)
    mock_response.text = ""
    mock_response.raise_for_status = MagicMock()
    
    mock_httpx_client.request = AsyncMock(return_value=mock_response)
    
    client = DXRClient(api_key="test-key", base_url="https://api.test.com")
    
    documents = await client.fetch_documents(datasource_id="ds_123")
    
    # Should handle single object and return as list
    assert len(documents) == 1
    assert documents[0].name == "2011_audited_financial_statement_msword.doc"


@pytest.mark.asyncio  
async def test_parse_dxr_file_with_extracted_metadata(mock_httpx_client):
    """Test parsing DXR file with extracted metadata array."""
    json_obj = {
        "fileId": "0KQUMpgBVo-c9i0dUnJ8",
        "fileName": "contract.pdf",
        "path": "/contracts/contract.pdf",
        "size": 1048576,
        "mimeType": "application/pdf",
        "createdAt": "2023-01-01T00:00:00Z",
        "lastModifiedAt": "2023-01-02T00:00:00Z",
        "extractedMetadata": [
            {
                "id": "b2c3d4e5-f617-4901-bcde-f23456789012",
                "name": "Contract Type",
                "value": "Annual Service Agreement",
                "type": "TEXT"
            }
        ]
    }
    
    # Test that DXRFile can parse this structure
    file = DXRFile(
        id=json_obj["fileId"],
        name=json_obj["fileName"],
        path=json_obj["path"],
        size=json_obj["size"],
        mime_type=json_obj["mimeType"],
        created_at=json_obj["createdAt"],
        updated_at=json_obj["lastModifiedAt"],
        extracted_metadata=json_obj.get("extractedMetadata", [])
    )
    
    assert file.name == "contract.pdf"
    assert len(file.extracted_metadata) == 1
