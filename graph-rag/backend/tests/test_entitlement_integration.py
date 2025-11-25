"""Integration tests for entitlement ingestion flow."""

import pytest
from unittest.mock import Mock, AsyncMock

from dxrpy.index.search_results import Hit
from ingestion.service import IngestionService
from ingestion.metadata_parser import DXRMetadataParser
from ingestion.embedder import EmbeddingService
from database.neo4j_client import Neo4jClient
from database.postgres_client import PostgresClient


class TestEntitlementIntegrationFlow:
    """Integration tests for entitlement metadata flow."""

    @pytest.mark.asyncio
    async def test_full_ingestion_with_entitlements(self):
        """Test complete ingestion flow with entitlement metadata."""
        # Setup mocks
        dxr_index = Mock()
        neo4j_client = Mock(spec=Neo4jClient)
        neo4j_client.execute_write = AsyncMock(return_value=[{"node_id": "neo4j_doc_id"}])
        
        postgres_client = Mock(spec=PostgresClient)
        embedding_service = Mock(spec=EmbeddingService)
        metadata_parser = Mock(spec=DXRMetadataParser)

        service = IngestionService(
            dxr_index=dxr_index,
            neo4j_client=neo4j_client,
            postgres_client=postgres_client,
            embedding_service=embedding_service,
            metadata_parser=metadata_parser,
        )

        # Create realistic hit with entitlements
        hit = Mock(spec=Hit)
        hit.id = "contract-001"
        hit.file_name = "employment_contract.pdf"
        hit._source = {
            "OWNER": "hr@company.com",
            "WHO_CAN_ACCESS": [
                "hr@company.com",
                "legal@company.com",
                "employee-123@company.com",
            ],
        }
        hit.metadata = {
            "ds#object_id": "/hr/contracts/employment_contract.pdf",
            "ds#size": 245000,
            "ds#mime_type": "application/pdf",
            "metadata#binary_hash": "sha256:abc123",
            "extractedMetadata": [],
            "dxr#raw_text": "",
        }

        # Ingest the document
        await service.ingest_document(
            hit=hit,
            project_id="hr-project",
            fetch_content=False,
        )

        # Verify Neo4j was called correctly
        neo4j_client.execute_write.assert_called()
        
        # Get the actual call arguments
        calls = neo4j_client.execute_write.call_args_list
        doc_creation_call = calls[0]  # First call should be document creation
        
        # Verify the query
        query = doc_creation_call[0][0]
        assert "MERGE (d:Document {id: $id})" in query
        
        # Verify the parameters
        params = doc_creation_call[0][1]
        assert params["id"] == "contract-001"
        
        properties = params["properties"]
        assert properties["name"] == "employment_contract.pdf"
        assert properties["project_id"] == "hr-project"
        assert properties["owner_email"] == "hr@company.com"
        assert properties["accessible_by_emails"] == [
            "hr@company.com",
            "legal@company.com",
            "employee-123@company.com",
        ]
        assert properties["path"] == "/hr/contracts/employment_contract.pdf"
        assert properties["size"] == 245000
        assert properties["mime_type"] == "application/pdf"

    @pytest.mark.asyncio
    async def test_ingestion_mixed_documents(self):
        """Test ingesting documents with and without entitlements."""
        # Setup
        dxr_index = Mock()
        neo4j_client = Mock(spec=Neo4jClient)
        neo4j_client.execute_write = AsyncMock(return_value=[{"node_id": "node_id"}])
        
        postgres_client = Mock(spec=PostgresClient)
        embedding_service = Mock(spec=EmbeddingService)
        metadata_parser = Mock(spec=DXRMetadataParser)

        service = IngestionService(
            dxr_index=dxr_index,
            neo4j_client=neo4j_client,
            postgres_client=postgres_client,
            embedding_service=embedding_service,
            metadata_parser=metadata_parser,
        )

        # Document 1: Has entitlements
        hit1 = Mock(spec=Hit)
        hit1.id = "doc1"
        hit1.file_name = "private.pdf"
        hit1._source = {"OWNER": "alice@example.com"}
        hit1.metadata = {
            "ds#object_id": "/private.pdf",
            "extractedMetadata": [],
            "dxr#raw_text": "",
        }

        # Document 2: No entitlements (public)
        hit2 = Mock(spec=Hit)
        hit2.id = "doc2"
        hit2.file_name = "public.pdf"
        hit2._source = {}
        hit2.metadata = {
            "ds#object_id": "/public.pdf",
            "extractedMetadata": [],
            "dxr#raw_text": "",
        }

        # Ingest both documents
        await service.ingest_document(hit=hit1, project_id="proj1", fetch_content=False)
        await service.ingest_document(hit=hit2, project_id="proj1", fetch_content=False)

        # Verify both were ingested
        assert neo4j_client.execute_write.call_count == 2

        # Check first document has entitlements
        call1_params = neo4j_client.execute_write.call_args_list[0][0][1]
        assert "owner_email" in call1_params["properties"]

        # Check second document has no entitlements
        call2_params = neo4j_client.execute_write.call_args_list[1][0][1]
        assert "owner_email" not in call2_params["properties"]

    @pytest.mark.asyncio
    async def test_entitlements_with_content_chunking(self):
        """Test that entitlements are preserved when content is chunked."""
        # Setup
        dxr_index = Mock()
        neo4j_client = Mock(spec=Neo4jClient)
        neo4j_client.execute_write = AsyncMock(return_value=[{"node_id": "node_id"}])
        
        postgres_client = Mock(spec=PostgresClient)
        
        embedding_service = Mock(spec=EmbeddingService)
        embedding_service.generate_embeddings_batch = AsyncMock(
            return_value=[[0.1] * 1536, [0.2] * 1536]  # 2 chunks
        )
        
        metadata_parser = Mock(spec=DXRMetadataParser)

        service = IngestionService(
            dxr_index=dxr_index,
            neo4j_client=neo4j_client,
            postgres_client=postgres_client,
            embedding_service=embedding_service,
            metadata_parser=metadata_parser,
        )

        # Document with content and entitlements
        hit = Mock(spec=Hit)
        hit.id = "doc-with-content"
        hit.file_name = "document.txt"
        hit._source = {
            "OWNER": "author@example.com",
            "WHO_CAN_ACCESS": ["reviewer1@example.com", "reviewer2@example.com"],
        }
        hit.metadata = {
            "ds#object_id": "/docs/document.txt",
            "extractedMetadata": [],
            "dxr#raw_text": "This is a long document. " * 100,  # Long enough for chunking
        }

        # Ingest with content
        await service.ingest_document(
            hit=hit,
            project_id="proj1",
            fetch_content=True,
        )

        # Verify document node was created with entitlements
        doc_call = neo4j_client.execute_write.call_args_list[0]
        doc_properties = doc_call[0][1]["properties"]
        
        assert doc_properties["owner_email"] == "author@example.com"
        assert doc_properties["accessible_by_emails"] == [
            "reviewer1@example.com",
            "reviewer2@example.com",
        ]

        # Verify chunks were created (should be multiple calls)
        assert neo4j_client.execute_write.call_count > 1


class TestEntitlementEdgeCases:
    """Test edge cases in entitlement handling."""

    @pytest.mark.asyncio
    async def test_special_characters_in_emails(self):
        """Test handling emails with special characters."""
        # Setup minimal service
        dxr_index = Mock()
        neo4j_client = Mock(spec=Neo4jClient)
        neo4j_client.execute_write = AsyncMock(return_value=[{"node_id": "node_id"}])
        postgres_client = Mock(spec=PostgresClient)
        embedding_service = Mock(spec=EmbeddingService)
        metadata_parser = Mock(spec=DXRMetadataParser)

        service = IngestionService(
            dxr_index=dxr_index,
            neo4j_client=neo4j_client,
            postgres_client=postgres_client,
            embedding_service=embedding_service,
            metadata_parser=metadata_parser,
        )

        # Email with special characters
        hit = Mock(spec=Hit)
        hit.id = "doc1"
        hit.file_name = "test.pdf"
        hit._source = {
            "OWNER": "first.last+tag@sub-domain.example.com",
            "WHO_CAN_ACCESS": [
                "user_name@example.com",
                "user-123@example.co.uk",
            ],
        }
        hit.metadata = {"ds#object_id": "/test.pdf", "extractedMetadata": [], "dxr#raw_text": ""}

        await service.ingest_document(hit=hit, project_id="proj1", fetch_content=False)

        # Verify special characters are preserved
        call_params = neo4j_client.execute_write.call_args[0][1]
        props = call_params["properties"]
        assert props["owner_email"] == "first.last+tag@sub-domain.example.com"
        assert "user_name@example.com" in props["accessible_by_emails"]

    @pytest.mark.asyncio
    async def test_large_access_list(self):
        """Test handling large WHO_CAN_ACCESS lists."""
        # Setup
        dxr_index = Mock()
        neo4j_client = Mock(spec=Neo4jClient)
        neo4j_client.execute_write = AsyncMock(return_value=[{"node_id": "node_id"}])
        postgres_client = Mock(spec=PostgresClient)
        embedding_service = Mock(spec=EmbeddingService)
        metadata_parser = Mock(spec=DXRMetadataParser)

        service = IngestionService(
            dxr_index=dxr_index,
            neo4j_client=neo4j_client,
            postgres_client=postgres_client,
            embedding_service=embedding_service,
            metadata_parser=metadata_parser,
        )

        # Large access list
        large_access_list = [f"user{i}@example.com" for i in range(100)]
        
        hit = Mock(spec=Hit)
        hit.id = "doc1"
        hit.file_name = "shared.pdf"
        hit._source = {
            "OWNER": "owner@example.com",
            "WHO_CAN_ACCESS": large_access_list,
        }
        hit.metadata = {"ds#object_id": "/shared.pdf", "extractedMetadata": [], "dxr#raw_text": ""}

        await service.ingest_document(hit=hit, project_id="proj1", fetch_content=False)

        # Verify all emails are preserved
        call_params = neo4j_client.execute_write.call_args[0][1]
        props = call_params["properties"]
        assert len(props["accessible_by_emails"]) == 100
        assert props["accessible_by_emails"][0] == "user0@example.com"
        assert props["accessible_by_emails"][99] == "user99@example.com"
