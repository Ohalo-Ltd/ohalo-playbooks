"""Tests for entitlement metadata ingestion."""

import pytest
from unittest.mock import Mock, AsyncMock
from dxrpy.index.search_results import Hit

from ingestion.service import extract_entitlement_metadata, IngestionService
from ingestion.metadata_parser import DXRMetadataParser
from ingestion.embedder import EmbeddingService


class TestExtractEntitlementMetadata:
    """Test entitlement metadata extraction from DXR hits."""

    def test_extract_from_source_single_owner(self):
        """Test extracting owner from _source field."""
        hit = Mock(spec=Hit)
        hit._source = {"OWNER": "alice@example.com"}
        hit.metadata = {}

        result = extract_entitlement_metadata(hit)

        assert result == {"owner_email": "alice@example.com"}

    def test_extract_from_source_with_accessible_list(self):
        """Test extracting WHO_CAN_ACCESS as list."""
        hit = Mock(spec=Hit)
        hit._source = {
            "OWNER": "alice@example.com",
            "WHO_CAN_ACCESS": ["bob@example.com", "charlie@example.com"],
        }
        hit.metadata = {}

        result = extract_entitlement_metadata(hit)

        assert result == {
            "owner_email": "alice@example.com",
            "accessible_by_emails": ["bob@example.com", "charlie@example.com"],
        }

    def test_extract_from_source_with_accessible_string(self):
        """Test extracting WHO_CAN_ACCESS as comma-separated string."""
        hit = Mock(spec=Hit)
        hit._source = {
            "OWNER": "alice@example.com",
            "WHO_CAN_ACCESS": "bob@example.com, charlie@example.com, dave@example.com",
        }
        hit.metadata = {}

        result = extract_entitlement_metadata(hit)

        assert result == {
            "owner_email": "alice@example.com",
            "accessible_by_emails": [
                "bob@example.com",
                "charlie@example.com",
                "dave@example.com",
            ],
        }

    def test_extract_from_metadata_fallback(self):
        """Test falling back to metadata if _source not available."""
        hit = Mock(spec=Hit)
        hit._source = None
        hit.metadata = {
            "OWNER": "alice@example.com",
            "WHO_CAN_ACCESS": ["bob@example.com"],
        }

        result = extract_entitlement_metadata(hit)

        assert result == {
            "owner_email": "alice@example.com",
            "accessible_by_emails": ["bob@example.com"],
        }

    def test_extract_no_entitlements(self):
        """Test when no entitlement metadata is present."""
        hit = Mock(spec=Hit)
        hit._source = {}
        hit.metadata = {}

        result = extract_entitlement_metadata(hit)

        assert result == {}

    def test_extract_only_owner(self):
        """Test extracting only owner without WHO_CAN_ACCESS."""
        hit = Mock(spec=Hit)
        hit._source = {"OWNER": "alice@example.com"}
        hit.metadata = {}

        result = extract_entitlement_metadata(hit)

        assert result == {"owner_email": "alice@example.com"}

    def test_extract_only_accessible(self):
        """Test extracting only WHO_CAN_ACCESS without owner."""
        hit = Mock(spec=Hit)
        hit._source = {"WHO_CAN_ACCESS": ["bob@example.com", "charlie@example.com"]}
        hit.metadata = {}

        result = extract_entitlement_metadata(hit)

        assert result == {
            "accessible_by_emails": ["bob@example.com", "charlie@example.com"]
        }

    def test_extract_empty_accessible_list(self):
        """Test with empty WHO_CAN_ACCESS list."""
        hit = Mock(spec=Hit)
        hit._source = {
            "OWNER": "alice@example.com",
            "WHO_CAN_ACCESS": [],
        }
        hit.metadata = {}

        result = extract_entitlement_metadata(hit)

        # Empty list should not be included
        assert result == {"owner_email": "alice@example.com"}

    def test_extract_whitespace_string(self):
        """Test WHO_CAN_ACCESS with whitespace and empty values."""
        hit = Mock(spec=Hit)
        hit._source = {
            "WHO_CAN_ACCESS": "bob@example.com,  , charlie@example.com,  ",
        }
        hit.metadata = {}

        result = extract_entitlement_metadata(hit)

        # Empty strings should be filtered out
        assert result == {
            "accessible_by_emails": ["bob@example.com", "charlie@example.com"]
        }

    def test_extract_none_values(self):
        """Test handling None values in list."""
        hit = Mock(spec=Hit)
        hit._source = {
            "WHO_CAN_ACCESS": ["bob@example.com", None, "charlie@example.com"],
        }
        hit.metadata = {}

        result = extract_entitlement_metadata(hit)

        # None values should be filtered out
        assert result == {
            "accessible_by_emails": ["bob@example.com", "charlie@example.com"]
        }


class TestIngestionServiceWithEntitlements:
    """Test IngestionService with entitlement metadata."""

    @pytest.mark.asyncio
    async def test_ingest_document_with_entitlements(self):
        """Test that entitlements are passed to document node creation."""
        # Setup mocks
        dxr_index = Mock()
        neo4j_client = AsyncMock()
        postgres_client = AsyncMock()
        embedding_service = Mock(spec=EmbeddingService)
        metadata_parser = Mock(spec=DXRMetadataParser)

        service = IngestionService(
            dxr_index=dxr_index,
            neo4j_client=neo4j_client,
            postgres_client=postgres_client,
            embedding_service=embedding_service,
            metadata_parser=metadata_parser,
        )

        # Mock graph_writer
        service.graph_writer.create_document_node = AsyncMock()
        service.graph_writer.create_chunk_node = AsyncMock()

        # Create mock hit with entitlements
        hit = Mock(spec=Hit)
        hit.id = "doc123"
        hit.file_name = "contract.pdf"
        hit._source = {
            "OWNER": "alice@example.com",
            "WHO_CAN_ACCESS": ["bob@example.com", "charlie@example.com"],
        }
        hit.metadata = {
            "ds#object_id": "/contracts/contract.pdf",
            "ds#size": 12345,
            "ds#mime_type": "application/pdf",
            "metadata#binary_hash": "abc123",
            "extractedMetadata": [],
            "dxr#raw_text": "",
        }

        # Ingest document
        await service.ingest_document(
            hit=hit,
            project_id="project123",
            fetch_content=False,
        )

        # Verify document node was created with entitlement properties
        service.graph_writer.create_document_node.assert_called_once()
        call_args = service.graph_writer.create_document_node.call_args

        assert call_args[1]["document_id"] == "doc123"
        assert call_args[1]["name"] == "contract.pdf"
        assert call_args[1]["project_id"] == "project123"

        properties = call_args[1]["properties"]
        assert properties["owner_email"] == "alice@example.com"
        assert properties["accessible_by_emails"] == [
            "bob@example.com",
            "charlie@example.com",
        ]
        assert properties["path"] == "/contracts/contract.pdf"
        assert properties["size"] == 12345

    @pytest.mark.asyncio
    async def test_ingest_document_without_entitlements(self):
        """Test that ingestion works without entitlement metadata."""
        # Setup mocks
        dxr_index = Mock()
        neo4j_client = AsyncMock()
        postgres_client = AsyncMock()
        embedding_service = Mock(spec=EmbeddingService)
        metadata_parser = Mock(spec=DXRMetadataParser)

        service = IngestionService(
            dxr_index=dxr_index,
            neo4j_client=neo4j_client,
            postgres_client=postgres_client,
            embedding_service=embedding_service,
            metadata_parser=metadata_parser,
        )

        # Mock graph_writer
        service.graph_writer.create_document_node = AsyncMock()

        # Create mock hit WITHOUT entitlements
        hit = Mock(spec=Hit)
        hit.id = "doc456"
        hit.file_name = "report.pdf"
        hit._source = {}
        hit.metadata = {
            "ds#object_id": "/reports/report.pdf",
            "ds#size": 54321,
            "ds#mime_type": "application/pdf",
            "extractedMetadata": [],
            "dxr#raw_text": "",
        }

        # Ingest document
        await service.ingest_document(
            hit=hit,
            project_id="project123",
            fetch_content=False,
        )

        # Verify document node was created without entitlement properties
        service.graph_writer.create_document_node.assert_called_once()
        call_args = service.graph_writer.create_document_node.call_args

        properties = call_args[1]["properties"]
        assert "owner_email" not in properties
        assert "accessible_by_emails" not in properties
        # Standard properties should still be present
        assert properties["path"] == "/reports/report.pdf"
        assert properties["size"] == 54321


class TestEntitlementScenarios:
    """Test real-world entitlement scenarios."""

    def test_public_document_no_restrictions(self):
        """Test document with no entitlement restrictions."""
        hit = Mock(spec=Hit)
        hit._source = {}
        hit.metadata = {}

        result = extract_entitlement_metadata(hit)

        # No restrictions means accessible to all (when entitlements enabled)
        assert result == {}

    def test_owner_only_document(self):
        """Test document accessible only by owner."""
        hit = Mock(spec=Hit)
        hit._source = {"OWNER": "alice@example.com"}
        hit.metadata = {}

        result = extract_entitlement_metadata(hit)

        assert result == {"owner_email": "alice@example.com"}

    def test_shared_document(self):
        """Test document shared with specific users."""
        hit = Mock(spec=Hit)
        hit._source = {
            "OWNER": "alice@example.com",
            "WHO_CAN_ACCESS": [
                "alice@example.com",  # Owner also in access list
                "bob@example.com",
                "team@example.com",
            ],
        }
        hit.metadata = {}

        result = extract_entitlement_metadata(hit)

        assert result["owner_email"] == "alice@example.com"
        assert len(result["accessible_by_emails"]) == 3
        assert "alice@example.com" in result["accessible_by_emails"]

    def test_group_access_document(self):
        """Test document accessible by group email."""
        hit = Mock(spec=Hit)
        hit._source = {
            "OWNER": "alice@example.com",
            "WHO_CAN_ACCESS": "engineering-team@example.com, product-team@example.com",
        }
        hit.metadata = {}

        result = extract_entitlement_metadata(hit)

        assert result == {
            "owner_email": "alice@example.com",
            "accessible_by_emails": [
                "engineering-team@example.com",
                "product-team@example.com",
            ],
        }
