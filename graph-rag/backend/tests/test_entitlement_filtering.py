"""Tests for entitlement-based query filtering (Phase 3)."""

import pytest
from unittest.mock import Mock, AsyncMock

from agent.models import AgentDependencies
from agent.tools.search import vector_search
from agent.utils import build_entitlement_filter
from database.neo4j_client import Neo4jClient
from ingestion.embedder import EmbeddingService


class TestBuildEntitlementFilter:
    """Test entitlement filter construction."""

    def test_no_user_email_returns_empty(self):
        """Test that no user email returns empty filter."""
        result = build_entitlement_filter(None)
        assert result == ""

    def test_user_email_creates_filter(self):
        """Test that user email creates proper WHERE clause."""
        result = build_entitlement_filter("alice@example.com")
        
        assert "doc.owner_email = 'alice@example.com'" in result
        assert "'alice@example.com' IN COALESCE(doc.accessible_by_emails, [])" in result
        assert "doc.owner_email IS NULL" in result
        assert "AND" in result
        assert result.count("OR") >= 2  # At least 2 OR conditions

    def test_filter_structure(self):
        """Test filter has proper logical structure."""
        result = build_entitlement_filter("bob@example.com")
        
        # Should have opening AND
        assert result.strip().startswith("AND")
        # Should have parentheses for grouping
        assert "(" in result
        assert ")" in result


class TestVectorSearchFiltering:
    """Test vector search with entitlement filtering."""

    @pytest.mark.asyncio
    async def test_vector_search_without_user_filters_all_docs(self):
        """Test vector search without user context returns all matching docs."""
        # Setup mocks
        neo4j_client = Mock(spec=Neo4jClient)
        neo4j_client.execute_read = AsyncMock(return_value=[
            {
                "chunk": {
                    "id": "chunk1",
                    "text": "Important document content",
                    "chunk_index": 0,
                },
                "score": 0.95,
            },
            {
                "chunk": {
                    "id": "chunk2",
                    "text": "Another document content",
                    "chunk_index": 0,
                },
                "score": 0.85,
            },
        ])

        embedding_service = Mock(spec=EmbeddingService)
        embedding_service.generate_embedding = AsyncMock(return_value=[0.1] * 1536)

        deps = AgentDependencies(
            neo4j_client=neo4j_client,
            embedding_service=embedding_service,
            project_id="project123",
            current_user_email=None,  # No user context
        )

        # Create mock context
        ctx = Mock()
        ctx.deps = deps

        # Call vector_search
        results = await vector_search(ctx, query="test query", top_k=5)

        # Verify query was called
        neo4j_client.execute_read.assert_called_once()
        call_args = neo4j_client.execute_read.call_args

        # Check the Cypher query doesn't include entitlement filter
        cypher_query = call_args[0][0]
        assert "WHERE doc.project_id = $project_id" in cypher_query
        # Should not have additional entitlement filtering when user_email is None
        assert cypher_query.count("WHERE") == 1  # Only project_id filter

        # Verify results
        assert len(results) == 2
        assert results[0].text == "Important document content"
        assert results[0].score == 0.95

    @pytest.mark.asyncio
    async def test_vector_search_with_user_filters_by_entitlements(self):
        """Test vector search with user context filters by entitlements."""
        # Setup mocks
        neo4j_client = Mock(spec=Neo4jClient)
        neo4j_client.execute_read = AsyncMock(return_value=[
            {
                "chunk": {
                    "id": "chunk1",
                    "text": "User's accessible document",
                    "chunk_index": 0,
                },
                "score": 0.92,
            },
        ])

        embedding_service = Mock(spec=EmbeddingService)
        embedding_service.generate_embedding = AsyncMock(return_value=[0.1] * 1536)

        deps = AgentDependencies(
            neo4j_client=neo4j_client,
            embedding_service=embedding_service,
            project_id="project123",
            current_user_email="alice@example.com",  # User context provided
        )

        ctx = Mock()
        ctx.deps = deps

        # Call vector_search
        results = await vector_search(ctx, query="test query", top_k=5)

        # Verify query was called
        neo4j_client.execute_read.assert_called_once()
        call_args = neo4j_client.execute_read.call_args

        # Check the Cypher query includes entitlement filter
        cypher_query = call_args[0][0]
        assert "doc.owner_email = 'alice@example.com'" in cypher_query
        assert "'alice@example.com' IN COALESCE(doc.accessible_by_emails, [])" in cypher_query
        assert "doc.owner_email IS NULL" in cypher_query

        # Verify results were filtered
        assert len(results) == 1
        assert results[0].text == "User's accessible document"


class TestUserAccessScenarios:
    """Test different user access scenarios."""

    @pytest.mark.asyncio
    async def test_user_sees_own_documents(self):
        """Test user can see documents they own."""
        neo4j_client = Mock(spec=Neo4jClient)
        neo4j_client.execute_read = AsyncMock(return_value=[
            {
                "chunk": {
                    "id": "chunk1",
                    "text": "Alice's document",
                    "chunk_index": 0,
                },
                "score": 0.9,
            },
        ])

        embedding_service = Mock(spec=EmbeddingService)
        embedding_service.generate_embedding = AsyncMock(return_value=[0.1] * 1536)

        deps = AgentDependencies(
            neo4j_client=neo4j_client,
            embedding_service=embedding_service,
            project_id="proj1",
            current_user_email="alice@example.com",
        )

        ctx = Mock()
        ctx.deps = deps

        results = await vector_search(ctx, query="my documents", top_k=10)

        # Verify filter includes owner check
        call_args = neo4j_client.execute_read.call_args
        cypher = call_args[0][0]
        assert "doc.owner_email = 'alice@example.com'" in cypher

    @pytest.mark.asyncio
    async def test_user_sees_shared_documents(self):
        """Test user can see documents shared with them."""
        neo4j_client = Mock(spec=Neo4jClient)
        neo4j_client.execute_read = AsyncMock(return_value=[
            {
                "chunk": {
                    "id": "chunk1",
                    "text": "Shared document content",
                    "chunk_index": 0,
                },
                "score": 0.88,
            },
        ])

        embedding_service = Mock(spec=EmbeddingService)
        embedding_service.generate_embedding = AsyncMock(return_value=[0.1] * 1536)

        deps = AgentDependencies(
            neo4j_client=neo4j_client,
            embedding_service=embedding_service,
            project_id="proj1",
            current_user_email="bob@example.com",
        )

        ctx = Mock()
        ctx.deps = deps

        results = await vector_search(ctx, query="shared docs", top_k=10)

        # Verify filter checks accessible_by_emails
        call_args = neo4j_client.execute_read.call_args
        cypher = call_args[0][0]
        assert "'bob@example.com' IN COALESCE(doc.accessible_by_emails, [])" in cypher

    @pytest.mark.asyncio
    async def test_user_sees_public_documents(self):
        """Test user can see documents with no entitlement restrictions."""
        neo4j_client = Mock(spec=Neo4jClient)
        neo4j_client.execute_read = AsyncMock(return_value=[
            {
                "chunk": {
                    "id": "chunk1",
                    "text": "Public document",
                    "chunk_index": 0,
                },
                "score": 0.85,
            },
        ])

        embedding_service = Mock(spec=EmbeddingService)
        embedding_service.generate_embedding = AsyncMock(return_value=[0.1] * 1536)

        deps = AgentDependencies(
            neo4j_client=neo4j_client,
            embedding_service=embedding_service,
            project_id="proj1",
            current_user_email="charlie@example.com",
        )

        ctx = Mock()
        ctx.deps = deps

        results = await vector_search(ctx, query="public info", top_k=10)

        # Verify filter includes NULL owner check (public docs)
        call_args = neo4j_client.execute_read.call_args
        cypher = call_args[0][0]
        assert "doc.owner_email IS NULL" in cypher


class TestTwoUsersSeeDifferentResults:
    """Test the acceptance criteria: two users see different things."""

    @pytest.mark.asyncio
    async def test_alice_and_bob_see_different_documents(self):
        """Test that Alice and Bob see different results for the same query."""
        embedding_service = Mock(spec=EmbeddingService)
        embedding_service.generate_embedding = AsyncMock(return_value=[0.1] * 1536)

        # Alice's perspective
        alice_neo4j = Mock(spec=Neo4jClient)
        alice_neo4j.execute_read = AsyncMock(return_value=[
            {
                "chunk": {
                    "id": "chunk1",
                    "text": "HR confidential document",
                    "chunk_index": 0,
                },
                "score": 0.9,
            },
            {
                "chunk": {
                    "id": "chunk2",
                    "text": "Public company policy",
                    "chunk_index": 0,
                },
                "score": 0.85,
            },
        ])

        alice_deps = AgentDependencies(
            neo4j_client=alice_neo4j,
            embedding_service=embedding_service,
            project_id="proj1",
            current_user_email="alice@hr.com",
        )

        alice_ctx = Mock()
        alice_ctx.deps = alice_deps

        alice_results = await vector_search(alice_ctx, query="company documents", top_k=10)

        # Bob's perspective
        bob_neo4j = Mock(spec=Neo4jClient)
        bob_neo4j.execute_read = AsyncMock(return_value=[
            {
                "chunk": {
                    "id": "chunk2",
                    "text": "Public company policy",
                    "chunk_index": 0,
                },
                "score": 0.85,
            },
        ])

        bob_deps = AgentDependencies(
            neo4j_client=bob_neo4j,
            embedding_service=embedding_service,
            project_id="proj1",
            current_user_email="bob@engineering.com",
        )

        bob_ctx = Mock()
        bob_ctx.deps = bob_deps

        bob_results = await vector_search(bob_ctx, query="company documents", top_k=10)

        # Verify Alice sees more documents (including HR confidential)
        assert len(alice_results) == 2
        alice_texts = [r.text for r in alice_results]
        assert "HR confidential document" in alice_texts
        assert "Public company policy" in alice_texts

        # Verify Bob only sees public document
        assert len(bob_results) == 1
        assert bob_results[0].text == "Public company policy"

        # Verify different filters were applied
        alice_call = alice_neo4j.execute_read.call_args[0][0]
        bob_call = bob_neo4j.execute_read.call_args[0][0]

        assert "alice@hr.com" in alice_call
        assert "bob@engineering.com" in bob_call
        assert alice_call != bob_call  # Different filters


class TestEdgeCases:
    """Test edge cases in entitlement filtering."""

    @pytest.mark.asyncio
    async def test_empty_email_string(self):
        """Test handling of empty string as user email."""
        filter_result = build_entitlement_filter("")
        # Empty string should be treated as falsy, return empty filter
        assert filter_result == ""

    @pytest.mark.asyncio
    async def test_special_characters_in_email(self):
        """Test emails with special characters in filter."""
        result = build_entitlement_filter("user+tag@sub-domain.example.com")
        
        assert "user+tag@sub-domain.example.com" in result
        # Verify special chars don't break the Cypher syntax
        assert "doc.owner_email = 'user+tag@sub-domain.example.com'" in result

    @pytest.mark.asyncio
    async def test_no_results_when_not_authorized(self):
        """Test that unauthorized user gets no results."""
        neo4j_client = Mock(spec=Neo4jClient)
        neo4j_client.execute_read = AsyncMock(return_value=[])  # No results

        embedding_service = Mock(spec=EmbeddingService)
        embedding_service.generate_embedding = AsyncMock(return_value=[0.1] * 1536)

        deps = AgentDependencies(
            neo4j_client=neo4j_client,
            embedding_service=embedding_service,
            project_id="proj1",
            current_user_email="unauthorized@example.com",
        )

        ctx = Mock()
        ctx.deps = deps

        results = await vector_search(ctx, query="secret docs", top_k=10)

        assert len(results) == 0
