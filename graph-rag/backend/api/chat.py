"""API endpoints for chat/query functionality."""


from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from agent.query_agent import query
from database.neo4j_client import Neo4jClient
from ingestion.embedder import EmbeddingService

router = APIRouter(prefix="/api/chat", tags=["chat"])


class QueryRequest(BaseModel):
    """Query request model."""

    question: str
    project_id: str = "default"
    top_k: int = 5


class QueryResponse(BaseModel):
    """Query response model."""

    answer: str
    sources: list[dict] = []


async def get_neo4j_client() -> Neo4jClient:
    """Get Neo4j client dependency."""
    client = Neo4jClient()
    await client.connect()
    try:
        yield client
    finally:
        await client.close()


async def get_embedding_service() -> EmbeddingService:
    """Get embedding service dependency."""
    return EmbeddingService()


@router.post("/query", response_model=QueryResponse)
async def query_endpoint(
    request: QueryRequest,
    neo4j_client: Neo4jClient = Depends(get_neo4j_client),
    embedding_service: EmbeddingService = Depends(get_embedding_service),
) -> QueryResponse:
    """Query the knowledge graph.

    Args:
        request: Query request
        neo4j_client: Neo4j client
        embedding_service: Embedding service

    Returns:
        Query response with answer
    """
    try:
        answer = await query(
            question=request.question,
            neo4j_client=neo4j_client,
            embedding_service=embedding_service,
            project_id=request.project_id,
        )

        return QueryResponse(answer=answer, sources=[])

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
