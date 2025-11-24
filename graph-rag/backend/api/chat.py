"""API endpoints for chat/query functionality."""

import asyncio
import json
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from agent.query_agent import query, query_with_steps
from database.neo4j_client import Neo4jClient
from database.postgres_client import PostgresClient
from ingestion.embedder import EmbeddingService

router = APIRouter(prefix="/api/chat", tags=["chat"])


class QueryRequest(BaseModel):
    """Query request model."""

    question: str
    project_id: str = "default"
    top_k: int = 5
    include_graph_context: bool = True


class SourceChunk(BaseModel):
    """Source chunk with relevance score."""

    id: str
    text: str
    score: float
    chunk_index: int = 0


class RelatedEntity(BaseModel):
    """Related entity from the graph."""

    id: str
    name: str
    type: str


class EntityRelationship(BaseModel):
    """Relationship between entities."""

    from_entity: str
    to_entity: str
    relationship_type: str


class QueryResponse(BaseModel):
    """Query response model."""

    answer: str
    sources: list[SourceChunk] = []
    related_entities: list[RelatedEntity] = []
    relationships: list[EntityRelationship] = []
    conversation_id: str = ""


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


async def get_postgres_client() -> PostgresClient:
    """Get Postgres client dependency."""
    client = PostgresClient()
    await client.connect()
    try:
        yield client
    finally:
        await client.close()


@router.post("/query", response_model=QueryResponse)
async def query_endpoint(
    request: QueryRequest,
    neo4j_client: Neo4jClient = Depends(get_neo4j_client),
    embedding_service: EmbeddingService = Depends(get_embedding_service),
    pg_client: PostgresClient = Depends(get_postgres_client),
) -> QueryResponse:
    """Query the knowledge graph using the AI agent.

    The agent will orchestrate its own strategy using multiple tools:
    - discover_schema to understand the graph structure
    - vector_search for semantic similarity
    - entity_lookup to find specific entities
    - graph_neighbors to explore relationships
    - graph_query for complex traversal

    Args:
        request: Query request
        neo4j_client: Neo4j client
        embedding_service: Embedding service
        pg_client: Postgres client

    Returns:
        Query response with answer from the agent
    """
    try:
        # Fetch project settings to get custom system prompt
        system_prompt = None
        if request.project_id != "default":
            try:
                project_row = await pg_client.fetchrow(
                    "SELECT system_prompt FROM projects WHERE id = $1",
                    UUID(request.project_id),
                )
                if project_row and project_row["system_prompt"]:
                    system_prompt = project_row["system_prompt"]
            except (ValueError, Exception):
                # Invalid UUID or project not found - use default prompt
                pass

        # Let the agent orchestrate its own hybrid search strategy
        answer = await query(
            question=request.question,
            neo4j_client=neo4j_client,
            embedding_service=embedding_service,
            project_id=request.project_id,
            system_prompt=system_prompt,
        )

        # For now, return a simple response
        # The agent's answer includes information from all tools it used
        return QueryResponse(
            answer=answer,
            sources=[],  # Agent handles its own source citation in the answer
            related_entities=[],  # Agent mentions entities in its answer
            relationships=[],  # Agent describes relationships in its answer
            conversation_id=request.project_id,
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/query/stream")
async def stream_query_endpoint(
    request: QueryRequest,
    neo4j_client: Neo4jClient = Depends(get_neo4j_client),
    embedding_service: EmbeddingService = Depends(get_embedding_service),
    pg_client: PostgresClient = Depends(get_postgres_client),
) -> StreamingResponse:
    """Stream agent reasoning steps via Server-Sent Events.

    This endpoint provides real-time visibility into the agent's decision-making:
    - thinking events: Agent reasoning
    - tool_call_start: Tool invocation with parameters
    - tool_call_result: Tool execution results
    - answer: Final response

    Args:
        request: Query request
        neo4j_client: Neo4j client
        embedding_service: Embedding service
        pg_client: Postgres client

    Returns:
        StreamingResponse with SSE events
    """

    async def event_generator():
        """Generate SSE events for agent steps."""
        try:
            # Fetch custom system prompt if available
            system_prompt = None
            if request.project_id != "default":
                try:
                    project_row = await pg_client.fetchrow(
                        "SELECT system_prompt FROM projects WHERE id = $1",
                        UUID(request.project_id),
                    )
                    if project_row and project_row["system_prompt"]:
                        system_prompt = project_row["system_prompt"]
                except (ValueError, Exception):
                    pass

            # Emit initial thinking event
            yield f"data: {json.dumps({'type': 'thinking', 'content': 'Analyzing your question...'})}\n\n"
            await asyncio.sleep(0.1)  # Small delay for visual feedback

            # Run agent with step instrumentation
            async for step in query_with_steps(
                question=request.question,
                neo4j_client=neo4j_client,
                embedding_service=embedding_service,
                project_id=request.project_id,
                system_prompt=system_prompt,
            ):
                # Emit step as SSE event
                yield f"data: {json.dumps(step)}\n\n"
                await asyncio.sleep(0.05)  # Throttle for better UX

            # Signal completion
            yield "data: [DONE]\n\n"

        except Exception as e:
            error_event = {
                "type": "error",
                "message": str(e),
            }
            yield f"data: {json.dumps(error_event)}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",  # Disable nginx buffering
        },
    )
