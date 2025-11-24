"""Query agent with vector search tool."""

from typing import Any

from pydantic import BaseModel
from pydantic_ai import Agent, RunContext

from database.neo4j_client import Neo4jClient
from ingestion.embedder import EmbeddingService


class AgentDependencies(BaseModel):
    """Dependencies for the query agent."""

    neo4j_client: Neo4jClient
    embedding_service: EmbeddingService
    project_id: str

    class Config:
        arbitrary_types_allowed = True


class SearchResult(BaseModel):
    """Search result item."""

    node_id: str
    text: str
    score: float
    metadata: dict[str, Any] = {}


# Define the query agent
query_agent = Agent(
    "openai:gpt-4o",
    deps_type=AgentDependencies,
    system_prompt="""You are a helpful assistant that answers questions based on a knowledge graph.
    
You have access to a vector search tool that can find relevant information.
Use the search tool to find relevant context, then provide a clear answer based on what you find.

Always cite your sources by mentioning which documents or entities you used.""",
)


@query_agent.tool
async def vector_search(
    ctx: RunContext[AgentDependencies],
    query: str,
    top_k: int = 5,
) -> list[SearchResult]:
    """Search for relevant information using semantic search.

    Args:
        ctx: Runtime context with dependencies
        query: Search query
        top_k: Number of results to return

    Returns:
        List of relevant search results
    """
    # Generate embedding for query
    embedding = await ctx.deps.embedding_service.generate_embedding(query)

    # Perform vector search
    results = await ctx.deps.neo4j_client.vector_search(
        embedding=embedding,
        label="Chunk",
        property_name="embedding",
        top_k=top_k,
    )

    # Format results
    search_results: list[SearchResult] = []

    for result in results:
        node = result.get("node", {})
        score = result.get("score", 0.0)

        search_results.append(
            SearchResult(
                node_id=node.get("id", ""),
                text=node.get("text", ""),
                score=score,
                metadata={
                    "chunk_index": node.get("chunk_index", 0),
                },
            )
        )

    return search_results


async def query(
    question: str,
    neo4j_client: Neo4jClient,
    embedding_service: EmbeddingService,
    project_id: str,
) -> str:
    """Query the knowledge graph.

    Args:
        question: User question
        neo4j_client: Neo4j client instance
        embedding_service: Embedding service instance
        project_id: Project ID

    Returns:
        Answer from the agent
    """
    deps = AgentDependencies(
        neo4j_client=neo4j_client,
        embedding_service=embedding_service,
        project_id=project_id,
    )

    result = await query_agent.run(question, deps=deps)

    return result.data
