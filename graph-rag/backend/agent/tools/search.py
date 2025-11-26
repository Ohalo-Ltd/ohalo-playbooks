"""Vector search tool."""

from pydantic_ai import RunContext

from agent.models import AgentDependencies, AgentStep, SearchResult
from agent.utils import build_entitlement_filter


async def vector_search(
    ctx: RunContext[AgentDependencies],
    query: str,
    top_k: int = 8,
) -> list[SearchResult]:
    """Search for relevant information using semantic search.

    Args:
        ctx: Runtime context with dependencies
        query: Search query
        top_k: Number of results to return

    Returns:
        List of relevant search results
    """
    # Emit tool start event
    if ctx.deps.step_callback:
        await ctx.deps.step_callback(
            AgentStep(
                type="tool_call_start",
                tool="vector_search",
                args={
                    "query": query,
                    "top_k": top_k,
                    "description": f"Searching for '{query}'...",
                },
            )
        )

    # Generate embedding for query
    embedding = await ctx.deps.embedding_service.generate_embedding(query)

    # Build entitlement filter if user email is provided
    entitlement_filter = build_entitlement_filter(ctx.deps.current_user_email)

    # Perform vector search with entitlement filtering
    # Get chunks and their parent documents, checking entitlements
    cypher_query = f"""
    CALL db.index.vector.queryNodes('chunk_embeddings', $top_k, $embedding)
    YIELD node as chunk, score
    MATCH (doc:Document)-[:HAS_CHUNK]->(chunk)
    WHERE doc.project_id = $project_id {entitlement_filter}
    RETURN chunk, score, doc
    ORDER BY score DESC
    LIMIT $top_k
    """

    results = await ctx.deps.neo4j_client.execute_query(
        cypher_query,
        {
            "embedding": embedding,
            "top_k": top_k,
            "project_id": ctx.deps.project_id,
        },
    )

    # Format results with document information
    search_results: list[SearchResult] = []

    for result in results:
        chunk = result.get("chunk", {})
        doc = result.get("doc", {})
        score = result.get("score", 0.0)

        search_results.append(
            SearchResult(
                node_id=chunk.get("id", ""),
                text=chunk.get("text", ""),
                score=score,
                document_name=doc.get("name", "Unknown Document"),
                document_id=doc.get("id", ""),
                chunk_index=chunk.get("chunk_index", 0),
                metadata={
                    "document_path": doc.get("path", ""),
                    "document_size": doc.get("size", 0),
                },
            )
        )

    # Emit tool result event
    if ctx.deps.step_callback:
        await ctx.deps.step_callback(
            AgentStep(
                type="tool_call_result",
                tool="vector_search",
                result={
                    "results": [r.model_dump() for r in search_results],
                    "count": len(search_results),
                },
            )
        )

    return search_results
