"""Query agent with vector search tool."""

from collections.abc import AsyncIterator
from typing import Any

from pydantic import BaseModel
from pydantic_ai import Agent, RunContext

from database.neo4j_client import Neo4jClient
from ingestion.embedder import EmbeddingService


class AgentStep(BaseModel):
    """Agent execution step for streaming."""

    type: str  # 'thinking', 'tool_call_start', 'tool_call_result', 'answer'
    content: str | None = None
    tool: str | None = None
    args: dict[str, Any] | None = None
    result: Any = None


class AgentDependencies(BaseModel):
    """Dependencies for the query agent."""

    neo4j_client: Neo4jClient
    embedding_service: EmbeddingService
    project_id: str
    system_prompt: str | None = None
    step_callback: Any | None = None  # Async callback for streaming steps
    current_user_email: str | None = (
        None  # Current user's email for entitlement filtering
    )

    class Config:
        arbitrary_types_allowed = True


class SearchResult(BaseModel):
    """Search result item."""

    node_id: str
    text: str
    score: float
    metadata: dict[str, Any] = {}


# Default system prompt - can be overridden per project
DEFAULT_SYSTEM_PROMPT = """You are an intelligent assistant that answers questions using a knowledge graph.

You have access to multiple tools to explore the knowledge base:
1. **discover_schema**: Understand the structure of the knowledge graph (entities, relationships, patterns)
2. **vector_search**: Find relevant document chunks using semantic similarity
3. **entity_lookup**: Find specific entities by name
4. **graph_neighbors**: Explore relationships between entities
5. **graph_query**: Execute Cypher queries for complex graph traversal

**Recommended Strategy:**
1. **First interaction**: Call discover_schema to understand what entities and relationships exist
2. **For questions**: Start with vector_search to find relevant context
3. **For entity questions**: Use entity_lookup, then graph_neighbors to expand context
4. **For complex queries**: Use graph_query for multi-hop reasoning

**Hybrid Search Approach:**
- Use vector_search to get initial relevant chunks
- Extract entity names from chunks or question
- Use entity_lookup to find those entities in the graph
- Use graph_neighbors to expand context around entities
- Combine all information for a comprehensive answer

Always:
- Cite your sources by mentioning documents and entities
- Explain relationships you discovered in the graph
- If you find related entities, mention them to provide context
- Be clear about what information comes from direct search vs graph traversal"""


def build_entitlement_filter(user_email: str | None) -> str:
    """Build Cypher WHERE clause for entitlement filtering.

    Args:
        user_email: Current user's email address, or None for no filtering

    Returns:
        Cypher WHERE clause fragment for entitlement filtering
    """
    if not user_email:
        # No user context - return all documents (or based on project settings)
        return ""

    # Filter to only documents where:
    # 1. User is the owner, OR
    # 2. User is in the accessible_by_emails list, OR
    # 3. Document has no entitlement restrictions (owner_email is NULL)
    return f"""
    AND (
        doc.owner_email = '{user_email}'
        OR '{user_email}' IN COALESCE(doc.accessible_by_emails, [])
        OR doc.owner_email IS NULL
    )
    """


# Define the query agent
query_agent = Agent(
    "openai:gpt-4o-mini",
    deps_type=AgentDependencies,
    system_prompt=DEFAULT_SYSTEM_PROMPT,
)


@query_agent.tool
async def discover_schema(
    ctx: RunContext[AgentDependencies],
) -> str:
    """Discover the structure of the knowledge graph.

    This tool provides a formatted overview of:
    - Entity types and their counts
    - Relationship patterns between entities
    - Key properties available on entities

    Call this tool at the start of a conversation to understand what's available
    in the knowledge graph, then use that knowledge to plan your search strategy.

    Args:
        ctx: Runtime context with dependencies

    Returns:
        Formatted markdown description of the graph schema
    """
    # Emit tool start event if callback is available
    if ctx.deps.step_callback:
        await ctx.deps.step_callback(
            AgentStep(
                type="tool_call_start",
                tool="discover_schema",
                args={},
            )
        )

    # Get entity types and counts
    entity_query = """
    MATCH (e:Entity)
    WITH e.type as entity_type, count(e) as count, collect(e)[0..3] as samples
    RETURN entity_type, count, 
           [sample IN samples | keys(sample)] as property_sets
    ORDER BY count DESC
    """

    entity_results = await ctx.deps.neo4j_client.execute_query(entity_query)

    # Get relationship patterns
    rel_query = """
    MATCH (a:Entity)-[r]->(b:Entity)
    WITH type(r) as rel_type, a.type as from_type, b.type as to_type, count(r) as count
    RETURN rel_type, from_type, to_type, count
    ORDER BY count DESC
    LIMIT 50
    """

    rel_results = await ctx.deps.neo4j_client.execute_query(rel_query)

    # Format as markdown
    schema_md = "# Knowledge Graph Schema\n\n"

    # Entity types section
    schema_md += "## Entity Types\n\n"
    total_entities = 0
    for result in entity_results:
        entity_type = result.get("entity_type", "Unknown")
        count = result.get("count", 0)
        total_entities += count

        # Get common properties across samples
        property_sets = result.get("property_sets", [])
        common_props = set(property_sets[0]) if property_sets else set()
        for prop_set in property_sets[1:]:
            common_props &= set(prop_set)

        schema_md += f"- **{entity_type}**: {count} entities\n"
        if common_props:
            schema_md += f"  - Properties: {', '.join(sorted(common_props))}\n"

    schema_md += f"\n**Total Entities**: {total_entities}\n\n"

    # Relationship patterns section
    schema_md += "## Relationship Patterns\n\n"
    total_relationships = 0
    for result in rel_results:
        rel_type = result.get("rel_type", "Unknown")
        from_type = result.get("from_type", "Unknown")
        to_type = result.get("to_type", "Unknown")
        count = result.get("count", 0)
        total_relationships += count

        schema_md += f"- **{from_type}** --[{rel_type}]--> **{to_type}**: {count} relationships\n"

    schema_md += f"\n**Total Relationships**: {total_relationships}\n"

    # Emit tool result event
    if ctx.deps.step_callback:
        await ctx.deps.step_callback(
            AgentStep(
                type="tool_call_result",
                tool="discover_schema",
                result={
                    "schema": schema_md,
                    "entity_count": total_entities,
                    "relationship_count": total_relationships,
                    "entity_types": [r.get("entity_type") for r in entity_results],
                },
            )
        )

    return schema_md


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
    # Emit tool start event
    if ctx.deps.step_callback:
        await ctx.deps.step_callback(
            AgentStep(
                type="tool_call_start",
                tool="vector_search",
                args={"query": query, "top_k": top_k},
            )
        )

    # Generate embedding for query
    embedding = await ctx.deps.embedding_service.generate_embedding(query)

    # Build entitlement filter if user email is provided
    entitlement_filter = build_entitlement_filter(ctx.deps.current_user_email)

    # Perform vector search with entitlement filtering
    # We need to get the parent Document and check entitlements
    cypher_query = f"""
    CALL db.index.vector.queryNodes('chunk_embeddings', $top_k, $embedding)
    YIELD node as chunk, score
    MATCH (chunk)-[:PART_OF]->(doc:Document)
    WHERE doc.project_id = $project_id {entitlement_filter}
    RETURN chunk, score
    ORDER BY score DESC
    LIMIT $top_k
    """

    results = await ctx.deps.neo4j_client.execute_read(
        cypher_query,
        {
            "embedding": embedding,
            "top_k": top_k,
            "project_id": ctx.deps.project_id,
        },
    )

    # Format results
    search_results: list[SearchResult] = []

    for result in results:
        chunk = result.get("chunk", {})
        score = result.get("score", 0.0)

        search_results.append(
            SearchResult(
                node_id=chunk.get("id", ""),
                text=chunk.get("text", ""),
                score=score,
                metadata={
                    "chunk_index": chunk.get("chunk_index", 0),
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


@query_agent.tool
async def entity_lookup(
    ctx: RunContext[AgentDependencies],
    entity_name: str,
    fuzzy: bool = True,
) -> list[dict[str, Any]]:
    """Look up entities in the knowledge graph by name.

    Args:
        ctx: Runtime context with dependencies
        entity_name: Name of the entity to find
        fuzzy: If True, use fuzzy matching (contains), else exact match

    Returns:
        List of matching entities with their properties
    """
    # Emit tool start event
    if ctx.deps.step_callback:
        await ctx.deps.step_callback(
            AgentStep(
                type="tool_call_start",
                tool="entity_lookup",
                args={"entity_name": entity_name, "fuzzy": fuzzy},
            )
        )

    results = await ctx.deps.neo4j_client.entity_lookup(
        entity_name=entity_name,
        fuzzy=fuzzy,
    )

    entities = []
    for result in results:
        entity_node = result.get("e", {})
        entities.append(
            {
                "id": entity_node.get("id", ""),
                "name": entity_node.get("name", ""),
                "type": entity_node.get("type", ""),
                "properties": entity_node,
            }
        )

    # Emit tool result event
    if ctx.deps.step_callback:
        await ctx.deps.step_callback(
            AgentStep(
                type="tool_call_result",
                tool="entity_lookup",
                result={"entities": entities, "count": len(entities)},
            )
        )

    return entities


@query_agent.tool
async def graph_neighbors(
    ctx: RunContext[AgentDependencies],
    entity_id: str,
    relationship_types: list[str] | None = None,
    max_depth: int = 1,
) -> dict[str, Any]:
    """Explore relationships and neighboring entities in the knowledge graph.

    Args:
        ctx: Runtime context with dependencies
        entity_id: ID of the entity to start from
        relationship_types: Optional list of relationship types to follow (e.g., ["LOCATED_IN", "PART_OF"])
        max_depth: How many hops to traverse (1-2 recommended)

    Returns:
        Dictionary with neighboring entities and their relationships
    """
    # Emit tool start event
    if ctx.deps.step_callback:
        await ctx.deps.step_callback(
            AgentStep(
                type="tool_call_start",
                tool="graph_neighbors",
                args={
                    "entity_id": entity_id,
                    "relationship_types": relationship_types,
                    "max_depth": max_depth,
                },
            )
        )

    results = await ctx.deps.neo4j_client.get_neighbors(
        node_id=entity_id,
        relationship_types=relationship_types,
        direction="both",
        max_depth=max_depth,
    )

    neighbors = []
    for result in results:
        neighbor_node = result.get("neighbor", {})
        relationships = result.get("relationships", [])
        depth = result.get("depth", 0)

        neighbors.append(
            {
                "entity": {
                    "id": neighbor_node.get("id", ""),
                    "name": neighbor_node.get("name", ""),
                    "type": neighbor_node.get("type", ""),
                },
                "relationships": relationships,
                "depth": depth,
            }
        )

    # Emit tool result event
    if ctx.deps.step_callback:
        await ctx.deps.step_callback(
            AgentStep(
                type="tool_call_result",
                tool="graph_neighbors",
                result={
                    "source_entity_id": entity_id,
                    "neighbors": neighbors,
                    "total_found": len(neighbors),
                },
            )
        )

    return {
        "source_entity_id": entity_id,
        "neighbors": neighbors,
        "total_found": len(neighbors),
    }


@query_agent.tool
async def graph_query(
    ctx: RunContext[AgentDependencies],
    cypher_query: str,
    parameters: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Execute a Cypher query against the knowledge graph (read-only).

    Use this for complex graph queries that can't be answered with other tools.

    Args:
        ctx: Runtime context with dependencies
        cypher_query: Cypher query to execute (read-only, no CREATE/DELETE/etc)
        parameters: Optional query parameters

    Returns:
        Query results

    Example queries:
    - Find all entities connected to a specific entity:
      MATCH (e:Entity {name: $name})-[r]-(related) RETURN related, type(r)
    - Find shortest path between two entities:
      MATCH path = shortestPath((a:Entity {name: $name1})-[*]-(b:Entity {name: $name2}))
      RETURN path
    """
    # Emit tool start event
    if ctx.deps.step_callback:
        await ctx.deps.step_callback(
            AgentStep(
                type="tool_call_start",
                tool="graph_query",
                args={"cypher_query": cypher_query, "parameters": parameters},
            )
        )

    try:
        results = await ctx.deps.neo4j_client.execute_safe_cypher(
            cypher_query=cypher_query,
            parameters=parameters or {},
        )

        # Emit tool result event
        if ctx.deps.step_callback:
            await ctx.deps.step_callback(
                AgentStep(
                    type="tool_call_result",
                    tool="graph_query",
                    result={"results": results, "count": len(results)},
                )
            )

        return results
    except ValueError as e:
        error_result = [{"error": str(e)}]

        # Emit error result
        if ctx.deps.step_callback:
            await ctx.deps.step_callback(
                AgentStep(
                    type="tool_call_result",
                    tool="graph_query",
                    result={"error": str(e)},
                )
            )

        return error_result


async def query(
    question: str,
    neo4j_client: Neo4jClient,
    embedding_service: EmbeddingService,
    project_id: str,
    system_prompt: str | None = None,
    current_user_email: str | None = None,
) -> str:
    """Query the knowledge graph.

    Args:
        question: User question
        neo4j_client: Neo4j client instance
        embedding_service: Embedding service instance
        project_id: Project ID
        system_prompt: Optional custom system prompt (uses default if not provided)
        current_user_email: Optional user email for entitlement filtering

    Returns:
        Answer from the agent
    """
    deps = AgentDependencies(
        neo4j_client=neo4j_client,
        embedding_service=embedding_service,
        project_id=project_id,
        system_prompt=system_prompt,
        current_user_email=current_user_email,
    )

    # Create agent with custom prompt if provided
    agent = query_agent
    if system_prompt:
        agent = Agent(
            "openai:gpt-4o-mini",
            deps_type=AgentDependencies,
            system_prompt=system_prompt,
        )
        # Register all tools on the new agent
        for tool in query_agent._function_tools.values():
            agent._function_tools[tool.name] = tool

    result = await agent.run(question, deps=deps)

    return result.output


async def query_with_steps(
    question: str,
    neo4j_client: Neo4jClient,
    embedding_service: EmbeddingService,
    project_id: str,
    system_prompt: str | None = None,
    current_user_email: str | None = None,
) -> AsyncIterator[dict[str, Any]]:
    """Query the knowledge graph with step-by-step streaming.

    Args:
        question: User question
        neo4j_client: Neo4j client instance
        embedding_service: Embedding service instance
        project_id: Project ID
        system_prompt: Optional custom system prompt
        current_user_email: Optional user email for entitlement filtering

    Yields:
        Agent step events (tool calls, results, final answer)
    """
    steps: list[AgentStep] = []

    async def step_callback(step: AgentStep):
        """Collect steps for yielding."""
        steps.append(step)

    deps = AgentDependencies(
        neo4j_client=neo4j_client,
        embedding_service=embedding_service,
        project_id=project_id,
        system_prompt=system_prompt,
        step_callback=step_callback,
        current_user_email=current_user_email,
    )

    # Create agent with custom prompt if provided
    agent = query_agent
    if system_prompt:
        agent = Agent(
            "openai:gpt-4o-mini",
            deps_type=AgentDependencies,
            system_prompt=system_prompt,
        )
        # Register all tools on the new agent
        for tool in query_agent._function_tools.values():
            agent._function_tools[tool.name] = tool

    # Run agent (steps will be collected via callback)
    result = await agent.run(question, deps=deps)

    # Yield all collected steps
    for step in steps:
        yield step.model_dump()

    # Yield final answer
    yield {
        "type": "answer",
        "content": result.output,
    }
