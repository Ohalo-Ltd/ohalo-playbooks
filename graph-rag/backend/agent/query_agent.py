"""Query agent with vector search tool."""

from collections.abc import AsyncIterator
from typing import Any

from pydantic import BaseModel
from pydantic_ai import Agent, RunContext
from pydantic_ai.messages import ModelMessage, ModelRequest, ModelResponse, TextPart, UserPromptPart

from database.neo4j_client import Neo4jClient
from ingestion.embedder import EmbeddingService


class AgentStep(BaseModel):
    """Agent execution step for streaming."""

    type: str  # 'thinking', 'tool_call_start', 'tool_call_result', 'answer', 'error'
    content: str | None = None
    tool: str | None = None
    args: dict[str, Any] | None = None
    result: Any = None
    message: str | None = None  # For error messages


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
    document_name: str  # Name of the parent document
    document_id: str  # ID of the parent document
    chunk_index: int = 0
    metadata: dict[str, Any] = {}


# Default system prompt - can be overridden per project
DEFAULT_SYSTEM_PROMPT = """You are an intelligent assistant that answers questions by searching documents.

**How Search Works:**
- Documents are split into chunks for better semantic search
- **Semantic search** finds chunks with similar MEANING, not exact keyword matches
- When you search, you're finding relevant chunks within documents
- **Always cite the DOCUMENT NAME** when answering, not chunk numbers

**Available Tools:**
1. **decompose_query**: Break down complex/ambiguous questions into multiple search queries (optional)
   - Use this when the question is broad, complex, or covers multiple topics
   - Helps broaden the search scope
2. **vector_search**: Semantic search over document chunks (ALWAYS AVAILABLE)
   - Finds chunks by semantic similarity, not keyword matching
   - Returns chunks with their parent document names
   - Try multiple searches with different phrasings if first search returns nothing
   - If looking for specific terms, include context: instead of "UAV", try "UAV unmanned aircraft applications"
3. **discover_graph**: Check for extracted entities/relationships (optional)
4. **entity_lookup**: Find entities by name (only if graph exists)
5. **graph_neighbors**: Explore entity relationships (only if graph exists)
6. **graph_query**: Complex graph queries (only if graph exists)

**Search Strategy:**
- **Analyze the question**: Is it complex? Does it need decomposition?
- **Decompose if needed**: Use `decompose_query` to get better search terms for complex questions
- **Search**: Use `vector_search` with the original or decomposed queries
- **Explore Graph**: If relevant entities are found, use graph tools to explore relationships
- **Synthesize**: Combine information from all sources to answer the question

**How to Answer Questions:**
1. Use vector_search to find relevant information
2. Read the chunk text to get the information
3. **Cite the document name** (not chunk index) when answering
4. Group information by document when possible
5. Be specific: "According to [Document Name]..." or "In [Document Name], it states..."
6. If user asks for more thorough search, or is trying to expand knowledge, run tools multiple times with varied queries and/or increase the number of top_k results to fetch from tools

**Example:**
- ✅ GOOD: "The MQ-1 Gray Eagle UAV is mentioned in the document 'Army RDT&E Volume 4b'..."
- ❌ BAD: "Chunk 236 mentions UAV..."

**Important:**
- Semantic search finds meaning, not exact words - a chunk about "drones" won't necessarily match "UAV"
- If search returns nothing, try broader/more contextual queries
- Graph tools are optional - vector_search always works
- Always mention document names in your citations
- If multiple documents contain information, list them all"""


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
async def discover_graph(
    ctx: RunContext[AgentDependencies],
) -> str:
    """Check if additional graph structure (entities and relationships) exists beyond document chunks.

    This tool shows:
    - Whether any Entity nodes were extracted during ingestion
    - Entity types and their counts (if any exist)
    - Relationship patterns between entities (if any exist)

    NOTE: This is optional! The system always has document chunks available for vector_search.
    Graph entities are additional enrichment that may or may not exist.

    Args:
        ctx: Runtime context with dependencies

    Returns:
        Description of available graph structure, or a message indicating only document chunks are available
    """
    # Emit tool start event if callback is available
    if ctx.deps.step_callback:
        await ctx.deps.step_callback(
            AgentStep(
                type="tool_call_start",
                tool="discover_graph",
                args={"description": "Analyzing graph structure..."},
            )
        )

    # First check what node types exist in the database
    node_stats_query = """
    CALL db.labels() YIELD label
    CALL {
        WITH label
        MATCH (n)
        WHERE label IN labels(n)
        RETURN count(n) as count
    }
    RETURN label, count
    ORDER BY count DESC
    """

    node_stats = await ctx.deps.neo4j_client.execute_query(node_stats_query)

    # Get entity types and counts (may be empty)
    entity_query = """
    MATCH (e:Entity)
    WITH e.type as entity_type, count(e) as count, collect(e)[0..3] as samples
    RETURN entity_type, count, 
           [sample IN samples | keys(sample)] as property_sets
    ORDER BY count DESC
    """

    entity_results = await ctx.deps.neo4j_client.execute_query(entity_query)

    # Get relationship patterns (may be empty)
    rel_query = """
    MATCH (a:Entity)-[r]->(b:Entity)
    WITH type(r) as rel_type, a.type as from_type, b.type as to_type, count(r) as count
    RETURN rel_type, from_type, to_type, count
    ORDER BY count DESC
    LIMIT 50
    """

    rel_results = await ctx.deps.neo4j_client.execute_query(rel_query)

    # Format as markdown
    schema_md = "# Graph Structure\n\n"

    # Show all node types
    schema_md += "## Available Data\n\n"
    for stat in node_stats:
        label = stat.get("label", "Unknown")
        count = stat.get("count", 0)
        schema_md += f"- **{label}**: {count} nodes\n"
    schema_md += "\n"

    # Entity types section (if any exist)
    total_entities = 0
    if entity_results:
        schema_md += "## Extracted Entities\n\n"
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
    else:
        schema_md += "## Extracted Entities\n\n"
        schema_md += "*No entity nodes found. The system contains document chunks that can be searched using vector_search.*\n\n"

    # Relationship patterns section (if any exist)
    total_relationships = 0
    if rel_results:
        schema_md += "## Relationship Patterns\n\n"
        for result in rel_results:
            rel_type = result.get("rel_type", "Unknown")
            from_type = result.get("from_type", "Unknown")
            to_type = result.get("to_type", "Unknown")
            count = result.get("count", 0)
            total_relationships += count

            schema_md += f"- **{from_type}** --[{rel_type}]--> **{to_type}**: {count} relationships\n"

        schema_md += f"\n**Total Relationships**: {total_relationships}\n"
    else:
        schema_md += "## Relationships\n\n"
        schema_md += "*No entity relationships found.*\n"

    # Emit tool result event
    if ctx.deps.step_callback:
        await ctx.deps.step_callback(
            AgentStep(
                type="tool_call_result",
                tool="discover_graph",
                result={
                    "schema": schema_md,
                    "node_stats": node_stats,
                    "entity_count": total_entities,
                    "relationship_count": total_relationships,
                    "entity_types": [r.get("entity_type") for r in entity_results],
                    "has_entities": total_entities > 0,
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
                args={
                    "entity_name": entity_name,
                    "fuzzy": fuzzy,
                    "description": f"Looking up entity '{entity_name}'...",
                },
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
                    "description": f"Exploring relationships for '{entity_id}'...",
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
                args={
                    "cypher_query": cypher_query,
                    "parameters": parameters,
                    "description": "Running custom graph query...",
                },
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


# Decomposition agent for breaking down complex queries
decomposition_agent = Agent(
    "openai:gpt-4o-mini",
    system_prompt="""Break this user question into 3 more diverse, but related sets of keywords. The context is military, defense, procurement, military doctrine + any inferred context from user question, biased towards user's question. Each set of sentence-like keywords attempts to broaden the semantic embedding search while keeping it on topic. Just output the list as a simple JSON array: ["equipment procurement for FY26", "military equipment bidding fiscal year 2026", "..."]""",
)


@query_agent.tool
async def decompose_query(
    ctx: RunContext[AgentDependencies],
    query: str,
) -> list[str]:
    """Decompose a complex query into multiple search queries.

    Use this tool when:
    - The user's question is complex or ambiguous
    - The question covers multiple topics
    - A direct search might miss relevant context
    - You want to broaden the search scope

    Args:
        ctx: Runtime context
        query: The user's original query

    Returns:
        List of search queries to try
    """
    # Emit tool start event
    if ctx.deps.step_callback:
        await ctx.deps.step_callback(
            AgentStep(
                type="tool_call_start",
                tool="decompose_query",
                args={
                    "query": query,
                    "description": "Decomposing query into multiple search variations...",
                },
            )
        )

    try:
        result = await decomposition_agent.run(f"User question:\n{query}")
        # Parse JSON array from response
        import json
        
        # Clean up response if it contains markdown code blocks
        content = getattr(result, "data", None)
        if content is None:
            content = getattr(result, "output", str(result))
            
        if "```json" in content:
            content = content.split("```json")[1].split("```")[0].strip()
        elif "```" in content:
            content = content.split("```")[1].split("```")[0].strip()
            
        queries = json.loads(content)
        
        # Emit tool result event
        if ctx.deps.step_callback:
            await ctx.deps.step_callback(
                AgentStep(
                    type="tool_call_result",
                    tool="decompose_query",
                    result={"queries": queries},
                )
            )
            
        return queries
    except Exception as e:
        # Fallback to original query if decomposition fails
        if ctx.deps.step_callback:
            await ctx.deps.step_callback(
                AgentStep(
                    type="error",
                    message=f"Decomposition failed: {str(e)}",
                )
            )
        return [query]


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
        # type: ignore
        if hasattr(query_agent, "_function_tools"):
            for tool in query_agent._function_tools.values():  # type: ignore
                agent._function_tools[tool.name] = tool  # type: ignore

    result = await agent.run(question, deps=deps)

    return result.output


async def query_with_steps(
    question: str,
    neo4j_client: Neo4jClient,
    embedding_service: EmbeddingService,
    project_id: str,
    system_prompt: str | None = None,
    current_user_email: str | None = None,
    messages: list[dict[str, str]] | None = None,
) -> AsyncIterator[dict[str, Any]]:
    """Query the knowledge graph with step-by-step streaming.

    Args:
        question: User question
        neo4j_client: Neo4j client instance
        embedding_service: Embedding service instance
        project_id: Project ID
        system_prompt: Optional custom system prompt
        current_user_email: Optional user email for entitlement filtering
        messages: Optional chat history

    Yields:
        Agent step events (tool calls, results, final answer)
    """
    import asyncio
    
    # Queue for streaming steps
    queue: asyncio.Queue[AgentStep | None] = asyncio.Queue()

    async def step_callback(step: AgentStep):
        """Collect steps for yielding."""
        await queue.put(step)

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
        # type: ignore
        if hasattr(query_agent, "_function_tools"):
            for tool in query_agent._function_tools.values():  # type: ignore
                agent._function_tools[tool.name] = tool  # type: ignore

    # Run agent in background task
    async def run_agent():
        try:
            # Convert history
            history: list[ModelMessage] = []
            if messages:
                for msg in messages:
                    if msg["role"] == "user":
                        history.append(ModelRequest(parts=[UserPromptPart(content=msg["content"])]))
                    elif msg["role"] == "assistant":
                        history.append(ModelResponse(parts=[TextPart(content=msg["content"])]))

            async with agent.run_stream(question, deps=deps, message_history=history) as result:
                async for chunk in result.stream():
                    await queue.put(AgentStep(type="answer_chunk", content=chunk))
                
                # We can also get the full result data if needed, but chunks are enough for streaming
                # await queue.put(AgentStep(type="answer", content=result.data)) 
                
        except Exception as e:
            await queue.put(AgentStep(type="error", message=str(e)))
        finally:
            await queue.put(None)  # Sentinel

    asyncio.create_task(run_agent())

    # Yield steps as they arrive
    while True:
        step = await queue.get()
        if step is None:
            break
        yield step.model_dump()
