"""Graph exploration tools."""

from typing import Any

from pydantic_ai import RunContext

from agent.models import AgentDependencies, AgentStep


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
