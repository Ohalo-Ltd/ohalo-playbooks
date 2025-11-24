"""Schema discovery and management for the knowledge graph."""

from typing import Any

from database.neo4j_client import Neo4jClient


class GraphSchema:
    """Represents the discovered schema of the knowledge graph."""

    def __init__(
        self,
        entity_types: list[dict[str, Any]],
        relationship_types: list[dict[str, Any]],
        statistics: dict[str, Any],
    ):
        """Initialize graph schema.

        Args:
            entity_types: List of entity type definitions
            relationship_types: List of relationship type definitions
            statistics: Schema statistics (counts, etc.)
        """
        self.entity_types = entity_types
        self.relationship_types = relationship_types
        self.statistics = statistics

    def to_dict(self) -> dict[str, Any]:
        """Convert schema to dictionary."""
        return {
            "entity_types": self.entity_types,
            "relationship_types": self.relationship_types,
            "statistics": self.statistics,
        }


class SchemaDiscoveryService:
    """Service for discovering and managing graph schema."""

    def __init__(self, neo4j_client: Neo4jClient):
        """Initialize schema discovery service.

        Args:
            neo4j_client: Neo4j client for querying schema
        """
        self.neo4j = neo4j_client

    async def discover_schema(self) -> GraphSchema:
        """Discover the current schema of the knowledge graph.

        Returns:
            GraphSchema with entity types, relationships, and statistics
        """
        # Get entity types and counts
        entity_query = """
        MATCH (e:Entity)
        WITH e.type as entity_type, count(e) as count, collect(e)[0] as sample
        RETURN entity_type, 
               count,
               keys(sample) as properties
        ORDER BY count DESC
        """

        entity_results = await self.neo4j.execute_query(entity_query)

        entity_types = []
        for result in entity_results:
            entity_types.append({
                "type": result.get("entity_type", "Unknown"),
                "count": result.get("count", 0),
                "properties": result.get("properties", []),
            })

        # Get relationship types and counts
        relationship_query = """
        MATCH (a:Entity)-[r]->(b:Entity)
        WITH type(r) as rel_type, 
             a.type as from_type, 
             b.type as to_type,
             count(r) as count
        RETURN rel_type, from_type, to_type, count
        ORDER BY count DESC
        """

        rel_results = await self.neo4j.execute_query(relationship_query)

        relationship_types = []
        for result in rel_results:
            relationship_types.append({
                "type": result.get("rel_type", "Unknown"),
                "from_type": result.get("from_type", "Unknown"),
                "to_type": result.get("to_type", "Unknown"),
                "count": result.get("count", 0),
            })

        # Get overall statistics
        stats_query = """
        MATCH (n)
        WITH labels(n) as labels, count(n) as count
        UNWIND labels as label
        RETURN label, sum(count) as total_nodes
        UNION ALL
        MATCH ()-[r]->()
        RETURN 'relationships' as label, count(r) as total_nodes
        """

        stats_results = await self.neo4j.execute_query(stats_query)

        statistics = {}
        for result in stats_results:
            label = result.get("label", "unknown")
            count = result.get("total_nodes", 0)
            statistics[label] = count

        return GraphSchema(
            entity_types=entity_types,
            relationship_types=relationship_types,
            statistics=statistics,
        )

    async def get_entity_type_sample(
        self,
        entity_type: str,
        limit: int = 10,
    ) -> list[dict[str, Any]]:
        """Get sample entities of a specific type.

        Args:
            entity_type: Type of entity to sample
            limit: Number of samples to return

        Returns:
            List of sample entities with their properties
        """
        query = """
        MATCH (e:Entity {type: $entity_type})
        RETURN e
        LIMIT $limit
        """

        results = await self.neo4j.execute_query(
            query,
            {"entity_type": entity_type, "limit": limit},
        )

        samples = []
        for result in results:
            entity = result.get("e", {})
            samples.append({
                "id": entity.get("id", ""),
                "name": entity.get("name", ""),
                "type": entity.get("type", ""),
                "properties": entity,
            })

        return samples

    async def get_relationship_examples(
        self,
        relationship_type: str,
        limit: int = 10,
    ) -> list[dict[str, Any]]:
        """Get example relationships of a specific type.

        Args:
            relationship_type: Type of relationship
            limit: Number of examples to return

        Returns:
            List of relationship examples with source and target
        """
        query = f"""
        MATCH (a:Entity)-[r:{relationship_type}]->(b:Entity)
        RETURN a.name as from_name, 
               a.type as from_type,
               type(r) as rel_type,
               properties(r) as rel_properties,
               b.name as to_name,
               b.type as to_type
        LIMIT $limit
        """

        results = await self.neo4j.execute_query(
            query,
            {"limit": limit},
        )

        examples = []
        for result in results:
            examples.append({
                "from": {
                    "name": result.get("from_name", ""),
                    "type": result.get("from_type", ""),
                },
                "relationship": {
                    "type": result.get("rel_type", ""),
                    "properties": result.get("rel_properties", {}),
                },
                "to": {
                    "name": result.get("to_name", ""),
                    "type": result.get("to_type", ""),
                },
            })

        return examples

    async def suggest_schema_improvements(self) -> dict[str, Any]:
        """Analyze the schema and suggest improvements.

        Returns:
            Dictionary with suggestions for schema improvements
        """
        schema = await self.discover_schema()

        suggestions = {
            "missing_indexes": [],
            "duplicate_entities": [],
            "orphaned_nodes": [],
            "schema_anomalies": [],
        }

        # Check for entities with very similar names (potential duplicates)
        duplicate_query = """
        MATCH (e1:Entity), (e2:Entity)
        WHERE e1.name = e2.name AND e1.id <> e2.id
        RETURN e1.name as entity_name, count(*) as duplicate_count
        ORDER BY duplicate_count DESC
        LIMIT 10
        """

        duplicate_results = await self.neo4j.execute_query(duplicate_query)

        for result in duplicate_results:
            suggestions["duplicate_entities"].append({
                "name": result.get("entity_name", ""),
                "count": result.get("duplicate_count", 0),
            })

        # Check for orphaned nodes (entities with no relationships)
        orphan_query = """
        MATCH (e:Entity)
        WHERE NOT (e)-[]-()
        RETURN count(e) as orphan_count
        """

        orphan_results = await self.neo4j.execute_query(orphan_query)

        if orphan_results:
            orphan_count = orphan_results[0].get("orphan_count", 0)
            if orphan_count > 0:
                suggestions["orphaned_nodes"].append({
                    "message": f"Found {orphan_count} entities with no relationships",
                    "count": orphan_count,
                })

        # Schema anomalies: entity types with very few instances
        for entity_type in schema.entity_types:
            if entity_type["count"] < 5:
                suggestions["schema_anomalies"].append({
                    "message": f"Entity type '{entity_type['type']}' has very few instances ({entity_type['count']})",
                    "type": entity_type["type"],
                    "count": entity_type["count"],
                })

        return suggestions
