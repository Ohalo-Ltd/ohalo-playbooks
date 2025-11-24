"""Neo4j database client."""

from typing import Any, Optional

from neo4j import AsyncGraphDatabase, AsyncDriver

from core.config import settings


class Neo4jClient:
    """Client for Neo4j graph database."""

    def __init__(
        self,
        uri: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        database: str = "neo4j",
    ):
        """Initialize Neo4j client.

        Args:
            uri: Neo4j connection URI
            user: Username
            password: Password
            database: Database name (default: "neo4j")
        """
        self.uri = uri or settings.neo4j_uri
        self.user = user or settings.neo4j_user
        self.password = password or settings.neo4j_password
        self.database = database

        self.driver: Optional[AsyncDriver] = None

    async def __aenter__(self) -> "Neo4jClient":
        """Async context manager entry."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit."""
        await self.close()

    async def connect(self) -> None:
        """Establish connection to Neo4j."""
        self.driver = AsyncGraphDatabase.driver(
            self.uri,
            auth=(self.user, self.password),
        )
        # Verify connectivity
        await self.driver.verify_connectivity()

    async def close(self) -> None:
        """Close Neo4j connection."""
        if self.driver:
            await self.driver.close()

    async def execute_query(
        self, query: str, parameters: Optional[dict[str, Any]] = None
    ) -> list[dict[str, Any]]:
        """Execute a Cypher query.

        Args:
            query: Cypher query string
            parameters: Query parameters

        Returns:
            List of result records as dictionaries
        """
        if not self.driver:
            raise RuntimeError("Not connected to Neo4j")

        parameters = parameters or {}

        async with self.driver.session(database=self.database) as session:
            result = await session.run(query, parameters)
            records = await result.data()
            return records

    async def execute_write(
        self, query: str, parameters: Optional[dict[str, Any]] = None
    ) -> list[dict[str, Any]]:
        """Execute a write query in a transaction.

        Args:
            query: Cypher query string
            parameters: Query parameters

        Returns:
            List of result records
        """
        if not self.driver:
            raise RuntimeError("Not connected to Neo4j")

        parameters = parameters or {}

        async def _transaction(tx: Any) -> list[dict[str, Any]]:
            result = await tx.run(query, parameters)
            return await result.data()

        async with self.driver.session(database=self.database) as session:
            return await session.execute_write(_transaction)

    async def create_vector_index(
        self,
        index_name: str,
        label: str,
        property_name: str,
        dimensions: int = 1536,
        similarity_function: str = "cosine",
    ) -> None:
        """Create a vector index for similarity search.

        Args:
            index_name: Name of the index
            label: Node label to index
            property_name: Property containing the vector
            dimensions: Vector dimensions (default: 1536 for OpenAI)
            similarity_function: Similarity function (cosine, euclidean)
        """
        query = f"""
        CREATE VECTOR INDEX {index_name} IF NOT EXISTS
        FOR (n:{label})
        ON (n.{property_name})
        OPTIONS {{
            indexConfig: {{
                `vector.dimensions`: {dimensions},
                `vector.similarity_function`: '{similarity_function}'
            }}
        }}
        """
        await self.execute_write(query)

    async def create_full_text_index(
        self,
        index_name: str,
        labels: list[str],
        properties: list[str],
    ) -> None:
        """Create a full-text search index.

        Args:
            index_name: Name of the index
            labels: Node labels to index
            properties: Properties to include in index
        """
        labels_str = "|".join(labels)
        properties_str = ", ".join([f"n.{prop}" for prop in properties])

        query = f"""
        CREATE FULLTEXT INDEX {index_name} IF NOT EXISTS
        FOR (n:{labels_str})
        ON EACH [{properties_str}]
        """
        await self.execute_write(query)

    async def create_constraints(self, label: str, property_name: str) -> None:
        """Create uniqueness constraint on a property.

        Args:
            label: Node label
            property_name: Property name
        """
        query = f"""
        CREATE CONSTRAINT IF NOT EXISTS
        FOR (n:{label})
        REQUIRE n.{property_name} IS UNIQUE
        """
        await self.execute_write(query)

    async def initialize_graph_schema(self) -> None:
        """Initialize graph schema with indexes and constraints."""
        # Create constraints for entity IDs
        await self.create_constraints("Entity", "id")
        await self.create_constraints("Document", "id")
        await self.create_constraints("Chunk", "id")

        # Create vector index for chunks
        await self.create_vector_index(
            index_name="chunk_embeddings",
            label="Chunk",
            property_name="embedding",
            dimensions=1536,
        )

        # Create full-text index for search
        await self.create_full_text_index(
            index_name="entity_fulltext",
            labels=["Entity"],
            properties=["name", "description"],
        )

    async def vector_search(
        self,
        embedding: list[float],
        label: str = "Chunk",
        property_name: str = "embedding",
        top_k: int = 10,
    ) -> list[dict[str, Any]]:
        """Perform vector similarity search.

        Args:
            embedding: Query embedding vector
            label: Node label to search
            property_name: Property containing embeddings
            top_k: Number of results to return

        Returns:
            List of similar nodes with scores
        """
        query = f"""
        CALL db.index.vector.queryNodes(
            'chunk_embeddings',
            $top_k,
            $embedding
        )
        YIELD node, score
        RETURN node, score
        ORDER BY score DESC
        LIMIT $top_k
        """

        results = await self.execute_query(
            query,
            {"embedding": embedding, "top_k": top_k},
        )

        return results

    async def entity_lookup(
        self,
        entity_name: str,
        fuzzy: bool = False,
    ) -> list[dict[str, Any]]:
        """Look up entities by name.

        Args:
            entity_name: Name to search for
            fuzzy: If True, use fuzzy matching (CONTAINS), else exact match

        Returns:
            List of matching entity nodes
        """
        if fuzzy:
            query = """
            MATCH (e:Entity)
            WHERE toLower(e.name) CONTAINS toLower($entity_name)
            RETURN e, id(e) as node_id
            LIMIT 10
            """
        else:
            query = """
            MATCH (e:Entity {name: $entity_name})
            RETURN e, id(e) as node_id
            """

        results = await self.execute_query(query, {"entity_name": entity_name})
        return results

    async def get_neighbors(
        self,
        node_id: str,
        relationship_types: Optional[list[str]] = None,
        direction: str = "both",
        max_depth: int = 1,
    ) -> list[dict[str, Any]]:
        """Get neighboring nodes connected by relationships.

        Args:
            node_id: Starting node ID
            relationship_types: List of relationship types to traverse (None = all)
            direction: Direction to traverse ("outgoing", "incoming", "both")
            max_depth: Maximum traversal depth (1-3 recommended)

        Returns:
            List of connected nodes with relationship info
        """
        # Build relationship pattern based on direction
        if relationship_types:
            rel_types = "|".join(relationship_types)
            rel_pattern = f"[r:{rel_types}]"
        else:
            rel_pattern = "[r]"

        if direction == "outgoing":
            pattern = f"-{rel_pattern}->"
        elif direction == "incoming":
            pattern = f"<-{rel_pattern}-"
        else:  # both
            pattern = f"-{rel_pattern}-"

        # Limit max depth to prevent expensive queries
        max_depth = min(max_depth, 3)

        query = f"""
        MATCH (start {{id: $node_id}})
        MATCH path = (start){pattern}*1..{max_depth}(neighbor)
        WITH neighbor, relationships(path) as rels, length(path) as depth
        RETURN DISTINCT neighbor, 
               [rel in rels | {{{{type: type(rel), properties: properties(rel)}}}}] as relationships,
               depth
        ORDER BY depth
        LIMIT 50
        """

        results = await self.execute_query(query, {"node_id": node_id})
        return results

    async def traverse_relationships(
        self,
        start_node_id: str,
        relationship_pattern: str,
        end_node_label: Optional[str] = None,
        max_results: int = 20,
    ) -> list[dict[str, Any]]:
        """Traverse relationships following a specific pattern.

        Args:
            start_node_id: Starting node ID
            relationship_pattern: Cypher relationship pattern (e.g., "MENTIONS", "LOCATED_IN")
            end_node_label: Optional label for end nodes
            max_results: Maximum number of results

        Returns:
            List of paths with nodes and relationships
        """
        end_label = f":{end_node_label}" if end_node_label else ""

        query = f"""
        MATCH (start {{id: $start_node_id}})
        MATCH path = (start)-[r:{relationship_pattern}]->(end{end_label})
        RETURN start, r, end, 
               properties(r) as rel_properties,
               labels(end) as end_labels
        LIMIT $max_results
        """

        results = await self.execute_query(
            query,
            {"start_node_id": start_node_id, "max_results": max_results},
        )
        return results

    async def expand_context_from_chunks(
        self,
        chunk_ids: list[str],
        expansion_depth: int = 1,
    ) -> dict[str, Any]:
        """Expand context from chunks by traversing to related entities.

        Args:
            chunk_ids: List of chunk IDs from vector search
            expansion_depth: How many hops to traverse (1-2 recommended)

        Returns:
            Dictionary with chunks, entities, relationships
        """
        query = """
        MATCH (c:Chunk)
        WHERE c.id IN $chunk_ids
        
        // Get the document
        OPTIONAL MATCH (c)<-[:HAS_CHUNK]-(doc:Document)
        
        // Get mentioned entities
        OPTIONAL MATCH (c)-[:MENTIONS]->(entity:Entity)
        
        // Get entity relationships (1-2 hops)
        OPTIONAL MATCH path = (entity)-[r*1..2]-(related:Entity)
        
        WITH c, doc, 
             collect(DISTINCT entity) as entities,
             collect(DISTINCT {
                 from: startNode(relationships(path)[0]).name,
                 to: endNode(relationships(path)[0]).name,
                 type: type(relationships(path)[0])
             }) as relationships
        
        RETURN {
            chunks: collect(DISTINCT {
                id: c.id,
                text: c.text,
                document: doc.name
            }),
            entities: collect(DISTINCT entities),
            relationships: collect(DISTINCT relationships)
        } as context
        """

        results = await self.execute_query(query, {"chunk_ids": chunk_ids})

        if results:
            return results[0].get("context", {})
        return {"chunks": [], "entities": [], "relationships": []}

    async def execute_safe_cypher(
        self,
        cypher_query: str,
        parameters: Optional[dict[str, Any]] = None,
    ) -> list[dict[str, Any]]:
        """Execute a Cypher query with safety checks.

        Args:
            cypher_query: Cypher query to execute
            parameters: Query parameters

        Returns:
            Query results

        Raises:
            ValueError: If query contains unsafe operations
        """
        # Disallow write operations
        unsafe_keywords = ["CREATE", "DELETE", "REMOVE", "SET", "MERGE", "DROP"]
        query_upper = cypher_query.upper()

        for keyword in unsafe_keywords:
            if keyword in query_upper:
                raise ValueError(
                    f"Unsafe operation '{keyword}' not allowed in agent queries"
                )

        # Limit result size
        if "LIMIT" not in query_upper:
            cypher_query += " LIMIT 100"

        return await self.execute_query(cypher_query, parameters)
