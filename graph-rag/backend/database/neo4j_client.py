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
