"""Graph writer service for writing entities and relationships to Neo4j."""

import logging
from typing import Any, Optional
from uuid import uuid4

from database.neo4j_client import Neo4jClient
from ingestion.metadata_parser import ExtractedEntity, ExtractedRelationship

logger = logging.getLogger(__name__)


class GraphWriter:
    """Service for writing graph data to Neo4j."""

    def __init__(self, neo4j_client: Neo4jClient):
        """Initialize graph writer.

        Args:
            neo4j_client: Neo4j client instance
        """
        self.client = neo4j_client

    async def merge_entity(
        self,
        entity: ExtractedEntity,
        embedding: Optional[list[float]] = None,
    ) -> str:
        """Merge an entity into the graph (create or update).

        Args:
            entity: Entity to merge
            embedding: Optional embedding vector

        Returns:
            Entity node ID in Neo4j
        """
        # Prepare properties
        properties = dict(entity.properties)
        properties["id"] = entity.id
        properties["type"] = entity.type

        if embedding:
            properties["embedding"] = embedding

        # Build SET clause for all properties
        set_clauses = [f"e.{k} = ${k}" for k in properties.keys()]
        set_clause = ", ".join(set_clauses)

        # Sanitize entity type for label (remove spaces, special chars)
        entity_label = entity.type.replace(" ", "_").replace("-", "_")

        query = f"""
        MERGE (e:Entity {{id: $id}})
        SET {set_clause}
        SET e:{entity_label}
        RETURN elementId(e) as node_id
        """

        result = await self.client.execute_write(query, properties)

        if result:
            return result[0]["node_id"]
        return entity.id

    async def create_relationship(
        self,
        relationship: ExtractedRelationship,
    ) -> None:
        """Create a relationship between entities.

        Args:
            relationship: Relationship to create
        """
        properties = relationship.properties

        query = f"""
        MATCH (from:Entity {{id: $from_id}})
        MATCH (to:Entity {{id: $to_id}})
        MERGE (from)-[r:{relationship.type}]->(to)
        SET r += $properties
        """

        await self.client.execute_write(
            query,
            {
                "from_id": relationship.from_id,
                "to_id": relationship.to_id,
                "properties": properties,
            },
        )

    async def create_document_node(
        self,
        document_id: str,
        name: str,
        properties: Optional[dict[str, Any]] = None,
        project_id: Optional[str] = None,
    ) -> str:
        """Create a document node.

        Args:
            document_id: Document ID
            name: Document name
            properties: Document properties
            project_id: Project ID

        Returns:
            Document node ID
        """
        props = properties or {}
        props["id"] = document_id
        props["name"] = name
        if project_id:
            props["project_id"] = project_id

        logger.debug(
            f"Creating/updating document node {document_id} with properties: {list(props.keys())}"
        )

        # Build SET clause dynamically based on available properties
        set_clauses = []
        for key in props.keys():
            if key != "id":  # Don't set id again, it's used in MERGE
                set_clauses.append(f"d.{key} = $props.{key}")

        set_clause = (
            ", ".join(set_clauses) if set_clauses else "d.id = d.id"
        )  # No-op if no properties

        query = f"""
        MERGE (d:Document {{id: $props.id}})
        SET {set_clause}
        RETURN elementId(d) as node_id
        """

        result = await self.client.execute_write(
            query,
            {"props": props},
        )

        if result:
            return result[0]["node_id"]
        return document_id

    async def create_chunk_node(
        self,
        chunk_id: Optional[str],
        document_id: str,
        text: str,
        embedding: list[float],
        chunk_index: int = 0,
        properties: Optional[dict[str, Any]] = None,
    ) -> str:
        """Create a text chunk node with embedding.

        Args:
            chunk_id: Chunk ID (auto-generated if None)
            document_id: Parent document ID
            text: Chunk text
            embedding: Embedding vector
            chunk_index: Index of chunk in document
            properties: Additional properties

        Returns:
            Chunk node ID
        """
        chunk_id = chunk_id or str(uuid4())
        props = properties or {}
        props.update({
            "id": chunk_id,
            "text": text,
            "embedding": embedding,
            "chunk_index": chunk_index,
        })

        query = """
        MERGE (c:Chunk {id: $chunk_id})
        SET c += $properties
        WITH c
        MATCH (d:Document {id: $document_id})
        MERGE (d)-[:HAS_CHUNK]->(c)
        RETURN elementId(c) as node_id
        """

        result = await self.client.execute_write(
            query,
            {
                "chunk_id": chunk_id,
                "document_id": document_id,
                "properties": props,
            },
        )

        if result:
            return result[0]["node_id"]
        return chunk_id

    async def link_entity_to_document(
        self,
        entity_id: str,
        document_id: str,
    ) -> None:
        """Create a link between an entity and its source document.

        Args:
            entity_id: Entity ID
            document_id: Document ID
        """
        query = """
        MATCH (e:Entity {id: $entity_id})
        MATCH (d:Document {id: $document_id})
        MERGE (e)-[:MENTIONED_IN]->(d)
        SET e.source_file_ids = CASE 
            WHEN e.source_file_ids IS NULL THEN [$document_id] 
            WHEN NOT $document_id IN e.source_file_ids THEN e.source_file_ids + $document_id 
            ELSE e.source_file_ids 
        END
        """

        await self.client.execute_write(
            query,
            {"entity_id": entity_id, "document_id": document_id},
        )

    async def batch_merge_entities(
        self,
        entities: list[tuple[ExtractedEntity, Optional[list[float]]]],
    ) -> list[str]:
        """Batch merge multiple entities.

        Args:
            entities: List of (entity, embedding) tuples

        Returns:
            List of entity IDs
        """
        entity_ids: list[str] = []

        for entity, embedding in entities:
            entity_id = await self.merge_entity(entity, embedding)
            entity_ids.append(entity_id)

        return entity_ids

    async def batch_create_relationships(
        self,
        relationships: list[ExtractedRelationship],
    ) -> None:
        """Batch create multiple relationships.

        Args:
            relationships: List of relationships
        """
        for relationship in relationships:
            await self.create_relationship(relationship)
