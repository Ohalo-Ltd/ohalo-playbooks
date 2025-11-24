"""API endpoints for document management."""

from typing import Any, Dict, AsyncGenerator

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from database.neo4j_client import Neo4jClient

router = APIRouter(prefix="/api/documents", tags=["documents"])


class DocumentResponse(BaseModel):
    """Document response model."""

    id: str
    name: str
    properties: Dict[str, Any]


async def get_neo4j_client() -> AsyncGenerator[Neo4jClient, None]:
    """Get Neo4j client dependency."""
    client = Neo4jClient()
    await client.connect()
    try:
        yield client
    finally:
        await client.close()


@router.get("/{document_id}", response_model=DocumentResponse)
async def get_document(
    document_id: str,
    client: Neo4jClient = Depends(get_neo4j_client),
) -> DocumentResponse:
    """Get document details by ID.

    Args:
        document_id: Document ID
        client: Neo4j client

    Returns:
        Document details
    """
    query = """
    MATCH (d:Document {id: $document_id})
    RETURN d
    """

    results = await client.execute_query(query, {"document_id": document_id})

    if not results:
        raise HTTPException(status_code=404, detail="Document not found")

    node = results[0]["d"]
    # Convert Neo4j Node to dict
    properties = dict(node)
    name = properties.pop("name", "Untitled")
    
    return DocumentResponse(
        id=document_id,
        name=name,
        properties=properties,
    )
