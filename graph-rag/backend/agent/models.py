"""Shared models for agent system."""

from typing import Any

from pydantic import BaseModel


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

    neo4j_client: Any  # Neo4jClient
    embedding_service: Any  # EmbeddingService
    project_id: str
    system_prompt: str | None = None
    step_callback: Any | None = None  # Async callback for streaming steps
    current_user_email: str | None = None  # Current user's email for entitlement filtering
    dxr_url: str | None = None  # DXR base URL for document link transformation

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
