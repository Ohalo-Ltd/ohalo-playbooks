"""Main orchestration functions for running agents."""

import asyncio
from collections.abc import AsyncIterator
from typing import Any

from pydantic_ai.messages import ModelMessage, ModelRequest, ModelResponse, TextPart, UserPromptPart

from agent.agents import create_query_agent, query_agent
from agent.models import AgentDependencies, AgentStep
from database.neo4j_client import Neo4jClient
from ingestion.embedder import EmbeddingService


async def query(
    question: str,
    neo4j_client: Neo4jClient,
    embedding_service: EmbeddingService,
    project_id: str,
    system_prompt: str | None = None,
    current_user_email: str | None = None,
    dxr_url: str | None = None,
) -> str:
    """Query the knowledge graph.

    Args:
        question: User question
        neo4j_client: Neo4j client instance
        embedding_service: Embedding service instance
        project_id: Project ID
        system_prompt: Optional custom system prompt (uses default if not provided)
        current_user_email: Optional user email for entitlement filtering
        dxr_url: Optional DXR base URL for document link transformation

    Returns:
        Answer from the agent
    """
    deps = AgentDependencies(
        neo4j_client=neo4j_client,
        embedding_service=embedding_service,
        project_id=project_id,
        system_prompt=system_prompt,
        current_user_email=current_user_email,
        dxr_url=dxr_url,
    )

    # Use appropriate agent
    agent = create_query_agent(system_prompt) if system_prompt else query_agent
    
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
    dxr_url: str | None = None,
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
        dxr_url: Optional DXR base URL for document link transformation

    Yields:
        Agent step events (tool calls, results, final answer)
    """
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
        dxr_url=dxr_url,
    )

    # Use appropriate agent
    agent = create_query_agent(system_prompt) if system_prompt else query_agent

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
