"""Main query agent."""

from pydantic_ai import Agent

from agent.models import AgentDependencies
from agent.prompts import DEFAULT_QUERY_SYSTEM_PROMPT
from agent.tools import (
    decompose_query,
    discover_graph,
    entity_lookup,
    graph_neighbors,
    graph_query,
    vector_search,
)

# Define the query agent with all tools
query_agent = Agent(
    "openai:gpt-4o-mini",
    deps_type=AgentDependencies,
    system_prompt=DEFAULT_QUERY_SYSTEM_PROMPT,
    tools=[
        discover_graph,
        vector_search,
        entity_lookup,
        graph_neighbors,
        graph_query,
        decompose_query,
    ],
)


def create_query_agent(system_prompt: str | None = None) -> Agent[AgentDependencies, str]:
    """Create a query agent with optional custom system prompt.

    Args:
        system_prompt: Optional custom system prompt (uses default if not provided)

    Returns:
        Configured query agent
    """
    if not system_prompt:
        return query_agent

    # Create agent with custom prompt and register all tools
    custom_agent = Agent(
        "openai:gpt-4o-mini",
        deps_type=AgentDependencies,
        system_prompt=system_prompt,
        tools=[
            discover_graph,
            vector_search,
            entity_lookup,
            graph_neighbors,
            graph_query,
            decompose_query,
        ],
    )
    
    return custom_agent
