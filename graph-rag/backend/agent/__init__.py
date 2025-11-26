"""Agent package."""

# Re-export main API components
from agent.agents import create_query_agent, query_agent
from agent.models import AgentDependencies, AgentStep, SearchResult
from agent.orchestrator import query, query_with_steps
from agent.prompts import DEFAULT_QUERY_SYSTEM_PROMPT
from agent.utils import build_entitlement_filter, transform_document_links

__all__ = [
    "query_agent",
    "create_query_agent",
    "query",
    "query_with_steps",
    "AgentDependencies",
    "AgentStep",
    "SearchResult",
    "DEFAULT_QUERY_SYSTEM_PROMPT",
    "build_entitlement_filter",
    "transform_document_links",
]
