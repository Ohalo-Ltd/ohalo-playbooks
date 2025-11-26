"""Query decomposition tool."""

import json

from pydantic_ai import RunContext

from agent.models import AgentDependencies, AgentStep


async def decompose_query(
    ctx: RunContext[AgentDependencies],
    query: str,
) -> list[str]:
    """Decompose a complex query into multiple search queries.

    Use this tool when:
    - The user's question is complex or ambiguous
    - The question covers multiple topics
    - A direct search might miss relevant context
    - You want to broaden the search scope

    Args:
        ctx: Runtime context
        query: The user's original query

    Returns:
        List of search queries to try
    """
    # Import here to avoid circular dependency
    from agent.agents.decomposition import decomposition_agent

    # Emit tool start event
    if ctx.deps.step_callback:
        await ctx.deps.step_callback(
            AgentStep(
                type="tool_call_start",
                tool="decompose_query",
                args={
                    "query": query,
                    "description": "Decomposing query into multiple search variations...",
                },
            )
        )

    try:
        result = await decomposition_agent.run(f"User question:\n{query}")
        # Parse JSON array from response
        
        # Clean up response if it contains markdown code blocks
        content = getattr(result, "data", None)
        if content is None:
            content = getattr(result, "output", str(result))
            
        if "```json" in content:
            content = content.split("```json")[1].split("```")[0].strip()
        elif "```" in content:
            content = content.split("```")[1].split("```")[0].strip()
            
        queries = json.loads(content)
        
        # Emit tool result event
        if ctx.deps.step_callback:
            await ctx.deps.step_callback(
                AgentStep(
                    type="tool_call_result",
                    tool="decompose_query",
                    result={"queries": queries},
                )
            )
            
        return queries
    except Exception as e:
        # Fallback to original query if decomposition fails
        if ctx.deps.step_callback:
            await ctx.deps.step_callback(
                AgentStep(
                    type="error",
                    message=f"Decomposition failed: {str(e)}",
                )
            )
        return [query]
