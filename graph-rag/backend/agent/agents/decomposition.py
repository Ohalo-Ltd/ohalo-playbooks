"""Decomposition agent for breaking down complex queries."""

from pydantic_ai import Agent

from agent.prompts import DECOMPOSITION_SYSTEM_PROMPT

decomposition_agent = Agent(
    "openai:gpt-4o-mini",
    system_prompt=DECOMPOSITION_SYSTEM_PROMPT,
)
