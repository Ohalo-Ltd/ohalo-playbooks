"""Utility functions for agent system."""

import re
import urllib.parse
from typing import Any


def build_entitlement_filter(user_email: str | None) -> str:
    """Build Cypher WHERE clause for entitlement filtering.

    Args:
        user_email: Current user's email address, or None for no filtering

    Returns:
        Cypher WHERE clause fragment for entitlement filtering
    """
    if not user_email:
        # No user context - return all documents (or based on project settings)
        return ""

    # Filter to only documents where:
    # 1. User is the owner, OR
    # 2. User is in the accessible_by_emails list, OR
    # 3. Document has no entitlement restrictions (owner_email is NULL)
    return f"""
    AND (
        doc.owner_email = '{user_email}'
        OR '{user_email}' IN COALESCE(doc.accessible_by_emails, [])
        OR doc.owner_email IS NULL
    )
    """


async def transform_document_links(
    text: str,
    neo4j_client: Any,
    dxr_url: str | None,
) -> str:
    """Transform internal document links to DXR search URLs.

    Converts links in format [Document Name](#/d/{document_id}) to DXR search links
    with proper URL encoding for document name filtering.

    Args:
        text: Text containing markdown links to transform
        neo4j_client: Neo4j client to fetch document names
        dxr_url: Base DXR URL (e.g., "https://leidos.dataxray.io")

    Returns:
        Text with transformed links
    """
    if not dxr_url:
        # No DXR URL configured, return text unchanged
        return text

    # Pattern to match [Document Name](#/d/{document_id})
    pattern = r"\[([^\]]+)\]\(#/d/([^\)]+)\)"

    # Find all document links
    matches = list(re.finditer(pattern, text))
    if not matches:
        return text

    # Extract unique document IDs
    doc_ids = list(set(match.group(2) for match in matches))

    # Fetch document names from Neo4j
    doc_id_to_name: dict[str, str] = {}
    if doc_ids:
        query = """
        MATCH (d:Document)
        WHERE d.id IN $doc_ids
        RETURN d.id as id, d.name as name
        """
        results = await neo4j_client.execute_query(query, {"doc_ids": doc_ids})
        for result in results:
            doc_id_to_name[result.get("id", "")] = result.get("name", "")

    # Replace each link
    result_text = text
    for match in reversed(matches):  # Reverse to preserve indices during replacement
        full_match = match.group(0)
        link_text = match.group(1)
        doc_id = match.group(2)

        # Get document name from database or use link text as fallback
        doc_name = doc_id_to_name.get(doc_id, link_text)

        # Build DXR search URL with file_name filter
        # Example: https://leidos.dataxray.io/search#query={"file_name":"Aircraft Procurement Army.pdf"}
        filter_query = {"file_name": doc_name}
        encoded_query = urllib.parse.quote(
            urllib.parse.quote(str(filter_query).replace("'", '"'))
        )
        dxr_link = f"{dxr_url.rstrip('/')}/search#query={encoded_query}"

        # Replace with new link
        new_link = f"[{link_text}]({dxr_link})"
        result_text = (
            result_text[: match.start()] + new_link + result_text[match.end() :]
        )

    return result_text
