"""DXR metadata parser for extracting entities and relationships."""

from typing import Any, Optional

from pydantic import BaseModel


class ExtractedEntity(BaseModel):
    """Extracted entity from DXR metadata."""

    id: str
    type: str
    properties: dict[str, Any]


class ExtractedRelationship(BaseModel):
    """Extracted relationship from DXR metadata."""

    from_id: str
    to_id: str
    type: str
    properties: dict[str, Any] = {}


class ParsedMetadata(BaseModel):
    """Parsed DXR metadata."""

    entities: list[ExtractedEntity]
    relationships: list[ExtractedRelationship]


class DXRMetadataParser:
    """Parser for DXR extracted metadata."""

    def __init__(self, extractor_id: str = "default"):
        """Initialize metadata parser.

        Args:
            extractor_id: ID of the DXR extractor to use
        """
        self.extractor_id = extractor_id

    def parse(self, metadata: dict[str, Any]) -> ParsedMetadata:
        """Parse DXR metadata to extract entities and relationships.

        The metadata should be in the format:
        {
            "extracted_metadata": {
                "EXTRACTOR_ID": {
                    "nodes": [
                        {
                            "id": "entity_1",
                            "type": "Contract",
                            "properties": {...}
                        }
                    ],
                    "relationships": [
                        {
                            "from": "entity_1",
                            "to": "entity_2",
                            "type": "SIGNED_BY",
                            "properties": {...}
                        }
                    ]
                }
            }
        }

        Args:
            metadata: DXR metadata dictionary

        Returns:
            Parsed entities and relationships
        """
        entities: list[ExtractedEntity] = []
        relationships: list[ExtractedRelationship] = []

        # Get extractor data
        extractor_data = metadata.get(self.extractor_id, {})

        # Parse nodes/entities
        nodes = extractor_data.get("nodes", [])
        for node in nodes:
            if not isinstance(node, dict):
                continue

            node_id = node.get("id")
            node_type = node.get("type")

            if not node_id or not node_type:
                continue

            properties = node.get("properties", {})
            if not isinstance(properties, dict):
                properties = {}

            entities.append(
                ExtractedEntity(
                    id=node_id,
                    type=node_type,
                    properties=properties,
                )
            )

        # Parse relationships
        rels = extractor_data.get("relationships", [])
        for rel in rels:
            if not isinstance(rel, dict):
                continue

            from_id = rel.get("from")
            to_id = rel.get("to")
            rel_type = rel.get("type")

            if not from_id or not to_id or not rel_type:
                continue

            properties = rel.get("properties", {})
            if not isinstance(properties, dict):
                properties = {}

            relationships.append(
                ExtractedRelationship(
                    from_id=from_id,
                    to_id=to_id,
                    type=rel_type,
                    properties=properties,
                )
            )

        return ParsedMetadata(entities=entities, relationships=relationships)

    def parse_from_dxr_file(
        self,
        extracted_metadata: list[dict[str, Any]],
        extractor_id: Optional[str] = None,
    ) -> Optional[ParsedMetadata]:
        """Parse metadata from a DXR file object.

        Args:
            extracted_metadata: The extractedMetadata field from DXR file (list of metadata objects)
            extractor_id: Optional specific extractor ID to use (e.g., "11" or "extracted_metadata#11")

        Returns:
            Parsed metadata or None if parsing fails
        """
        if not extracted_metadata:
            return None

        try:
            # If extractor_id is specified, find that specific metadata item
            if extractor_id:
                # Handle both "11" and "extracted_metadata#11" formats
                extractor_num = extractor_id.replace("extracted_metadata#", "").strip()

                for item in extracted_metadata:
                    item_id = str(item.get("id", ""))
                    if item_id == extractor_id or item_id == extractor_num:
                        # Found the specific extractor - parse its value
                        value = item.get("value")
                        if isinstance(value, str):
                            # Try to parse as JSON
                            import json

                            try:
                                value = json.loads(value)
                            except json.JSONDecodeError:
                                pass

                        if isinstance(value, dict):
                            return self.parse(value)
                        else:
                            print(
                                f"Extractor {extractor_id} value is not a dict/JSON object"
                            )
                            return None

                print(f"Warning: Extractor {extractor_id} not found in metadata")
                return None

            # No specific extractor - try to find any JSON metadata
            for item in extracted_metadata:
                value = item.get("value")
                if isinstance(value, str):
                    # Try to parse as JSON
                    import json

                    try:
                        value = json.loads(value)
                    except json.JSONDecodeError:
                        continue

                if isinstance(value, dict):
                    # Found a JSON object - use it
                    return self.parse(value)

            # No JSON metadata found
            return None

        except Exception as e:
            # Log error but don't fail
            print(f"Failed to parse metadata: {e}")
            return None
