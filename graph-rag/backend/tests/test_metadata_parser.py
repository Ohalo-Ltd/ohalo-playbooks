from ingestion.metadata_parser import DXRMetadataParser

def test_parse_valid_metadata():
    parser = DXRMetadataParser(extractor_id="test_extractor")
    metadata = {
        "test_extractor": {
            "nodes": [
                {
                    "id": "e1",
                    "type": "Person",
                    "properties": {"name": "Alice"}
                }
            ],
            "relationships": [
                {
                    "from": "e1",
                    "to": "e2",
                    "type": "KNOWS",
                    "properties": {"since": 2020}
                }
            ]
        }
    }
    
    result = parser.parse(metadata)
    
    assert len(result.entities) == 1
    assert result.entities[0].id == "e1"
    assert result.entities[0].type == "Person"
    assert result.entities[0].properties["name"] == "Alice"
    
    assert len(result.relationships) == 1
    assert result.relationships[0].from_id == "e1"
    assert result.relationships[0].to_id == "e2"
    assert result.relationships[0].type == "KNOWS"
    assert result.relationships[0].properties["since"] == 2020

def test_parse_empty_metadata():
    parser = DXRMetadataParser()
    result = parser.parse({})
    assert len(result.entities) == 0
    assert len(result.relationships) == 0

def test_parse_invalid_nodes():
    parser = DXRMetadataParser(extractor_id="test")
    metadata = {
        "test": {
            "nodes": [
                {"id": "e1"}, # Missing type
                {"type": "Person"}, # Missing id
                "invalid" # Not a dict
            ]
        }
    }
    result = parser.parse(metadata)
    assert len(result.entities) == 0
