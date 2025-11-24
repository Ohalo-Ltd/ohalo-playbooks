"""Test configuration."""

import pytest


@pytest.fixture
def sample_data() -> dict[str, str]:
    """Sample test data."""
    return {"test": "data"}
