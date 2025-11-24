"""Test API endpoints."""

import pytest
from fastapi.testclient import TestClient

from api.app import app


@pytest.fixture
def client() -> TestClient:
    """Test client."""
    return TestClient(app)


def test_root(client: TestClient) -> None:
    """Test root endpoint."""
    response = client.get("/")
    assert response.status_code == 200
    assert "message" in response.json()


def test_health(client: TestClient) -> None:
    """Test health endpoint."""
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "healthy"
