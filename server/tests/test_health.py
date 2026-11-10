"""server/tests/test_health.py

Unit and liveness tests for the health check controller.
Validates HTTP response codes and verification payloads returned by the /health endpoint.
"""

from fastapi.testclient import TestClient

from main import app

# Create a local test client instance bound to the FastAPI application
client = TestClient(app)


def test_health_check_returns_200():
    """Verify GET /health returns HTTP 200 OK and confirms operational status."""
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json()["status"] == "healthy"
    assert response.json()["service"] == "marathon-pulse-api"
    assert "timestamp" in response.json()