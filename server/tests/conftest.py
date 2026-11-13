"""server/tests/conftest.py

Pytest test harness and fixture configuration.
Configures FastAPI dependency overrides and service mocks to test API routes,
request validation, and HTTP responses in isolation without active database,
Azure Blob Storage, or Azure Storage Queue infrastructure.
"""

from unittest.mock import MagicMock
import pytest
from fastapi.testclient import TestClient

from app.controllers.upload_controller import get_upload_service
from app.services.upload_service import UploadService
from main import app


@pytest.fixture
def mock_upload_service() -> MagicMock:
    """Create a mock instance conforming to UploadService interface.

    Returns:
        MagicMock: A mock restricted to the attributes and methods of UploadService.
    """
    return MagicMock(spec=UploadService)


@pytest.fixture
def client(mock_upload_service: MagicMock):
    """Provide a configured TestClient with mocked service dependencies.

    Overrides the FastAPI dependency injector to replace real database and cloud
    operations with mock_upload_service. Restores original dependencies after
    test execution.

    Yields:
        TestClient: An HTTP test client bound to the configured FastAPI application.
    """
    # Swap out real dependency injection provider with test mock
    app.dependency_overrides[get_upload_service] = lambda: mock_upload_service

    with TestClient(app) as test_client:
        yield test_client

    # Clean up overrides to prevent cross-test state leakage
    app.dependency_overrides.clear()