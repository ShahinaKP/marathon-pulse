"""server/tests/test_upload.py

Integration and route validation tests for file ingestion and status polling endpoints.
Validates file extension restrictions, asynchronous job dispatch semantics,
and status progress polling using mocked application service layers.
"""

import io
import pytest


def test_upload_rejects_non_excel(client):
    """Verify that uploading non-Excel file types (e.g., CSV) returns HTTP 400 Bad Request."""
    files = {"file": ("test.csv", io.BytesIO(b"name,email\nJohn,j@a.com"), "text/csv")}
    response = client.post("/api/upload", files=files)

    assert response.status_code == 400
    assert "Invalid file format" in response.json()["detail"]


def test_streaming_upload_accepted(client, mock_upload_service):
    """Verify that valid .xlsx uploads return HTTP 202 Accepted with a queued job payload."""
    mock_upload_service.process_streaming_upload.return_value = {
        "job_id": "test-uuid-1234",
        "status": "QUEUED",
        "message": "File accepted for processing.",
    }
    file_bytes = b"PK\x03\x04fake_excel_bytes"
    files = {
        "file": (
            "runners.xlsx",
            io.BytesIO(file_bytes),
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    }

    response = client.post("/api/upload", files=files)

    assert response.status_code == 202
    assert response.json()["job_id"] == "test-uuid-1234"
    assert response.json()["status"] == "QUEUED"


def test_status_polling(client, mock_upload_service):
    """Verify that polling /api/status/{job_id} returns HTTP 200 with progress metrics."""
    mock_upload_service.get_job_status.return_value = {
        "job_id": "test-uuid-1234",
        "status": "PROCESSING",
        "total_rows": 10000,
        "processed_rows": 4500,
        "errors": [],
    }

    response = client.get("/api/status/test-uuid-1234")

    assert response.status_code == 200
    assert response.json()["status"] == "PROCESSING"
    assert response.json()["processed_rows"] == 4500