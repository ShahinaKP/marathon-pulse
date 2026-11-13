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


def test_upload_rejects_legacy_xls(client):
    """.xls is not readable by openpyxl, so it must be rejected at the API."""
    files = {"file": ("old.xls", io.BytesIO(b"\xd0\xcf\x11\xe0"), "application/vnd.ms-excel")}
    response = client.post("/api/upload", files=files)

    assert response.status_code == 400
    

def test_streaming_upload_accepted(client, mock_upload_service):
    """Verify that valid .xlsx uploads return HTTP 202 Accepted with a queued job payload."""
    mock_upload_service.process_upload.return_value = {
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

# --- UploadService unit tests (real service, fake cloud dependencies) --------

from unittest.mock import MagicMock, patch  # noqa: E402

from fastapi import HTTPException  # noqa: E402

from app.services.upload_service import UploadService  # noqa: E402


def _service():
    return UploadService(blob_service=MagicMock(), queue_service=MagicMock())


def test_service_rejects_non_xlsx_content():
    """A renamed non-workbook (no zip magic bytes) is refused before any cloud call."""
    svc = _service()
    with pytest.raises(HTTPException) as exc:
        svc.process_upload("fake.xlsx", io.BytesIO(b"not a workbook at all"))
    assert exc.value.status_code == 400
    svc.blob_service.upload_stream.assert_not_called()


def test_service_marks_job_failed_when_enqueue_fails():
    """If the queue is down the job must not stay QUEUED forever."""
    svc = _service()
    svc.queue_service.enqueue_job.side_effect = RuntimeError("queue down")
    with patch("app.services.upload_service.get_db_connection"), patch.object(
        UploadService, "_mark_failed"
    ) as mark_failed:
        with pytest.raises(HTTPException) as exc:
            svc.process_upload("runners.xlsx", io.BytesIO(b"PK\x03\x04" + b"x" * 100))
    assert exc.value.status_code == 503
    mark_failed.assert_called_once()


def test_service_marks_job_failed_when_blob_upload_fails():
    svc = _service()
    svc.blob_service.upload_stream.side_effect = RuntimeError("storage down")
    with patch("app.services.upload_service.get_db_connection"), patch.object(
        UploadService, "_mark_failed"
    ) as mark_failed:
        with pytest.raises(HTTPException) as exc:
            svc.process_upload("runners.xlsx", io.BytesIO(b"PK\x03\x04" + b"x" * 100))
    assert exc.value.status_code == 502
    mark_failed.assert_called_once()
    svc.queue_service.enqueue_job.assert_not_called()
