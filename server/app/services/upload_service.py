"""server/app/services/upload_service.py

Ingestion and job tracking for marathon data uploads.

Flow (all synchronous; the controller runs it in a thread pool so the event
loop is never blocked):
    1. validate the file (.xlsx magic bytes, size cap)
    2. insert the job row            -> status QUEUED
    3. stream the raw file to Blob Storage
    4. publish {job_id, blob_name} to the Storage Queue
The API returns right after step 4 - it never parses the Excel file. Row-level
duplicate detection (email + bib number) happens later, in the worker, via the
database unique constraint.
"""

import logging
import uuid
from typing import Any, BinaryIO, Dict

from fastapi import HTTPException, status

from app.config import settings
from app.database import get_db_connection
from app.services.blob_service import BlobService
from app.services.queue_service import QueueService

logger = logging.getLogger("upload_service")

XLSX_MAGIC = b"PK\x03\x04"  # .xlsx is a zip container


class UploadService:
    """Coordinates file ingestion and job state tracking."""

    def __init__(self, blob_service: BlobService, queue_service: QueueService) -> None:
        self.blob_service = blob_service
        self.queue_service = queue_service

    # ------------------------------------------------------------------ upload
    def process_upload(self, filename: str, fileobj: BinaryIO) -> Dict[str, Any]:
        """Validate, register, store and enqueue one uploaded workbook."""
        self._validate_file(fileobj)

        job_id = str(uuid.uuid4())
        blob_name = f"{job_id}.xlsx"

        # 2. Register the job first so every later failure can be recorded against it.
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO upload_jobs (id, file_name, blob_name, status)
                    VALUES (%s, %s, %s, 'QUEUED')
                    """,
                    (job_id, filename[:255], blob_name),
                )
            conn.commit()

        # 3. Raw file -> Blob Storage
        try:
            self.blob_service.upload_stream(blob_name, fileobj)
        except Exception as exc:  # noqa: BLE001 - any storage failure fails the job
            logger.exception("Blob upload failed for job %s", job_id)
            self._mark_failed(job_id, f"File storage failed: {exc}")
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail="Could not store the uploaded file. Please try again.",
            ) from exc

        # 4. Job notification -> Storage Queue (this is what wakes the worker)
        try:
            self.queue_service.enqueue_job(job_id=job_id, blob_name=blob_name)
        except Exception as exc:  # noqa: BLE001
            logger.exception("Enqueue failed for job %s", job_id)
            self._mark_failed(job_id, f"Could not queue the job: {exc}")
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Could not queue the file for processing. Please try again.",
            ) from exc

        return {
            "job_id": job_id,
            "status": "QUEUED",
            "message": "File accepted for processing.",
        }

    @staticmethod
    def _validate_file(fileobj: BinaryIO) -> None:
        head = fileobj.read(4)
        if head != XLSX_MAGIC:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="File is not a valid .xlsx workbook.",
            )
        fileobj.seek(0, 2)  # end
        size = fileobj.tell()
        fileobj.seek(0)
        limit = settings.MAX_UPLOAD_MB * 1024 * 1024
        if size > limit:
            raise HTTPException(
                status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                detail=f"File too large. Maximum size is {settings.MAX_UPLOAD_MB} MB.",
            )
        if size == 0:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="File is empty.")

    @staticmethod
    def _mark_failed(job_id: str, message: str) -> None:
        """Best-effort: record why a job failed before it ever reached the worker."""
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE upload_jobs
                        SET status = 'FAILED', error_message = %s,
                            updated_at = NOW(), completed_at = NOW()
                        WHERE id = %s
                        """,
                        (message[:1000], job_id),
                    )
                conn.commit()
        except Exception:  # noqa: BLE001
            logger.exception("Could not mark job %s as FAILED", job_id)

    # ------------------------------------------------------------------ status
    def get_job_status(self, job_id: str) -> Dict[str, Any]:
        """Return progress counters and up to 100 row-level errors for a job."""
        try:
            uuid.UUID(job_id)
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Job with ID '{job_id}' not found.",
            )

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT status, total_rows, processed_rows, inserted_rows,
                           duplicate_rows, invalid_rows, error_message,
                           created_at, updated_at
                    FROM upload_jobs WHERE id = %s
                    """,
                    (job_id,),
                )
                job = cur.fetchone()
                if not job:
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail=f"Job with ID '{job_id}' not found.",
                    )

                cur.execute(
                    """
                    SELECT row_number, error_message FROM job_errors
                    WHERE job_id = %s ORDER BY row_number ASC LIMIT 100
                    """,
                    (job_id,),
                )
                errors = [{"row": r[0], "message": r[1]} for r in cur.fetchall()]

        return {
            "job_id": job_id,
            "status": job[0],
            "total_rows": job[1] or 0,
            "processed_rows": job[2] or 0,
            "inserted_rows": job[3] or 0,
            "duplicate_rows": job[4] or 0,
            "invalid_rows": job[5] or 0,
            "error_message": job[6],
            "created_at": job[7].isoformat() if job[7] else None,
            "updated_at": job[8].isoformat() if job[8] else None,
            "errors": errors,
        }
