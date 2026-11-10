"""server/app/services/upload_service.py

Ingestion and job tracking service for marathon data uploads.
Orchestrates streaming file reads, incremental SHA-256 deduplication,
cloud blob staging, database state creation, message dispatch, and status polling.
"""

import hashlib
import tempfile
import uuid
from typing import Any, Dict

from fastapi import HTTPException, UploadFile, status

from app.database import get_db_connection
from app.services.blob_service import BlobService
from app.services.queue_service import QueueService


class UploadService:
    """Coordinates file ingestion pipelines and job state tracking."""

    def __init__(self, blob_service: BlobService, queue_service: QueueService) -> None:
        """Inject required cloud storage and queueing dependencies.

        Args:
            blob_service: Service instance handling Azure Blob Storage operations.
            queue_service: Service instance handling message publishing to Azure Service Bus.
        """
        self.blob_service = blob_service
        self.queue_service = queue_service

    async def process_streaming_upload(self, file: UploadFile) -> Dict[str, Any]:
        """Stream an uploaded file to blob storage and register an asynchronous processing job.

        Streams data in 1 MB chunks to manage memory footprint, calculates an
        incremental SHA-256 hash for deduplication, uploads to Azure Blob Storage,
        persists the initial job record in PostgreSQL, and enqueues a job notification.

        Args:
            file: FastAPI UploadFile received from client HTTP request.

        Returns:
            dict: Job acceptance metadata including job_id, status, and status message.
        """
        hasher = hashlib.sha256()
        chunk_size = 1024 * 1024  # 1 MB read buffer

        # Hold up to 10 MB in RAM before spilling over to a temporary disk file
        with tempfile.SpooledTemporaryFile(max_size=10 * 1024 * 1024) as spool:
            while True:
                chunk = await file.read(chunk_size)
                if not chunk:
                    break
                hasher.update(chunk)
                spool.write(chunk)

            file_hash = hasher.hexdigest()
            # Reset stream pointer back to beginning for downstream consumer reading
            spool.seek(0)

            # Check for duplicate file hash in PostgreSQL
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT id, status FROM upload_jobs WHERE file_hash = %s",
                        (file_hash,),
                    )
                    existing = cur.fetchone()
                    if existing and existing[1] in ["QUEUED", "PROCESSING", "COMPLETED"]:
                        return {
                            "job_id": str(existing[0]),
                            "status": existing[1],
                            "message": "Duplicate file detected. Existing job returned.",
                        }

                    job_id = str(uuid.uuid4())
                    blob_name = f"uploads/{job_id}_{file.filename}"

                    # Stream upload to Azure Blob Storage directly from temporary file
                    self.blob_service.upload_stream(blob_name, spool)

                    # Insert initial job state into database
                    cur.execute(
                        """
                        INSERT INTO upload_jobs (id, file_name, file_hash, status, created_at, updated_at)
                        VALUES (%s, %s, %s, 'QUEUED', NOW(), NOW())
                        """,
                        (job_id, file.filename, file_hash),
                    )
                    conn.commit()

            # Enqueue the background processing event to Service Bus
            self.queue_service.enqueue_job(job_id=job_id, blob_name=blob_name)

            return {
                "job_id": job_id,
                "status": "QUEUED",
                "message": "File accepted for processing.",
            }

    def get_job_status(self, job_id: str) -> Dict[str, Any]:
        """Fetch current execution status and row-level parsing errors for a job.

        Args:
            job_id: Unique UUID string identifying the target upload job.

        Returns:
            dict: Job progress details conforming to JobStatusResponse schema.

        Raises:
            HTTPException: 404 status if no job matches the provided job_id.
        """
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT status, total_rows, processed_rows, created_at, updated_at 
                    FROM upload_jobs 
                    WHERE id = %s
                    """,
                    (job_id,),
                )
                job = cur.fetchone()
                if not job:
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail=f"Job with ID '{job_id}' not found.",
                    )

                # Fetch up to 100 row-level parsing/validation errors
                cur.execute(
                    """
                    SELECT row_number, error_message 
                    FROM job_errors 
                    WHERE job_id = %s 
                    ORDER BY row_number ASC 
                    LIMIT 100
                    """,
                    (job_id,),
                )
                errors = [{"row": r[0], "message": r[1]} for r in cur.fetchall()]

                return {
                    "job_id": job_id,
                    "status": job[0],
                    "total_rows": job[1] or 0,
                    "processed_rows": job[2] or 0,
                    "created_at": job[3].isoformat() if job[3] else None,
                    "updated_at": job[4].isoformat() if job[4] else None,
                    "errors": errors,
                }