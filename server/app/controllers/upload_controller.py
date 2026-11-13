"""server/app/controllers/upload_controller.py

File ingestion and job progress tracking controller.
Exposes endpoints to accept multipart Excel uploads via streaming
and query background ingestion lifecycle states with row-level error reporting.
"""

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile, status
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse

from app.models.schemas import JobStatusResponse, JobUploadResponse
from app.services import BlobService, QueueService, UploadService


router = APIRouter(prefix="/api", tags=["Upload"])


def get_upload_service() -> UploadService:
    """Dependency provider for UploadService.

    Instantiates and injects BlobService and QueueService dependencies.
    """
    return UploadService(
        blob_service=BlobService(),
        queue_service=QueueService()
    )


@router.post(
    "/upload",
    response_model=JobUploadResponse,
    status_code=status.HTTP_202_ACCEPTED,
    responses={
        status.HTTP_202_ACCEPTED: {
            "model": JobUploadResponse,
            "description": "File accepted and queued for background ingestion.",
        },
        status.HTTP_400_BAD_REQUEST: {
            "description": "Not a valid .xlsx workbook (extension or content).",
        },
        status.HTTP_413_REQUEST_ENTITY_TOO_LARGE: {
            "description": "File exceeds the configured size limit.",
        },
        status.HTTP_502_BAD_GATEWAY: {"description": "Blob storage unavailable."},
        status.HTTP_503_SERVICE_UNAVAILABLE: {"description": "Queue unavailable."},
    },
)
async def upload_file(
    file: UploadFile = File(...),
    service: UploadService = Depends(get_upload_service),
) -> JSONResponse:
    """Register, store and enqueue an Excel file, then return immediately (HTTP 202).

    Parsing and database insertion happen later in the cloud worker. The
    blocking work (DB, Blob, Queue SDK calls) runs in a thread pool so the
    event loop stays free for concurrent uploads.
    """
    if not (file.filename or "").lower().endswith(".xlsx"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid file format. Only .xlsx files are accepted.",
        )

    result = await run_in_threadpool(service.process_upload, file.filename, file.file)
    return JSONResponse(status_code=status.HTTP_202_ACCEPTED, content=result)


@router.get(
    "/status/{job_id}",
    response_model=JobStatusResponse,
    responses={
        status.HTTP_200_OK: {
            "description": "Current processing progress and validation errors for the job.",
        },
        status.HTTP_404_NOT_FOUND: {
            "description": "Provided job_id does not exist.",
        },
    },
)
def check_job_status(
    job_id: str,
    service: UploadService = Depends(get_upload_service),
) -> JobStatusResponse:
    """Retrieve job execution status and parsing error diagnostics by job ID."""
    return service.get_job_status(job_id)