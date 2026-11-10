"""server/app/controllers/upload_controller.py

File ingestion and job progress tracking controller.
Exposes endpoints to accept multipart Excel uploads via streaming
and query background ingestion lifecycle states with row-level error reporting.
"""

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile, status
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
        status.HTTP_200_OK: {
            "model": JobUploadResponse,
            "description": "File has already been uploaded previously (duplicate detected).",
        },
        status.HTTP_202_ACCEPTED: {
            "model": JobUploadResponse,
            "description": "File accepted and queued for background ingestion.",
        },
        status.HTTP_400_BAD_REQUEST: {
            "description": "Invalid file type. Only .xlsx and .xls extensions are supported.",
        },
    },
)
async def upload_file(
    file: UploadFile = File(...),
    service: UploadService = Depends(get_upload_service),
) -> JSONResponse:
    """Stream and enqueue an Excel file for background processing.

    Validates extension, computes checksum deduplication, uploads binary payload
    to blob storage, creates database record, and publishes a queue notification.
    """
    if not file.filename.lower().endswith((".xlsx", ".xls")):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid file format. Only Excel files (.xlsx, .xls) are accepted.",
        )

    result = await service.process_streaming_upload(file)
    http_code = (
        status.HTTP_200_OK
        if "Duplicate" in result["message"]
        else status.HTTP_202_ACCEPTED
    )
    return JSONResponse(status_code=http_code, content=result)


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