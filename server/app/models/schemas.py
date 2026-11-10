"""server/app/models/schemas.py

Pydantic schemas for API response validation and serialization.
Defines contracts for file upload acknowledgments, job status tracking,
and per-row processing error diagnostics.
"""

from typing import List, Optional
from pydantic import BaseModel


class JobUploadResponse(BaseModel):
    """Payload returned immediately after an upload request is accepted."""

    job_id: str
    status: str
    message: str


class RowErrorDetail(BaseModel):
    """Diagnostic details for an individual failed row during file parsing."""

    row: int
    message: str


class JobStatusResponse(BaseModel):
    """Payload returned when polling background processing progress."""

    job_id: str
    status: str
    total_rows: int
    processed_rows: int
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    errors: List[RowErrorDetail] = []