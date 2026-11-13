"""worker/processor.py

Everything that touches a job: atomic claim, blob download, streaming parse,
validation, batch insert with insert-time duplicate detection, and the
retry-or-fail decision. No queue code lives here (see consumer.py).

Delivery from Azure Storage Queue is at-least-once, so every step is written
to be safe to repeat:
  * claim_job() lets exactly one worker own a job at a time
  * each attempt starts by deleting that job's rows (clean slate)
  * duplicates are decided by the database unique constraint (email, bib_number)
"""

import logging
import os
import tempfile
import zipfile
from dataclasses import dataclass
from typing import List, Optional, Tuple

import openpyxl
import psycopg2
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import BlobServiceClient
from openpyxl.utils.exceptions import InvalidFileException
from psycopg2.extras import execute_values
from pydantic import ValidationError

from schemas import ParticipantRowSchema
from settings import settings

logger = logging.getLogger("worker.processor")

# claim_job() outcomes
CLAIMED = "claimed"      # this worker now owns the job
DONE = "done"            # already COMPLETED or FAILED - just drop the message
BUSY = "busy"            # another worker is actively processing it
EXHAUSTED = "exhausted"  # out of attempts - job was marked FAILED
MISSING = "missing"      # no such job row

DUPLICATE_MESSAGE = "Duplicate: a participant with this email and bib number already exists"

INSERT_SQL = """
    INSERT INTO marathon_participants
        (job_id, row_number, first_name, last_name, email, bib_number, category, registration_fee)
    VALUES %s
    ON CONFLICT ON CONSTRAINT uq_participant_email_bib DO NOTHING
    RETURNING row_number
"""


class PermanentJobError(Exception):
    """The job can never succeed (corrupt file, missing blob) - do not retry."""


@dataclass
class Counters:
    processed: int = 0
    inserted: int = 0
    duplicates: int = 0
    invalid: int = 0


def get_connection():
    return psycopg2.connect(settings.DATABASE_URL, connect_timeout=10)


# ---------------------------------------------------------------------------
# Claiming
# ---------------------------------------------------------------------------
CLAIM_SQL = """
    UPDATE upload_jobs
    SET status = 'PROCESSING',
        attempt_count = attempt_count + 1,
        started_at = COALESCE(started_at, NOW()),
        updated_at = NOW()
    WHERE id = %s
      AND attempt_count < %s
      AND (status = 'QUEUED'
           OR (status = 'PROCESSING' AND updated_at < NOW() - make_interval(secs => %s)))
    RETURNING blob_name
"""


def claim_job(job_id: str) -> Tuple[str, Optional[str]]:
    """Atomically take ownership of a job.

    One UPDATE decides the winner, so two workers can never both process the
    same job. A job stuck in PROCESSING with a stale heartbeat (worker died) is
    re-claimable. Returns (outcome, blob_name); blob_name is set only for CLAIMED.
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(CLAIM_SQL, (job_id, settings.MAX_ATTEMPTS, settings.CLAIM_STALE_SECONDS))
            row = cur.fetchone()
            if row:
                conn.commit()
                return CLAIMED, row[0]

            # Not claimable - work out why.
            cur.execute(
                """
                SELECT status, attempt_count,
                       updated_at < NOW() - make_interval(secs => %s) AS stale
                FROM upload_jobs WHERE id = %s
                """,
                (settings.CLAIM_STALE_SECONDS, job_id),
            )
            info = cur.fetchone()
            if info is None:
                return MISSING, None
            status, attempts, stale = info
            if status in ("COMPLETED", "FAILED"):
                return DONE, None
            if status == "PROCESSING" and not stale:
                return BUSY, None
            if attempts >= settings.MAX_ATTEMPTS:
                cur.execute(
                    """
                    UPDATE upload_jobs
                    SET status = 'FAILED', completed_at = NOW(), updated_at = NOW(),
                        error_message = 'Exceeded maximum processing attempts. Last error: '
                                        || COALESCE(error_message, 'worker stopped responding')
                    WHERE id = %s AND status IN ('QUEUED', 'PROCESSING')
                    """,
                    (job_id,),
                )
                cur.execute("DELETE FROM marathon_participants WHERE job_id = %s", (job_id,))
                conn.commit()
                return EXHAUSTED, None
            return BUSY, None  # lost a race between the UPDATE and this SELECT
    finally:
        conn.close()


def mark_retry_or_fail(job_id: str, message: str, permanent: bool) -> Optional[str]:
    """After a failed attempt: put the job back to QUEUED for a retry, or fail it for good.

    A job fails for good when the error is permanent or attempts are used up.
    Final failure removes the job's partial rows, so a FAILED job leaves no
    half-loaded data behind. Returns the new status, or None if the job was
    already finished.
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE upload_jobs
                SET status = CASE WHEN %(perm)s OR attempt_count >= %(max)s
                                  THEN 'FAILED'::job_status_enum
                                  ELSE 'QUEUED'::job_status_enum END,
                    completed_at = CASE WHEN %(perm)s OR attempt_count >= %(max)s
                                        THEN NOW() ELSE NULL END,
                    error_message = %(msg)s,
                    updated_at = NOW()
                WHERE id = %(id)s AND status IN ('QUEUED', 'PROCESSING')
                RETURNING status
                """,
                {"perm": permanent, "max": settings.MAX_ATTEMPTS, "msg": message[:1000], "id": job_id},
            )
            row = cur.fetchone()
            new_status = row[0] if row else None
            if new_status == "FAILED":
                cur.execute("DELETE FROM marathon_participants WHERE job_id = %s", (job_id,))
        conn.commit()
        return new_status
    finally:
        conn.close()


def get_attempt_count(job_id: str) -> int:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT attempt_count FROM upload_jobs WHERE id = %s", (job_id,))
            row = cur.fetchone()
            return row[0] if row else 0
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Blob download
# ---------------------------------------------------------------------------
def download_blob(blob_name: str, dest_path: str) -> None:
    """Download the uploaded workbook to a local temp file.

    openpyxl needs a seekable file, and a temp file keeps memory flat for big workbooks.
    """
    service = BlobServiceClient.from_connection_string(settings.AZURE_STORAGE_CONNECTION_STRING)
    blob = service.get_blob_client(container=settings.BLOB_CONTAINER_NAME, blob=blob_name)
    try:
        with open(dest_path, "wb") as fh:
            blob.download_blob().readinto(fh)
    except ResourceNotFoundError as exc:
        raise PermanentJobError(f"Uploaded file '{blob_name}' was not found in storage.") from exc


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------
def estimate_total_rows(path: str) -> int:
    """Fast row estimate by counting <row> tags in the first worksheet XML (header excluded)."""
    try:
        with zipfile.ZipFile(path, "r") as z:
            sheets = sorted(
                n for n in z.namelist() if n.startswith("xl/worksheets/sheet") and n.endswith(".xml")
            )
            if not sheets:
                return 0
            count = 0
            with z.open(sheets[0]) as f:
                while chunk := f.read(1024 * 1024):
                    count += chunk.count(b"<row ")
            return max(0, count - 1)
    except Exception as exc:  # noqa: BLE001 - estimate only, never fatal
        logger.warning("Could not estimate row count: %s", exc)
        return 0


def _is_blank_row(row) -> bool:
    return not row or all(c is None or (isinstance(c, str) and not c.strip()) for c in row)


def _row_to_dict(row) -> dict:
    cells = (list(row) + [None] * 6)[:6]
    return {
        "first_name": cells[0],
        "last_name": cells[1],
        "email": cells[2],
        "bib_number": cells[3],
        "category": cells[4],
        "registration_fee": cells[5],
    }


def format_validation_error(err: ValidationError) -> str:
    parts = [
        f"{'.'.join(str(p) for p in e['loc']) or 'row'}: {e['msg']}" for e in err.errors()
    ]
    return "; ".join(parts)[:500]


# ---------------------------------------------------------------------------
# Ingestion
# ---------------------------------------------------------------------------
def _flush(conn, cur, job_id: str, batch: List[tuple], errors: List[tuple], stats: Counters) -> None:
    """Insert a batch, turn conflicts into duplicate errors, save progress, commit."""
    if batch:
        returned = execute_values(cur, INSERT_SQL, batch, page_size=len(batch), fetch=True)
        inserted_rows = {r[0] for r in returned}
        for rec in batch:
            row_number = rec[1]
            if row_number not in inserted_rows:
                errors.append((job_id, row_number, DUPLICATE_MESSAGE))
                stats.duplicates += 1
        stats.inserted += len(inserted_rows)
        batch.clear()

    if errors:
        execute_values(
            cur,
            "INSERT INTO job_errors (job_id, row_number, error_message) VALUES %s",
            errors,
            page_size=1000,
        )
        errors.clear()

    # Heartbeat: updated_at proves this worker is alive.
    cur.execute(
        """
        UPDATE upload_jobs
        SET processed_rows = %s, inserted_rows = %s, duplicate_rows = %s,
            invalid_rows = %s, updated_at = NOW()
        WHERE id = %s
        """,
        (stats.processed, stats.inserted, stats.duplicates, stats.invalid, job_id),
    )
    conn.commit()


def ingest_file(path: str, job_id: str) -> Counters:
    """Stream-parse a local .xlsx and load it into PostgreSQL for `job_id`."""
    stats = Counters()
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            # Clean slate: a retry must not see (or double-count) a previous attempt's rows.
            cur.execute("DELETE FROM marathon_participants WHERE job_id = %s", (job_id,))
            cur.execute("DELETE FROM job_errors WHERE job_id = %s", (job_id,))
            total = estimate_total_rows(path)
            cur.execute(
                """
                UPDATE upload_jobs
                SET total_rows = %s, processed_rows = 0, inserted_rows = 0,
                    duplicate_rows = 0, invalid_rows = 0, updated_at = NOW()
                WHERE id = %s
                """,
                (total, job_id),
            )
        conn.commit()
        logger.info("Job %s: ~%d rows detected", job_id, total)

        try:
            wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
        except (zipfile.BadZipFile, InvalidFileException, KeyError) as exc:
            raise PermanentJobError(f"Not a readable .xlsx workbook: {exc}") from exc

        batch: List[tuple] = []
        errors: List[tuple] = []
        try:
            sheet = wb.active
            with conn.cursor() as cur:
                for row_idx, row in enumerate(sheet.iter_rows(min_row=2, values_only=True), start=2):
                    if _is_blank_row(row):
                        continue
                    stats.processed += 1
                    try:
                        rec = ParticipantRowSchema(**_row_to_dict(row))
                    except ValidationError as err:
                        stats.invalid += 1
                        errors.append((job_id, row_idx, format_validation_error(err)))
                    else:
                        batch.append(
                            (
                                job_id, row_idx, rec.first_name, rec.last_name, rec.email,
                                rec.bib_number, rec.category.value, rec.registration_fee,
                            )
                        )

                    if stats.processed % settings.BATCH_SIZE == 0:
                        _flush(conn, cur, job_id, batch, errors, stats)
                        logger.info("Job %s: %d rows processed", job_id, stats.processed)

                _flush(conn, cur, job_id, batch, errors, stats)  # remainder

                cur.execute(
                    """
                    UPDATE upload_jobs
                    SET status = 'COMPLETED', total_rows = %s, processed_rows = %s,
                        inserted_rows = %s, duplicate_rows = %s, invalid_rows = %s,
                        error_message = NULL, completed_at = NOW(), updated_at = NOW()
                    WHERE id = %s
                    """,
                    (stats.processed, stats.processed, stats.inserted,
                     stats.duplicates, stats.invalid, job_id),
                )
            conn.commit()
        finally:
            wb.close()

        logger.info(
            "Job %s COMPLETED: %d rows, %d inserted, %d duplicates, %d invalid",
            job_id, stats.processed, stats.inserted, stats.duplicates, stats.invalid,
        )
        return stats
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def process_job(job_id: str, blob_name: str) -> Counters:
    """Download the blob and ingest it. Raises on failure; the caller decides retry vs fail."""
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "upload.xlsx")
        download_blob(blob_name, path)
        return ingest_file(path, job_id)
