import io
import json
import logging
import os
import zipfile
from typing import Any, Dict

from azure.servicebus import ServiceBusClient, ServiceBusReceiver
from azure.storage.blob import BlobServiceClient
import openpyxl
import psycopg2
from psycopg2.extras import execute_values
from pydantic import ValidationError

from schemas import ParticipantRowSchema

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("worker")

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/marathon_pulse",
)
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING", "")
SERVICEBUS_CONNECTION_STRING = os.getenv("SERVICEBUS_CONNECTION_STRING", "local")
BLOB_CONTAINER_NAME = os.getenv("BLOB_CONTAINER_NAME", "excel-uploads")
SERVICEBUS_QUEUE_NAME = os.getenv("SERVICEBUS_QUEUE_NAME", "excel-jobs")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))


def get_db_connection():
    return psycopg2.connect(DATABASE_URL)


def detect_xlsx_total_rows(file_stream: io.BytesIO) -> int:
    """Instantly counts <row elements in the first sheet XML within ~0.04s."""
    try:
        file_stream.seek(0)
        with zipfile.ZipFile(file_stream, "r") as z:
            sheet_files = [
                n for n in z.namelist() 
                if n.startswith("xl/worksheets/sheet") and n.endswith(".xml")
            ]
            if not sheet_files:
                return 0
            with z.open(sorted(sheet_files)[0]) as f:
                count = 0
                while chunk := f.read(1024 * 1024):
                    count += chunk.count(b"<row ")
                return max(0, count - 1)  # Subtract 1 for the header row
    except Exception as e:
        logger.warning(f"Could not inspect XML row count: {e}")
        return 0
    finally:
        file_stream.seek(0)


def process_excel_stream(file_stream: io.BytesIO, job_id: str) -> None:
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        # 1. Determine total rows upfront before openpyxl iteration
        estimated_total = detect_xlsx_total_rows(file_stream)
        logger.info(f"Job {job_id}: Total records detected: {estimated_total}")

        # 2. Immediately update database so the frontend has total_rows on first poll
        cur.execute(
            """
            UPDATE upload_jobs
            SET status = 'PROCESSING',
                total_rows = %s,
                updated_at = NOW()
            WHERE id = %s
            """,
            (estimated_total, job_id),
        )
        conn.commit()

        # 3. Stream workbook records
        wb = openpyxl.load_workbook(file_stream, read_only=True, data_only=True)
        sheet = wb.active

        valid_rows = []
        error_rows = []
        processed_count = 0

        for row_idx, row in enumerate(
            sheet.iter_rows(min_row=2, values_only=True), start=2
        ):
            if not row or not any(row):
                continue

            processed_count += 1
            raw_data = {
                "first_name": row[0],
                "last_name": row[1],
                "email": row[2],
                "bib_number": str(row[3]) if row[3] is not None else None,
                "category": str(row[4]).strip().upper() if row[4] else None,
                "registration_fee": row[5],
            }

            try:
                validated = ParticipantRowSchema(**raw_data)
                valid_rows.append(
                    (
                        validated.first_name,
                        validated.last_name,
                        validated.email,
                        validated.bib_number,
                        validated.category.value,
                        validated.registration_fee,
                        job_id,
                    )
                )
            except (ValidationError, Exception) as err:
                error_rows.append((job_id, row_idx, str(err)))

            # Flush valid rows in batches
            if len(valid_rows) >= BATCH_SIZE:
                execute_values(
                    cur,
                    """
                    INSERT INTO marathon_participants 
                        (first_name, last_name, email, bib_number, category, registration_fee, job_id)
                    VALUES %s
                    """,
                    valid_rows,
                )
                valid_rows.clear()

            # Flush error rows in batches
            if len(error_rows) >= BATCH_SIZE:
                execute_values(
                    cur,
                    """
                    INSERT INTO job_errors (job_id, row_number, error_message)
                    VALUES %s
                    """,
                    error_rows,
                )
                error_rows.clear()

            # Periodic progress heartbeat to PostgreSQL
            if processed_count % BATCH_SIZE == 0:
                cur.execute(
                    """
                    UPDATE upload_jobs 
                    SET processed_rows = %s, updated_at = NOW() 
                    WHERE id = %s
                    """,
                    (processed_count, job_id),
                )
                conn.commit()
                logger.info(f"Job {job_id}: Processed {processed_count} rows...")

        # Final flush for remaining valid rows
        if valid_rows:
            execute_values(
                cur,
                """
                INSERT INTO marathon_participants 
                    (first_name, last_name, email, bib_number, category, registration_fee, job_id)
                VALUES %s
                """,
                valid_rows,
            )

        # Final flush for remaining error rows
        if error_rows:
            execute_values(
                cur,
                """
                INSERT INTO job_errors (job_id, row_number, error_message)
                VALUES %s
                """,
                error_rows,
            )

        # Mark job COMPLETED
        cur.execute(
            """
            UPDATE upload_jobs 
            SET status = 'COMPLETED', total_rows = %s, processed_rows = %s, updated_at = NOW() 
            WHERE id = %s
            """,
            (processed_count, processed_count, job_id),
        )
        conn.commit()
        logger.info(f"Job {job_id} COMPLETED. Total rows: {processed_count}")

    except Exception as exc:
        conn.rollback()
        logger.error(f"Error processing job {job_id}: {exc}", exc_info=True)
        try:
            cur.execute(
                """
                UPDATE upload_jobs 
                SET status = 'FAILED', updated_at = NOW() 
                WHERE id = %s
                """,
                (job_id,),
            )
            conn.commit()
        except Exception as inner_exc:
            logger.error(f"Failed to record FAILED status for job {job_id}: {inner_exc}")
        raise exc
    finally:
        cur.close()
        conn.close()