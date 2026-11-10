"""worker/consumer.py

Azure Service Bus queue consumer and Excel ingestion worker.
Listens for job events on the 'excel-jobs' queue, streams the Excel blob
from Azure Blob Storage using openpyxl (read-only mode), validates each row against
ParticipantRowSchema, batch-inserts valid rows into PostgreSQL via execute_values,
and persists row-level errors and job progress states.
"""

import io
import json
import logging
import os
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
    """Establish and return a connection to the PostgreSQL database."""
    return psycopg2.connect(DATABASE_URL)


def process_excel_stream(file_stream: io.BytesIO, job_id: str) -> None:
    """Stream and parse an Excel workbook, validate records, and batch insert.

    Args:
        file_stream: In-memory byte stream of the Excel workbook (.xlsx).
        job_id: UUID of the upload job.
    """
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        # Load workbook in read-only streaming mode to minimize memory footprint
        wb = openpyxl.load_workbook(file_stream, read_only=True, data_only=True)
        sheet = wb.active

        # Update status to PROCESSING
        cur.execute(
            """
            UPDATE upload_jobs
            SET status = 'PROCESSING', updated_at = NOW()
            WHERE id = %s
            """,
            (job_id,),
        )
        conn.commit()

        valid_rows = []
        error_rows = []
        processed_count = 0

        # Row 1 is header: first_name, last_name, email, bib_number, category, registration_fee
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

            # Flush valid rows in batches using psycopg2 execute_values
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

            # Periodic progress heartbeat
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

        # Mark job as COMPLETED
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


def run_worker() -> None:
    """Listen for incoming jobs on Azure Service Bus and dispatch to the processor."""
    logger.info("Starting marathon background worker...")

    if (
        not AZURE_STORAGE_CONNECTION_STRING
        or not SERVICEBUS_CONNECTION_STRING
        or SERVICEBUS_CONNECTION_STRING.lower() == "local"
    ):
        logger.warning(
            "Cloud connection strings are not configured or set to 'local'. "
            "Worker running in standby idle state."
        )
        return

    blob_service_client = BlobServiceClient.from_connection_string(
        AZURE_STORAGE_CONNECTION_STRING
    )
    sb_client = ServiceBusClient.from_connection_string(SERVICEBUS_CONNECTION_STRING)

    with sb_client:
        receiver: ServiceBusReceiver = sb_client.get_queue_receiver(
            queue_name=SERVICEBUS_QUEUE_NAME,
            max_wait_time=10,
        )
        with receiver:
            logger.info(f"Worker listening on queue: '{SERVICEBUS_QUEUE_NAME}'")
            for msg in receiver:
                try:
                    payload: Dict[str, Any] = json.loads(str(msg))
                    job_id = payload["job_id"]
                    blob_name = payload["blob_name"]
                    logger.info(f"Received job {job_id} for blob {blob_name}")

                    blob_client = blob_service_client.get_blob_client(
                        container=BLOB_CONTAINER_NAME,
                        blob=blob_name,
                    )
                    download_stream = io.BytesIO()
                    blob_client.download_blob().readinto(download_stream)
                    download_stream.seek(0)

                    process_excel_stream(download_stream, job_id)
                    receiver.complete_message(msg)

                except Exception as e:
                    logger.error(f"Failed to process message: {e}")
                    receiver.abandon_message(msg)


if __name__ == "__main__":
    run_worker()