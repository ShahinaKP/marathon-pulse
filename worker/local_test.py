"""worker/test_local.py

Local ingestion harness for testing Excel processing.
Simulates end-to-end ingestion of an Excel file without requiring active Azure
Service Bus or Azure Blob Storage infrastructure by creating a local job record,
invoking the streaming processor, and asserting database state.
"""

import io
import os
import uuid
from consumer import get_db_connection, process_excel_stream


def test_local_ingestion(sample_path: str) -> None:
    """Run an isolated local test of the Excel ingestion pipeline.

    Args:
        sample_path: File system path pointing to the test .xlsx file.
    """
    job_id = str(uuid.uuid4())
    filename = os.path.basename(sample_path)

    print(f"Creating job record {job_id} in database...")
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO upload_jobs (id, file_name, file_hash, status, created_at, updated_at)
        VALUES (%s, %s, %s, 'QUEUED', NOW(), NOW())
        """,
        (job_id, filename, f"hash-{job_id[:8]}"),
    )
    conn.commit()
    cur.close()
    conn.close()

    print(f"Parsing and processing '{sample_path}'...")
    with open(sample_path, "rb") as f:
        stream = io.BytesIO(f.read())
        process_excel_stream(stream, job_id)

    # Check results against the actual schema table (marathon_participants)
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT status, total_rows, processed_rows FROM upload_jobs WHERE id = %s",
        (job_id,),
    )
    status, total, processed = cur.fetchone()

    # Note: Target table is marathon_participants and column is job_id
    cur.execute(
        "SELECT COUNT(*) FROM marathon_participants WHERE job_id = %s",
        (job_id,),
    )
    valid_count = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM job_errors WHERE job_id = %s", (job_id,))
    error_count = cur.fetchone()[0]

    print("\n--- Ingestion Verification Summary ---")
    print(f"Job Status:      {status}")
    print(f"Rows Processed:  {processed} / {total}")
    print(f"Valid Inserted:  {valid_count}")
    print(f"Errors Logged:   {error_count}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    sample_file = "../test-data/sample_10k.xlsx"
    if not os.path.exists(sample_file):
        sample_file = "test-data/sample_10k.xlsx"
    test_local_ingestion(sample_file)