"""worker/local_test.py

Developer harness: run the ingestion pipeline on a local .xlsx WITHOUT the queue
or blob storage. It still writes to whatever database DATABASE_URL points at.

    cd worker
    python local_test.py ../test-data/sample_10k.xlsx
    python local_test.py ../test-data/sample_10k.xlsx   # 2nd run -> every row is a duplicate
"""
import os
import sys
import uuid

from processor import get_connection, ingest_file


def run(sample_path: str) -> None:
    job_id = str(uuid.uuid4())
    conn = get_connection()
    with conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO upload_jobs (id, file_name, blob_name, status, attempt_count, started_at)
            VALUES (%s, %s, %s, 'PROCESSING', 1, NOW())
            """,
            (job_id, os.path.basename(sample_path), f"local/{job_id}.xlsx"),
        )
    conn.close()

    print(f"Job {job_id}: ingesting {sample_path} ...")
    stats = ingest_file(sample_path, job_id)

    conn = get_connection()
    with conn, conn.cursor() as cur:
        cur.execute(
            "SELECT status, total_rows, processed_rows, inserted_rows, duplicate_rows, invalid_rows "
            "FROM upload_jobs WHERE id = %s",
            (job_id,),
        )
        status, total, processed, inserted, dupes, invalid = cur.fetchone()
        cur.execute("SELECT COUNT(*) FROM marathon_participants WHERE job_id = %s", (job_id,))
        in_table = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM job_errors WHERE job_id = %s", (job_id,))
        error_rows = cur.fetchone()[0]
    conn.close()

    print("\n--- Ingestion summary ---")
    print(f"Status:            {status}")
    print(f"Rows processed:    {processed} / {total}")
    print(f"Inserted:          {inserted} (rows in table: {in_table})")
    print(f"Duplicates:        {dupes}")
    print(f"Invalid:           {invalid}")
    print(f"job_errors rows:   {error_rows}")
    assert in_table == inserted == stats.inserted
    assert error_rows == dupes + invalid


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else "../test-data/sample_10k.xlsx"
    run(path)
