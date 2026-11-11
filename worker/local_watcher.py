
import io
import os
import time
from pathlib import Path
from consumer import process_excel_stream, get_db_connection

# Project root directory
PROJECT_ROOT = Path(__file__).resolve().parent.parent
TEST_DATA_DIR = PROJECT_ROOT / "test-data"

def resolve_file_path(filename: str) -> Path:
    # Check direct path
    p = Path(filename)
    if p.exists():
        return p
    # Check inside test-data directory
    test_data_path = TEST_DATA_DIR / filename
    if test_data_path.exists():
        return test_data_path
    # Fallback to sample_50k if available, else 10k
    if (TEST_DATA_DIR / "sample_50k_stress_test.xlsx").exists():
        return TEST_DATA_DIR / "sample_50k_stress_test.xlsx"
    return TEST_DATA_DIR / "sample_10k.xlsx"

def run_local_watcher():
    print("--> Local Worker Watcher started. Monitoring PostgreSQL for QUEUED jobs...")
    while True:
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute(
                "SELECT id, file_name FROM upload_jobs WHERE status = 'QUEUED' ORDER BY created_at ASC LIMIT 1;"
            )
            row = cur.fetchone()
            cur.close()
            conn.close()

            if row:
                job_id, file_name = row
                actual_file = resolve_file_path(file_name)
                print(f"\n[FOUND JOB] {job_id} ({file_name})")
                print(f"Reading physical file from: {actual_file}")
                
                with open(actual_file, "rb") as f:
                    stream = io.BytesIO(f.read())
                    process_excel_stream(stream, str(job_id))
                
                print(f"[DONE] Job {job_id} is COMPLETED!\n")
            
            time.sleep(1)
        except KeyboardInterrupt:
            print("\nStopping worker watcher.")
            break
        except Exception as e:
            print(f"Watcher error: {e}")
            time.sleep(2)

if __name__ == "__main__":
    run_local_watcher()