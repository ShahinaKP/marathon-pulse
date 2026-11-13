"""Apply database/schema.sql to the database in DATABASE_URL.

Usage (from the repo root):
    python devops/apply_schema.py            # reads DATABASE_URL from server/.env
    python devops/apply_schema.py --reset    # DROP the app tables first, then re-create
Cross-platform replacement for `psql -f`, so no psql install is needed.
"""
import os
import sys
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / "server" / ".env")

url = os.getenv("DATABASE_URL")
if not url:
    sys.exit("DATABASE_URL is not set (put it in server/.env or export it).")

reset = "--reset" in sys.argv
sql = (ROOT / "database" / "schema.sql").read_text(encoding="utf-8")

conn = psycopg2.connect(url)
try:
    if reset:
        host = url.split("@")[-1].split("/")[0]
        if input(f"DROP all Marathon Pulse tables on {host}? Type 'yes': ") != "yes":
            sys.exit("Aborted.")
        with conn, conn.cursor() as cur:
            cur.execute(
                "DROP TABLE IF EXISTS job_errors, marathon_participants, upload_jobs CASCADE;"
                "DROP TYPE IF EXISTS job_status_enum, race_category_enum;"
            )
        print("Old tables dropped.")
    with conn, conn.cursor() as cur:
        cur.execute(sql)
    print("Schema applied successfully.")
finally:
    conn.close()
