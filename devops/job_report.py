"""Print recent Marathon Pulse jobs and totals straight from the database.

    python devops/job_report.py            # last 10 jobs, once
    python devops/job_report.py 25         # last 25 jobs
    python devops/job_report.py --watch    # refresh every 3 seconds (Ctrl+C to stop)

Reads DATABASE_URL from server/.env (or the environment). Handy during the demo.
"""
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / "server" / ".env")

JOBS_SQL = """
    SELECT left(id::text, 8), file_name, status, processed_rows, total_rows,
           inserted_rows, duplicate_rows, invalid_rows, attempt_count,
           COALESCE(EXTRACT(EPOCH FROM (COALESCE(completed_at, NOW()) - started_at))::int, 0),
           error_message
    FROM upload_jobs ORDER BY created_at DESC LIMIT %s
"""


def fmt_duration(seconds: int) -> str:
    return f"{seconds // 60:02d}:{seconds % 60:02d}"


def render(conn, limit: int) -> str:
    with conn.cursor() as cur:
        cur.execute(JOBS_SQL, (limit,))
        jobs = cur.fetchall()
        cur.execute("SELECT COUNT(*) FROM marathon_participants")
        participants = cur.fetchone()[0]
        cur.execute("SELECT status, COUNT(*) FROM upload_jobs GROUP BY status")
        by_status = dict(cur.fetchall())
    conn.rollback()

    lines = [f"Marathon Pulse - jobs, newest first    {datetime.now():%H:%M:%S}", ""]
    lines.append(f"{'ID':<9}{'FILE':<26}{'STATUS':<12}{'ROWS done/total':<18}"
                 f"{'INSERTED':>9}{'DUPES':>8}{'INVALID':>9}{'TRIES':>7}{'TIME':>7}")
    for (jid, name, status, done, total, ins, dup, inv, tries, secs, _err) in jobs:
        rows = f"{done:,}/{total:,}"
        lines.append(f"{jid:<9}{name[:24]:<26}{status:<12}{rows:<18}"
                     f"{ins:>9,}{dup:>8,}{inv:>9,}{tries:>7}{fmt_duration(secs):>7}")
    if not jobs:
        lines.append("(no jobs yet)")
    lines.append("")
    summary = "  ".join(f"{k}={v}" for k, v in sorted(by_status.items())) or "none"
    lines.append(f"Participants in database: {participants:,}    Jobs: {summary}")
    for (jid, name, status, *_rest, err) in jobs:
        if status == "FAILED" and err:
            lines.append(f"  FAILED {jid} ({name}): {err}")
        elif status == "QUEUED" and err:
            lines.append(f"  RETRYING {jid} ({name}) after: {err}")
    return "\n".join(lines)


def main() -> None:
    url = os.getenv("DATABASE_URL")
    if not url:
        sys.exit("DATABASE_URL is not set (put it in server/.env or export it).")
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    limit = int(args[0]) if args else 10
    watch = "--watch" in sys.argv

    conn = psycopg2.connect(url, connect_timeout=10)
    try:
        while True:
            out = render(conn, limit)
            print("\033[2J\033[H" + out if watch else out, flush=True)
            if not watch:
                break
            time.sleep(3)
    except KeyboardInterrupt:
        pass
    finally:
        conn.close()


if __name__ == "__main__":
    main()
