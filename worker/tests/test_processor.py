"""Job logic with the database faked out: claiming, retry/fail, batching and duplicate counting."""
import zipfile
from unittest.mock import MagicMock

import pytest

import processor
from processor import Counters
from settings import settings


# --------------------------------------------------------------------------- fakes
class FakeCursor:
    def __init__(self, conn, fetch_rows=()):
        self.conn = conn
        self._fetch = list(fetch_rows)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self.conn.executed.append((" ".join(sql.split()), params))

    def fetchone(self):
        return self._fetch.pop(0) if self._fetch else None


class FakeConn:
    def __init__(self, fetch_rows=()):
        self.executed = []
        self.commits = 0
        self.rollbacks = 0
        self.closed = False
        self._fetch_rows = list(fetch_rows)

    def cursor(self):
        return FakeCursor(self, self._fetch_rows)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed = True


def use_conn(monkeypatch, conn):
    monkeypatch.setattr(processor, "get_connection", lambda: conn)
    return conn


# --------------------------------------------------------------------------- claim_job
@pytest.mark.parametrize(
    "claim_row, info, expected",
    [
        (("blob.xlsx",), None, processor.CLAIMED),
        (None, None, processor.MISSING),
        (None, ("COMPLETED", 1, False), processor.DONE),
        (None, ("FAILED", 3, True), processor.DONE),
        (None, ("PROCESSING", 1, False), processor.BUSY),        # someone is alive on it
        (None, ("QUEUED", 3, False), processor.EXHAUSTED),        # out of attempts
        (None, ("PROCESSING", 3, True), processor.EXHAUSTED),     # stale and out of attempts
    ],
)
def test_claim_job_outcomes(monkeypatch, claim_row, info, expected):
    rows = [claim_row] if claim_row is not None else [None, info]
    conn = use_conn(monkeypatch, FakeConn(rows))
    outcome, blob = processor.claim_job("job-1")
    assert outcome == expected
    assert (blob == "blob.xlsx") == (expected == processor.CLAIMED)
    assert conn.closed


def test_claim_sql_is_a_single_conditional_update(monkeypatch):
    conn = use_conn(monkeypatch, FakeConn([("blob.xlsx",)]))
    processor.claim_job("job-1")
    sql, params = conn.executed[0]
    assert sql.startswith("UPDATE upload_jobs") and "attempt_count < %s" in sql
    assert "status = 'QUEUED'" in sql and "status = 'PROCESSING'" in sql
    assert params == ("job-1", settings.MAX_ATTEMPTS, settings.CLAIM_STALE_SECONDS)


def test_exhausted_job_is_failed_and_partial_rows_removed(monkeypatch):
    conn = use_conn(monkeypatch, FakeConn([None, ("QUEUED", 3, False)]))
    assert processor.claim_job("job-1")[0] == processor.EXHAUSTED
    sqls = [s for s, _ in conn.executed]
    assert any("SET status = 'FAILED'" in s for s in sqls)
    assert any(s.startswith("DELETE FROM marathon_participants") for s in sqls)
    assert conn.commits == 1


# --------------------------------------------------------------------------- retry / fail
def test_retry_returns_status_and_attempts(monkeypatch):
    conn = use_conn(monkeypatch, FakeConn([("QUEUED", 1)]))
    assert processor.mark_retry_or_fail("job-1", "boom", permanent=False) == ("QUEUED", 1)
    assert not any(s.startswith("DELETE") for s, _ in conn.executed)


def test_final_failure_removes_partial_rows(monkeypatch):
    conn = use_conn(monkeypatch, FakeConn([("FAILED", 3)]))
    assert processor.mark_retry_or_fail("job-1", "boom", permanent=True) == ("FAILED", 3)
    assert any(s.startswith("DELETE FROM marathon_participants") for s, _ in conn.executed)


def test_already_finished_job_returns_none(monkeypatch):
    use_conn(monkeypatch, FakeConn([None]))
    assert processor.mark_retry_or_fail("job-1", "boom", permanent=False) is None


def test_error_message_is_truncated(monkeypatch):
    conn = use_conn(monkeypatch, FakeConn([("FAILED", 1)]))
    processor.mark_retry_or_fail("job-1", "x" * 5000, permanent=True)
    assert len(conn.executed[0][1]["msg"]) == 1000


def test_touch_job_only_touches_processing_jobs(monkeypatch):
    conn = use_conn(monkeypatch, FakeConn())
    processor.touch_job("job-1")
    sql, params = conn.executed[0]
    assert "status = 'PROCESSING'" in sql and params == ("job-1",)
    assert conn.commits == 1


# --------------------------------------------------------------------------- helpers
def test_blank_row_detection():
    assert processor._is_blank_row(None)
    assert processor._is_blank_row((None, "  ", ""))
    assert not processor._is_blank_row((0, None))  # a literal 0 is data


def test_short_rows_are_padded():
    d = processor._row_to_dict(("a", "b"))
    assert d["first_name"] == "a" and d["email"] is None and d["registration_fee"] is None


def test_estimate_total_rows_counts_data_rows(tmp_path):
    path = tmp_path / "tiny.xlsx"
    with zipfile.ZipFile(path, "w") as z:
        z.writestr("xl/worksheets/sheet1.xml", "<sheetData><row r='1'/><row r='2'/><row r='3'/><row r='4'/></sheetData>")
    assert processor.estimate_total_rows(str(path)) == 3  # header excluded


def test_estimate_total_rows_never_raises(tmp_path):
    path = tmp_path / "junk.xlsx"
    path.write_bytes(b"not a zip")
    assert processor.estimate_total_rows(str(path)) == 0


# --------------------------------------------------------------------------- _flush
def test_flush_turns_conflicts_into_duplicate_errors(monkeypatch):
    captured = []

    def fake_execute_values(cur, sql, rows, **kw):
        captured.append((sql, list(rows)))  # copy: the caller clears the list afterwards
        return [(2,), (3,)] if "marathon_participants" in sql else None

    monkeypatch.setattr(processor, "execute_values", fake_execute_values)
    conn, cur = MagicMock(), MagicMock()
    batch = [("J", 2, "a", "b", "e1", "B1", "5K", 1.0),
             ("J", 3, "a", "b", "e2", "B2", "5K", 1.0),
             ("J", 4, "a", "b", "e1", "B1", "5K", 1.0)]  # row 4 conflicts with row 2
    errors = [("J", 9, "invalid: bad email")]
    stats = Counters(processed=4, invalid=1)

    processor._flush(conn, cur, "J", batch, errors, stats)

    assert (stats.inserted, stats.duplicates, stats.invalid) == (2, 1, 1)
    assert batch == [] and errors == []
    error_rows = captured[1][1]
    assert ("J", 4, processor.DUPLICATE_MESSAGE) in error_rows and ("J", 9, "invalid: bad email") in error_rows
    conn.commit.assert_called_once()


def test_flush_with_nothing_still_saves_progress(monkeypatch):
    monkeypatch.setattr(processor, "execute_values", MagicMock(side_effect=AssertionError("no insert expected")))
    conn, cur = MagicMock(), MagicMock()
    processor._flush(conn, cur, "J", [], [], Counters(processed=5, invalid=5))
    conn.commit.assert_called_once()  # heartbeat/progress is saved even when every row was invalid


# --------------------------------------------------------------------------- end to end (needs real libs)
def _real_libs():
    import openpyxl
    import pydantic
    return hasattr(openpyxl, "Workbook") and hasattr(pydantic, "TypeAdapter")


@pytest.mark.skipif(not _real_libs(), reason="needs real openpyxl and pydantic")
def test_ingest_file_end_to_end_with_fake_db(monkeypatch, tmp_path):
    import openpyxl

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(["First Name", "Last Name", "Email", "Bib Number", "Category", "Registration Fee"])
    ws.append(["Ann", "Lee", "ann@marathon.com", 1001, "10K", 45])                 # row 2 valid
    ws.append(["Bad", "Email", "not-an-email", 1002, "5K", 20])                      # row 3 invalid
    ws.append(["Ann", "Lee", " ANN@marathon.com ", 1001.0, "10k", 45])               # row 4 duplicate of row 2
    ws.append([None, None, None, None, None, None])                                  # blank: skipped
    ws.append(["Cy", "Ng", "cy@marathon.com", "BIB-9", "FULL_MARATHON", 100])        # row 6 valid
    path = tmp_path / "small.xlsx"
    wb.save(path)

    seen, error_rows = set(), []

    def fake_execute_values(cur, sql, rows, **kw):
        if "marathon_participants" in sql:
            out = []
            for r in rows:  # emulate UNIQUE (email, bib_number) + ON CONFLICT DO NOTHING
                if (r[4], r[5]) not in seen:
                    seen.add((r[4], r[5]))
                    out.append((r[1],))
            return out
        error_rows.extend(list(rows))
        return None

    conn = use_conn(monkeypatch, FakeConn())
    monkeypatch.setattr(processor, "execute_values", fake_execute_values)
    monkeypatch.setattr(settings, "BATCH_SIZE", 2)  # force several flushes

    stats = processor.ingest_file(str(path), "J")

    assert (stats.processed, stats.inserted, stats.duplicates, stats.invalid) == (4, 2, 1, 1)
    assert sorted(r[1] for r in error_rows) == [3, 4]
    assert any("SET status = 'COMPLETED'" in s for s, _ in conn.executed)


def test_corrupt_workbook_is_a_permanent_error(monkeypatch, tmp_path):
    path = tmp_path / "bad.xlsx"
    path.write_bytes(b"PK\x03\x04" + b"garbage" * 50)  # passes the API's magic-byte check, not a real workbook
    conn = use_conn(monkeypatch, FakeConn())
    with pytest.raises(processor.PermanentJobError):
        processor.ingest_file(str(path), "J")
    assert conn.rollbacks == 1


# --------------------------------------------------------------------------- demo crash hook
class _Exited(Exception):
    pass


def _fake_exit(code):
    raise _Exited(code)


def test_crash_hook_is_off_by_default(monkeypatch):
    monkeypatch.setattr(processor.os, "_exit", _fake_exit)
    monkeypatch.setattr(settings, "CHAOS_CRASH_AFTER_ROWS", 0)
    use_conn(monkeypatch, FakeConn([(1,)]))
    processor._maybe_crash_for_demo("job-1", 10_000_000)   # must not exit


def test_crash_hook_waits_for_the_row_threshold(monkeypatch):
    monkeypatch.setattr(processor.os, "_exit", _fake_exit)
    monkeypatch.setattr(settings, "CHAOS_CRASH_AFTER_ROWS", 20000)
    use_conn(monkeypatch, FakeConn([(1,)]))
    processor._maybe_crash_for_demo("job-1", 19000)        # not yet


def test_crash_hook_kills_the_process_on_the_first_attempt(monkeypatch):
    monkeypatch.setattr(processor.os, "_exit", _fake_exit)
    monkeypatch.setattr(settings, "CHAOS_CRASH_AFTER_ROWS", 20000)
    use_conn(monkeypatch, FakeConn([(1,)]))
    with pytest.raises(_Exited) as exc:
        processor._maybe_crash_for_demo("job-1", 20000)
    assert exc.value.args == (137,)


def test_crash_hook_spares_the_retry(monkeypatch):
    monkeypatch.setattr(processor.os, "_exit", _fake_exit)
    monkeypatch.setattr(settings, "CHAOS_CRASH_AFTER_ROWS", 20000)
    use_conn(monkeypatch, FakeConn([(2,)]))                # second attempt
    processor._maybe_crash_for_demo("job-1", 50000)        # must not exit, so the job can finish
