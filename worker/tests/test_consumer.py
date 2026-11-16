"""Queue handling with a fake queue: success, retry, dead-letter, lease renewal, concurrency."""
import json
import threading
import time

import pytest

import consumer
import processor
from settings import settings

JOB = "11111111-1111-1111-1111-111111111111"


# --------------------------------------------------------------------------- fakes
class FakeMsg:
    def __init__(self, id="m1", content=None, dequeue_count=1, pop_receipt="r0"):
        self.id = id
        self.content = content if content is not None else json.dumps({"job_id": JOB, "blob_name": "b.xlsx"})
        self.dequeue_count = dequeue_count
        self.pop_receipt = pop_receipt


class FakeQueue:
    """Mimics the parts of QueueClient the worker uses, including pop-receipt rotation."""

    def __init__(self, messages=()):
        self.pending = list(messages)
        self.deleted = []   # (id, pop_receipt used)
        self.updates = []   # (id, pop_receipt used, visibility_timeout)
        self.sent = []      # dead-letter payloads
        self.receive_errors = 0
        self._counter = 0
        self._lock = threading.Lock()

    def receive_messages(self, **kwargs):
        if self.receive_errors > 0:
            self.receive_errors -= 1
            raise RuntimeError("queue unavailable")

        def gen():
            while True:
                with self._lock:
                    if not self.pending:
                        return
                    msg = self.pending.pop(0)
                yield msg

        return gen()

    def update_message(self, msg, visibility_timeout=None, **kw):
        with self._lock:
            if getattr(self, "fail_updates", False):
                raise RuntimeError("pop receipt expired")
            self.updates.append((msg.id, msg.pop_receipt, visibility_timeout))
            self._counter += 1
            return FakeMsg(msg.id, msg.content, msg.dequeue_count, f"r{self._counter}")

    def delete_message(self, msg):
        with self._lock:
            self.deleted.append((msg.id, msg.pop_receipt))

    def send_message(self, content):
        with self._lock:
            self.sent.append(json.loads(content))


@pytest.fixture
def env(monkeypatch):
    """Fast settings plus recorders for the processor functions the consumer calls."""
    monkeypatch.setattr(settings, "LEASE_RENEW_SECONDS", 0.02)
    monkeypatch.setattr(settings, "VISIBILITY_TIMEOUT_SECONDS", 1)
    monkeypatch.setattr(settings, "POLL_INTERVAL_SECONDS", 0.01)
    monkeypatch.setattr(settings, "ERROR_BACKOFF_SECONDS", 0.01)
    monkeypatch.setattr(settings, "RETRY_DELAY_SECONDS", 30)
    monkeypatch.setattr(settings, "BUSY_REQUEUE_SECONDS", 60)
    monkeypatch.setattr(settings, "MAX_DEQUEUE_COUNT", 10)

    state = {"claim": (processor.CLAIMED, "b.xlsx"), "process": lambda job_id, blob: None,
             "mark": ("FAILED", 1), "touches": 0, "mark_calls": []}

    monkeypatch.setattr(processor, "claim_job", lambda job_id: state["claim"])
    monkeypatch.setattr(processor, "process_job", lambda job_id, blob: state["process"](job_id, blob))

    def fake_mark(job_id, message, permanent):
        state["mark_calls"].append((job_id, message, permanent))
        if isinstance(state["mark"], Exception):
            raise state["mark"]
        return state["mark"]

    monkeypatch.setattr(processor, "mark_retry_or_fail", fake_mark)

    def fake_touch(job_id):
        state["touches"] += 1

    monkeypatch.setattr(processor, "touch_job", fake_touch)
    return state


def handle(queue, msg=None):
    poison = FakeQueue()
    msg = msg or FakeMsg()
    consumer.handle_message(queue, poison, msg)
    return poison


# --------------------------------------------------------------------------- outcomes
def test_success_deletes_the_message(env):
    q = FakeQueue()
    poison = handle(q)
    assert q.deleted == [("m1", "r0")] and q.updates == [] and poison.sent == []


def test_busy_job_is_rechecked_later_not_deleted(env):
    env["claim"] = (processor.BUSY, None)
    q = FakeQueue()
    handle(q)
    assert q.updates == [("m1", "r0", 60)] and q.deleted == []


@pytest.mark.parametrize("outcome", [processor.DONE, processor.MISSING])
def test_finished_or_unknown_job_message_is_dropped(env, outcome):
    env["claim"] = (outcome, None)
    q = FakeQueue()
    poison = handle(q)
    assert q.deleted == [("m1", "r0")] and poison.sent == []


def test_exhausted_job_is_dead_lettered(env):
    env["claim"] = (processor.EXHAUSTED, None)
    q = FakeQueue()
    poison = handle(q)
    assert q.deleted == [("m1", "r0")]
    assert poison.sent[0]["job_id"] == JOB and "attempts" in poison.sent[0]["reason"]


def test_permanent_error_fails_the_job_and_dead_letters(env):
    def boom(job_id, blob):
        raise processor.PermanentJobError("Not a readable .xlsx workbook")

    env["process"], env["mark"] = boom, ("FAILED", 1)
    q = FakeQueue()
    poison = handle(q)
    assert env["mark_calls"] == [(JOB, "Not a readable .xlsx workbook", True)]
    assert q.deleted and poison.sent[0]["reason"] == "Not a readable .xlsx workbook"


def test_transient_error_is_retried_with_growing_delay(env):
    def boom(job_id, blob):
        raise ConnectionError("db hiccup")

    env["process"], env["mark"] = boom, ("QUEUED", 2)
    q = FakeQueue()
    poison = handle(q)
    assert env["mark_calls"][0][2] is False
    assert q.updates[-1][2] == 30 * 2          # RETRY_DELAY x attempts used
    assert q.deleted == [] and poison.sent == []


def test_error_on_the_last_attempt_is_dead_lettered(env):
    def boom(job_id, blob):
        raise ConnectionError("still failing")

    env["process"], env["mark"] = boom, ("FAILED", 3)
    q = FakeQueue()
    poison = handle(q)
    assert q.deleted and poison.sent and "ConnectionError" in poison.sent[0]["reason"]


def test_job_already_finished_elsewhere_just_drops_the_message(env):
    def boom(job_id, blob):
        raise RuntimeError("x")

    env["process"], env["mark"] = boom, None
    q = FakeQueue()
    handle(q)
    assert q.deleted == [("m1", "r0")]


def test_if_the_failure_cannot_be_recorded_the_lease_is_left_to_expire(env):
    def boom(job_id, blob):
        raise RuntimeError("x")

    env["process"], env["mark"] = boom, ConnectionError("db down")
    q = FakeQueue()
    handle(q)
    assert q.deleted == []                                  # not deleted
    assert all(u[2] != settings.RETRY_DELAY_SECONDS for u in q.updates)  # and no retry scheduled


def test_malformed_message_is_dead_lettered(env):
    q = FakeQueue()
    poison = handle(q, FakeMsg(content="this is not json"))
    assert q.deleted and poison.sent[0]["job_id"] is None and env["mark_calls"] == []


def test_message_without_a_valid_job_id_is_dead_lettered(env):
    q = FakeQueue()
    poison = handle(q, FakeMsg(content=json.dumps({"job_id": "nope"})))
    assert q.deleted and poison.sent


def test_poison_message_backstop(env):
    q = FakeQueue()
    poison = handle(q, FakeMsg(dequeue_count=11))
    assert env["mark_calls"][0][2] is True and q.deleted and poison.sent
    assert "delivered 11 times" in poison.sent[0]["reason"]


# --------------------------------------------------------------------------- lease renewal
def test_lease_is_renewed_while_a_job_runs_and_the_latest_receipt_is_used(env):
    def slow(job_id, blob):
        time.sleep(0.2)

    env["process"] = slow
    q = FakeQueue()
    handle(q)
    assert len(q.updates) >= 2                          # renewed repeatedly during the 0.2s job
    assert env["touches"] >= 2                          # heartbeat sent alongside
    receipts_used = [u[1] for u in q.updates]
    assert receipts_used[0] == "r0" and receipts_used[1] != "r0"   # each renewal used the newest receipt
    assert q.deleted[0][1] == f"r{len(q.updates)}"      # delete used the receipt from the last renewal
    assert all(u[2] == settings.VISIBILITY_TIMEOUT_SECONDS for u in q.updates)


def test_failed_renewal_does_not_crash_the_job(env):
    def slow(job_id, blob):
        time.sleep(0.1)

    env["process"] = slow
    q = FakeQueue()
    q.fail_updates = True
    handle(q)
    assert q.deleted and env["touches"] >= 1            # job finished and was still heartbeating


def test_lease_keeper_stops_when_the_job_ends(env):
    q = FakeQueue()
    handle(q)
    count = len(q.updates)
    time.sleep(0.1)
    assert len(q.updates) == count


# --------------------------------------------------------------------------- main loop
def run_until_drained(monkeypatch, q, concurrency, job_seconds, expected):
    monkeypatch.setattr(settings, "MAX_CONCURRENT_JOBS", concurrency)
    stop = threading.Event()
    active = {"now": 0, "max": 0}
    lock = threading.Lock()

    def tracked(job_id, blob):
        with lock:
            active["now"] += 1
            active["max"] = max(active["max"], active["now"])
        time.sleep(job_seconds)
        with lock:
            active["now"] -= 1

    monkeypatch.setattr(processor, "process_job", tracked)
    poison = FakeQueue()
    thread = threading.Thread(target=consumer.run_loop, args=(q, poison, stop))
    thread.start()
    deadline = time.time() + 15
    while len(q.deleted) < expected and time.time() < deadline:
        time.sleep(0.02)
    stop.set()
    thread.join(timeout=10)
    assert not thread.is_alive()
    return active["max"]


def messages(n):
    return [FakeMsg(id=f"m{i}") for i in range(n)]


def test_runs_jobs_in_parallel_up_to_the_limit(env, monkeypatch):
    q = FakeQueue(messages(6))
    peak = run_until_drained(monkeypatch, q, concurrency=2, job_seconds=0.15, expected=6)
    assert len(q.deleted) == 6 and peak == 2


def test_concurrency_of_one_runs_jobs_one_at_a_time(env, monkeypatch):
    q = FakeQueue(messages(3))
    peak = run_until_drained(monkeypatch, q, concurrency=1, job_seconds=0.05, expected=3)
    assert len(q.deleted) == 3 and peak == 1


def test_loop_survives_queue_errors(env, monkeypatch):
    q = FakeQueue(messages(2))
    q.receive_errors = 2
    peak = run_until_drained(monkeypatch, q, concurrency=1, job_seconds=0.01, expected=2)
    assert len(q.deleted) == 2


def test_messages_are_not_received_until_a_worker_slot_is_free(env, monkeypatch):
    """A message that is received but not yet running has no lease renewal and would expire.

    So the loop must leave messages on the queue while every slot is busy."""
    monkeypatch.setattr(settings, "MAX_CONCURRENT_JOBS", 1)
    q = FakeQueue(messages(3))
    stop = threading.Event()

    def slow(job_id, blob):
        time.sleep(0.4)

    monkeypatch.setattr(processor, "process_job", slow)
    thread = threading.Thread(target=consumer.run_loop, args=(q, FakeQueue(), stop))
    thread.start()
    time.sleep(0.2)                 # first job is running
    still_on_queue = len(q.pending)
    stop.set()
    thread.join(timeout=5)
    assert still_on_queue == 2      # only one message was taken off the queue


def test_shutdown_waits_for_the_running_job(env, monkeypatch):
    monkeypatch.setattr(settings, "MAX_CONCURRENT_JOBS", 1)
    q = FakeQueue(messages(1))
    stop = threading.Event()
    finished = threading.Event()

    def slow(job_id, blob):
        time.sleep(0.3)
        finished.set()

    monkeypatch.setattr(processor, "process_job", slow)
    thread = threading.Thread(target=consumer.run_loop, args=(q, FakeQueue(), stop))
    thread.start()
    time.sleep(0.1)          # job is now running
    stop.set()               # simulate SIGTERM
    thread.join(timeout=5)
    assert finished.is_set() and q.deleted   # the loop did not return until the job completed


def test_unhandled_error_in_a_job_does_not_kill_the_loop(env, monkeypatch):
    def explode(*a, **k):
        raise RuntimeError("bug")

    monkeypatch.setattr(consumer, "handle_message", explode)
    monkeypatch.setattr(settings, "MAX_CONCURRENT_JOBS", 1)
    q = FakeQueue(messages(2))
    stop = threading.Event()
    thread = threading.Thread(target=consumer.run_loop, args=(q, FakeQueue(), stop))
    thread.start()
    deadline = time.time() + 5
    while q.pending and time.time() < deadline:
        time.sleep(0.02)
    stop.set()
    thread.join(timeout=5)
    assert not q.pending and not thread.is_alive()


# --------------------------------------------------------------------------- settings guard rails
def test_validate_rejects_unsafe_lease_settings(monkeypatch):
    monkeypatch.setattr(settings, "AZURE_STORAGE_CONNECTION_STRING", "x")
    monkeypatch.setattr(settings, "LEASE_RENEW_SECONDS", 60)
    monkeypatch.setattr(settings, "VISIBILITY_TIMEOUT_SECONDS", 90)
    with pytest.raises(RuntimeError, match="VISIBILITY_TIMEOUT_SECONDS"):
        settings.validate()


def test_validate_accepts_the_defaults(monkeypatch):
    monkeypatch.setattr(settings, "AZURE_STORAGE_CONNECTION_STRING", "x")
    for name, value in [("LEASE_RENEW_SECONDS", 20), ("VISIBILITY_TIMEOUT_SECONDS", 90),
                        ("CLAIM_STALE_SECONDS", 60), ("MAX_CONCURRENT_JOBS", 1), ("MAX_ATTEMPTS", 3)]:
        monkeypatch.setattr(settings, name, value)
    settings.validate()
