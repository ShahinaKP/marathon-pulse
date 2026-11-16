"""worker/consumer.py

Entry point of the cloud worker (the container's CMD).

Polls the Azure Storage Queue and processes up to MAX_CONCURRENT_JOBS jobs at the
same time (one thread per job). For every message it decides:

  success                 -> delete the message
  already done / unknown  -> delete the message
  another worker busy     -> hide the message for a while (no attempt is used)
  transient failure       -> hide the message with a growing delay (retry)
  permanent / last try    -> job is FAILED, copy to the dead-letter queue, delete

While a job runs, a lease keeper renews the message's invisibility and the job's
heartbeat on a timer. That is what stops a long file from being redelivered to a second
worker, and what lets a second worker take over quickly when this one dies.

The process never exits on its own when the queue is empty: KEDA scales replicas (down to
zero) from the queue length. On scale-in the platform sends SIGTERM; the loop stops taking
new messages and waits for running jobs to finish. If the platform kills the process
anyway, the lease expires and the job is retried elsewhere, which is safe because every
attempt starts from a clean slate.
"""

import json
import logging
import signal
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager

from azure.core.exceptions import ResourceExistsError
from azure.storage.queue import QueueClient

import processor
from settings import settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(threadName)s %(name)s: %(message)s",
)
logger = logging.getLogger("worker.consumer")

shutdown = threading.Event()


def _request_shutdown(signum, _frame) -> None:
    logger.info("Signal %s received - taking no new jobs; waiting for running jobs.", signum)
    shutdown.set()


# ---------------------------------------------------------------------------
# Queue message with a renewable lease
# ---------------------------------------------------------------------------
class LeasedMessage:
    """A received queue message plus the bookkeeping needed to keep using it.

    Every time the lease is extended Azure issues a NEW pop receipt and the old one stops
    working, so the latest message object must be used for every later call. This class
    owns that, and is thread-safe because the lease keeper and the job thread share it.
    """

    def __init__(self, queue: QueueClient, msg) -> None:
        self._queue = queue
        self._msg = msg
        self._lock = threading.Lock()
        self.id = msg.id
        self.content = msg.content
        self.dequeue_count = msg.dequeue_count

    def renew(self, seconds: int) -> bool:
        """Set the message invisible for `seconds` from now. False if it is no longer ours."""
        with self._lock:
            try:
                updated = self._queue.update_message(self._msg, visibility_timeout=seconds)
            except Exception as exc:  # noqa: BLE001 - e.g. receipt expired: someone else has it
                logger.warning("Could not extend lease on message %s: %s", self.id, exc)
                return False
            if getattr(updated, "pop_receipt", None):
                self._msg = updated
            return True

    def hide(self, seconds: int) -> bool:
        """Make the message reappear after `seconds` (used for retries and 'busy')."""
        return self.renew(seconds)

    def delete(self) -> None:
        with self._lock:
            try:
                self._queue.delete_message(self._msg)
            except Exception as exc:  # noqa: BLE001
                logger.warning("Could not delete message %s: %s", self.id, exc)


def _renew_loop(leased: LeasedMessage, job_id: str, stop: threading.Event) -> None:
    while not stop.wait(settings.LEASE_RENEW_SECONDS):
        leased.renew(settings.VISIBILITY_TIMEOUT_SECONDS)
        try:
            processor.touch_job(job_id)
        except Exception as exc:  # noqa: BLE001 - one missed heartbeat is survivable
            logger.warning("Heartbeat failed for job %s: %s", job_id, exc)


@contextmanager
def keep_alive(leased: LeasedMessage, job_id: str):
    """Renew the queue lease and the job heartbeat in the background while the body runs."""
    stop = threading.Event()
    thread = threading.Thread(
        target=_renew_loop, args=(leased, job_id, stop), name=f"lease-{job_id[:8]}", daemon=True
    )
    thread.start()
    try:
        yield
    finally:
        stop.set()
        thread.join(timeout=10)


# ---------------------------------------------------------------------------
# Message handling
# ---------------------------------------------------------------------------
def _dead_letter(poison: QueueClient, leased: LeasedMessage, job_id, reason: str) -> None:
    """Keep a copy of a failed job's message for inspection or replay."""
    try:
        poison.send_message(
            json.dumps(
                {
                    "job_id": job_id,
                    "reason": reason[:500],
                    "dequeue_count": leased.dequeue_count,
                    "original": leased.content,
                }
            )
        )
    except Exception:  # noqa: BLE001 - never let the DLQ block normal handling
        logger.exception("Could not write message %s to the dead-letter queue", leased.id)


def handle_message(queue: QueueClient, poison: QueueClient, raw) -> None:
    leased = LeasedMessage(queue, raw)

    # --- parse -----------------------------------------------------------
    try:
        job_id = json.loads(leased.content)["job_id"]
        uuid.UUID(job_id)
    except Exception:  # noqa: BLE001 - anything unparseable is unprocessable
        logger.error("Dropping malformed message %s: %r", leased.id, leased.content)
        _dead_letter(poison, leased, None, "Malformed message")
        leased.delete()
        return

    # --- poison-message backstop ----------------------------------------
    if leased.dequeue_count > settings.MAX_DEQUEUE_COUNT:
        reason = f"Message delivered {leased.dequeue_count} times."
        logger.error("Job %s: %s Failing it.", job_id, reason)
        processor.mark_retry_or_fail(job_id, reason, permanent=True)
        _dead_letter(poison, leased, job_id, reason)
        leased.delete()
        return

    # --- claim -------------------------------------------------------------
    outcome, blob_name = processor.claim_job(job_id)
    if outcome == processor.BUSY:
        logger.info("Job %s is being processed elsewhere - rechecking later.", job_id)
        leased.hide(settings.BUSY_REQUEUE_SECONDS)
        return
    if outcome == processor.EXHAUSTED:
        logger.error("Job %s used all %d attempts - FAILED.", job_id, settings.MAX_ATTEMPTS)
        _dead_letter(poison, leased, job_id, "Exceeded maximum processing attempts.")
        leased.delete()
        return
    if outcome != processor.CLAIMED:
        logger.info("Job %s not claimable (%s) - dropping message.", job_id, outcome)
        leased.delete()
        return

    # --- process (lease and heartbeat are renewed in the background) ----------
    logger.info("Job %s claimed (blob %s)", job_id, blob_name)
    error = None
    permanent = False
    try:
        with keep_alive(leased, job_id):
            processor.process_job(job_id, blob_name)
    except processor.PermanentJobError as exc:
        logger.error("Job %s failed permanently: %s", job_id, exc)
        error, permanent = str(exc), True
    except Exception as exc:  # noqa: BLE001
        logger.exception("Job %s attempt failed", job_id)
        error = f"{type(exc).__name__}: {exc}"

    if error is None:
        leased.delete()
        return
    _finish_failed_attempt(leased, poison, job_id, error, permanent)


def _finish_failed_attempt(
    leased: LeasedMessage, poison: QueueClient, job_id: str, error: str, permanent: bool
) -> None:
    try:
        result = processor.mark_retry_or_fail(job_id, error, permanent)
    except Exception:  # noqa: BLE001 - DB unreachable: leave the lease to expire and be redelivered
        logger.exception("Could not record failure for job %s; it will be redelivered.", job_id)
        return

    if result is None:  # job was already finished by someone else
        leased.delete()
        return

    status, attempts = result
    if status == "QUEUED":
        delay = settings.RETRY_DELAY_SECONDS * max(1, attempts)
        logger.info("Job %s will be retried in %ss (attempt %d used).", job_id, delay, attempts)
        leased.hide(delay)
    else:
        logger.info("Job %s is FAILED - dead-lettering the message.", job_id)
        _dead_letter(poison, leased, job_id, error)
        leased.delete()


def _run_one(queue: QueueClient, poison: QueueClient, raw) -> None:
    try:
        handle_message(queue, poison, raw)
    except Exception:  # noqa: BLE001 - the lease will expire and the message reappear
        logger.exception("Unhandled error for message %s", getattr(raw, "id", "?"))


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------
def run_loop(queue: QueueClient, poison: QueueClient, stop: threading.Event) -> None:
    """Receive messages until `stop` is set, running up to MAX_CONCURRENT_JOBS at once."""
    slots = threading.BoundedSemaphore(settings.MAX_CONCURRENT_JOBS)
    pool = ThreadPoolExecutor(max_workers=settings.MAX_CONCURRENT_JOBS, thread_name_prefix="job")
    try:
        while not stop.is_set():
            if not slots.acquire(timeout=1):
                continue  # every slot is busy
            submitted = False
            try:
                for raw in queue.receive_messages(
                    messages_per_page=1,
                    visibility_timeout=settings.VISIBILITY_TIMEOUT_SECONDS,
                ):
                    future = pool.submit(_run_one, queue, poison, raw)
                    submitted = True
                    future.add_done_callback(lambda _f: slots.release())
                    break  # one message per receive; the loop comes back for more
            except Exception:  # noqa: BLE001 - never die on a transient queue/network error
                logger.exception("Queue loop error; backing off.")
                stop.wait(settings.ERROR_BACKOFF_SECONDS)
            finally:
                if not submitted:
                    slots.release()
            if not submitted:
                stop.wait(settings.POLL_INTERVAL_SECONDS)
    finally:
        logger.info("Waiting for running jobs to finish...")
        pool.shutdown(wait=True)


def main() -> None:
    settings.validate()
    signal.signal(signal.SIGTERM, _request_shutdown)
    signal.signal(signal.SIGINT, _request_shutdown)

    conn = settings.AZURE_STORAGE_CONNECTION_STRING
    queue = QueueClient.from_connection_string(conn, settings.QUEUE_NAME)
    poison = QueueClient.from_connection_string(conn, settings.POISON_QUEUE_NAME)
    try:
        poison.create_queue()
    except ResourceExistsError:
        pass
    except Exception as exc:  # noqa: BLE001
        logger.warning("Could not ensure dead-letter queue exists: %s", exc)

    logger.info(
        "Worker started: queue=%s concurrency=%d batch_size=%d max_attempts=%d "
        "lease=%ss/%ss stale_after=%ss",
        settings.QUEUE_NAME, settings.MAX_CONCURRENT_JOBS, settings.BATCH_SIZE,
        settings.MAX_ATTEMPTS, settings.VISIBILITY_TIMEOUT_SECONDS,
        settings.LEASE_RENEW_SECONDS, settings.CLAIM_STALE_SECONDS,
    )
    run_loop(queue, poison, shutdown)
    logger.info("Worker stopped.")


if __name__ == "__main__":
    main()
