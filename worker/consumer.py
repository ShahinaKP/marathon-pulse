"""worker/consumer.py

Entry point of the cloud worker (the container's CMD).

Polls the Azure Storage Queue, claims one job at a time, processes it, and
decides what to do with the message:

  success                -> delete the message
  already done / unknown -> delete the message
  another worker busy    -> hide the message for a while, do not count an attempt
  transient failure      -> hide the message with a growing delay (retry)
  permanent / last try   -> job is FAILED, delete the message

The process never exits on its own when the queue is empty. KEDA scales the
replica count (down to zero) from the queue length; on scale-in the platform
sends SIGTERM, which this loop honours between jobs.
"""

import json
import logging
import signal
import threading
import uuid

from azure.storage.queue import QueueClient

import processor
from settings import settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("worker.consumer")

shutdown = threading.Event()


def _request_shutdown(signum, _frame) -> None:
    logger.info("Signal %s received - finishing the current job, then stopping.", signum)
    shutdown.set()


def _safe_delete(queue: QueueClient, msg) -> None:
    """Delete a message; a stale receipt (message already redelivered) is not fatal."""
    try:
        queue.delete_message(msg)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Could not delete message %s: %s", msg.id, exc)


def _safe_hide(queue: QueueClient, msg, seconds: int) -> None:
    try:
        queue.update_message(msg, visibility_timeout=seconds)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Could not reschedule message %s: %s", msg.id, exc)


def handle_message(queue: QueueClient, msg) -> None:
    # --- parse -----------------------------------------------------------
    try:
        job_id = json.loads(msg.content)["job_id"]
        uuid.UUID(job_id)
    except Exception:  # noqa: BLE001 - anything unparseable is unprocessable
        logger.error("Dropping malformed message %s: %r", msg.id, msg.content)
        _safe_delete(queue, msg)
        return

    # --- poison-message backstop ----------------------------------------
    if msg.dequeue_count > settings.MAX_DEQUEUE_COUNT:
        logger.error("Job %s delivered %d times - failing it.", job_id, msg.dequeue_count)
        processor.mark_retry_or_fail(job_id, "Message delivered too many times.", permanent=True)
        _safe_delete(queue, msg)
        return

    # --- claim -------------------------------------------------------------
    outcome, blob_name = processor.claim_job(job_id)
    if outcome == processor.BUSY:
        logger.info("Job %s is being processed elsewhere - rechecking later.", job_id)
        _safe_hide(queue, msg, settings.BUSY_REQUEUE_SECONDS)
        return
    if outcome != processor.CLAIMED:
        logger.info("Job %s not claimable (%s) - dropping message.", job_id, outcome)
        _safe_delete(queue, msg)
        return

    # --- process -----------------------------------------------------------
    logger.info("Job %s claimed (blob %s)", job_id, blob_name)
    try:
        processor.process_job(job_id, blob_name)
    except processor.PermanentJobError as exc:
        logger.error("Job %s failed permanently: %s", job_id, exc)
        _finish_failed_attempt(queue, msg, job_id, str(exc), permanent=True)
    except Exception as exc:  # noqa: BLE001
        logger.exception("Job %s attempt failed", job_id)
        _finish_failed_attempt(queue, msg, job_id, f"{type(exc).__name__}: {exc}", permanent=False)
    else:
        _safe_delete(queue, msg)


def _finish_failed_attempt(queue, msg, job_id: str, message: str, permanent: bool) -> None:
    try:
        new_status = processor.mark_retry_or_fail(job_id, message, permanent)
    except Exception:  # noqa: BLE001 - DB unreachable: leave the message to reappear
        logger.exception("Could not record failure for job %s; message will be redelivered.", job_id)
        return

    if new_status == "QUEUED":
        attempt = max(1, processor.get_attempt_count(job_id))
        delay = settings.RETRY_DELAY_SECONDS * attempt
        logger.info("Job %s will be retried in %ss (attempt %d used).", job_id, delay, attempt)
        _safe_hide(queue, msg, delay)
    else:
        logger.info("Job %s is FAILED - dropping message.", job_id)
        _safe_delete(queue, msg)


def main() -> None:
    settings.validate()
    signal.signal(signal.SIGTERM, _request_shutdown)
    signal.signal(signal.SIGINT, _request_shutdown)

    queue = QueueClient.from_connection_string(
        settings.AZURE_STORAGE_CONNECTION_STRING, settings.QUEUE_NAME
    )
    logger.info(
        "Worker started: queue=%s batch_size=%d max_attempts=%d",
        settings.QUEUE_NAME, settings.BATCH_SIZE, settings.MAX_ATTEMPTS,
    )

    while not shutdown.is_set():
        try:
            got_message = False
            for msg in queue.receive_messages(
                messages_per_page=1,
                visibility_timeout=settings.VISIBILITY_TIMEOUT_SECONDS,
            ):
                got_message = True
                handle_message(queue, msg)
                break  # one job at a time; loop around for the next
            if not got_message:
                shutdown.wait(settings.POLL_INTERVAL_SECONDS)
        except Exception:  # noqa: BLE001 - never die on a transient queue/network error
            logger.exception("Queue loop error; backing off.")
            shutdown.wait(5)

    logger.info("Worker stopped.")


if __name__ == "__main__":
    main()
