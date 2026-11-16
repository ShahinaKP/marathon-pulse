"""worker/settings.py

All worker tuning lives in environment variables so the same image runs
locally and in Azure Container Apps without code changes.
"""
import os

from dotenv import load_dotenv

load_dotenv()


def _int(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)))


class Settings:
    # --- connections -------------------------------------------------------
    DATABASE_URL: str = os.getenv(
        "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/marathon_pulse"
    )
    AZURE_STORAGE_CONNECTION_STRING: str = os.getenv("AZURE_STORAGE_CONNECTION_STRING", "")
    BLOB_CONTAINER_NAME: str = os.getenv("BLOB_CONTAINER_NAME", "excel-uploads")
    QUEUE_NAME: str = os.getenv("QUEUE_NAME", "excel-jobs")

    # --- processing --------------------------------------------------------
    BATCH_SIZE: int = _int("BATCH_SIZE", 1000)  # rows read per DB flush / progress update

    # --- concurrency -------------------------------------------------------
    # Jobs processed in parallel inside ONE replica (a thread per job). Scale-out across
    # replicas is KEDA's job (see QUEUE_LENGTH in devops/provision.sh).
    MAX_CONCURRENT_JOBS: int = _int("MAX_CONCURRENT_JOBS", 1)

    # --- reliability -------------------------------------------------------
    MAX_ATTEMPTS: int = _int("MAX_ATTEMPTS", 3)  # processing attempts per job
    # A received message stays hidden for this long. A running job renews it every
    # LEASE_RENEW_SECONDS, so long files never time out; if the worker dies, the message
    # reappears after at most this long.
    VISIBILITY_TIMEOUT_SECONDS: int = _int("VISIBILITY_TIMEOUT_SECONDS", 90)
    LEASE_RENEW_SECONDS: int = _int("LEASE_RENEW_SECONDS", 20)
    # A PROCESSING job whose heartbeat (updated_at) is older than this is considered dead
    # and may be claimed by another worker.
    CLAIM_STALE_SECONDS: int = _int("CLAIM_STALE_SECONDS", 60)
    RETRY_DELAY_SECONDS: int = _int("RETRY_DELAY_SECONDS", 30)  # x attempt number
    BUSY_REQUEUE_SECONDS: int = _int("BUSY_REQUEUE_SECONDS", 60)
    MAX_DEQUEUE_COUNT: int = _int("MAX_DEQUEUE_COUNT", 10)  # backstop against poison messages
    POLL_INTERVAL_SECONDS: int = _int("POLL_INTERVAL_SECONDS", 2)
    ERROR_BACKOFF_SECONDS: int = _int("ERROR_BACKOFF_SECONDS", 5)  # pause after a queue error
    # Dead-letter queue: every job that ends FAILED (and every unreadable message) is copied
    # here for inspection or replay.
    POISON_QUEUE_NAME: str = os.getenv("POISON_QUEUE_NAME", "excel-jobs-poison")

    # DEMO/TEST ONLY. 0 = off. When set (e.g. 20000), the worker hard-kills itself once, after
    # that many rows, on a job's FIRST attempt - to prove crash recovery. See devops/provision.sh chaos.
    CHAOS_CRASH_AFTER_ROWS: int = _int("CHAOS_CRASH_AFTER_ROWS", 0)

    def validate(self) -> None:
        if not self.AZURE_STORAGE_CONNECTION_STRING:
            raise RuntimeError("AZURE_STORAGE_CONNECTION_STRING is not set.")
        if self.MAX_CONCURRENT_JOBS < 1:
            raise RuntimeError("MAX_CONCURRENT_JOBS must be at least 1.")
        if self.MAX_ATTEMPTS < 1:
            raise RuntimeError("MAX_ATTEMPTS must be at least 1.")
        if self.VISIBILITY_TIMEOUT_SECONDS <= 2 * self.LEASE_RENEW_SECONDS:
            raise RuntimeError(
                "VISIBILITY_TIMEOUT_SECONDS must be more than twice LEASE_RENEW_SECONDS "
                "so a renewal can fail once without losing the message."
            )
        if self.CLAIM_STALE_SECONDS <= 2 * self.LEASE_RENEW_SECONDS:
            raise RuntimeError(
                "CLAIM_STALE_SECONDS must be more than twice LEASE_RENEW_SECONDS "
                "or a healthy job could be taken over."
            )


settings = Settings()
