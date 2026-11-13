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

    # --- reliability -------------------------------------------------------
    MAX_ATTEMPTS: int = _int("MAX_ATTEMPTS", 3)  # processing attempts per job
    # How long a received message stays hidden. Must exceed the longest expected job
    # until visibility renewal is added (Day 2).
    VISIBILITY_TIMEOUT_SECONDS: int = _int("VISIBILITY_TIMEOUT_SECONDS", 600)
    # A PROCESSING job whose heartbeat (updated_at) is older than this is considered dead.
    CLAIM_STALE_SECONDS: int = _int("CLAIM_STALE_SECONDS", 300)
    RETRY_DELAY_SECONDS: int = _int("RETRY_DELAY_SECONDS", 30)  # x attempt number
    BUSY_REQUEUE_SECONDS: int = _int("BUSY_REQUEUE_SECONDS", 60)
    MAX_DEQUEUE_COUNT: int = _int("MAX_DEQUEUE_COUNT", 10)  # backstop against poison messages
    POLL_INTERVAL_SECONDS: int = _int("POLL_INTERVAL_SECONDS", 2)

    def validate(self) -> None:
        if not self.AZURE_STORAGE_CONNECTION_STRING:
            raise RuntimeError("AZURE_STORAGE_CONNECTION_STRING is not set.")


settings = Settings()
