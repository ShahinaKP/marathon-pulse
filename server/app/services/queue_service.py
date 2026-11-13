"""server/app/services/queue_service.py

Azure Storage Queue producer. Publishes one message per job; the message is
what KEDA watches to start the Container App worker. No local mock mode:
if the message cannot be sent, the caller marks the job FAILED.
"""

import json
from functools import lru_cache

from azure.core.exceptions import ResourceExistsError
from azure.storage.queue import QueueClient

from app.config import settings


@lru_cache(maxsize=1)
def _queue_client() -> QueueClient:
    if not settings.AZURE_STORAGE_CONNECTION_STRING:
        raise RuntimeError("AZURE_STORAGE_CONNECTION_STRING is not set.")
    client = QueueClient.from_connection_string(
        settings.AZURE_STORAGE_CONNECTION_STRING, settings.QUEUE_NAME
    )
    try:
        client.create_queue()
    except ResourceExistsError:
        pass
    return client


class QueueService:
    """Wrapper for sending job notifications (synchronous - call from a worker thread)."""

    def enqueue_job(self, job_id: str, blob_name: str) -> None:
        """Send {"job_id", "blob_name"} to the queue. Raises on failure."""
        _queue_client().send_message(json.dumps({"job_id": job_id, "blob_name": blob_name}))
