"""server/app/services/queue_service.py

Azure Service Bus integration service.
Handles dispatching asynchronous job notifications to the configured message queue.
Supports an offline/local development mode that simulates queue dispatching
via console output and logging without making cloud network calls.
"""

import json
import logging
from app.config import settings

logger = logging.getLogger("queue_service")


class QueueService:
    """Service wrapper for producing messages to Azure Service Bus."""

    def __init__(self):
        """Initialize queue connection settings and Azure client.

        Evaluates SERVICEBUS_CONNECTION_STRING from settings. If empty or
        explicitly set to 'local', enters mock dispatch mode; otherwise,
        instantiates an active ServiceBusClient.
        """
        self.conn_str = settings.SERVICEBUS_CONNECTION_STRING
        self.queue_name = settings.SERVICEBUS_QUEUE_NAME
        self.is_local = not self.conn_str or self.conn_str.lower() == "local"
        self.client = None

        if not self.is_local:
            # Lazy import to avoid import errors in environments without the SDK
            from azure.servicebus import ServiceBusClient

            self.client = ServiceBusClient.from_connection_string(self.conn_str)

    def enqueue_job(self, job_id: str, blob_name: str) -> None:
        """Publish a background processing job event to the target queue.

        Args:
            job_id: Unique identifier of the processing task.
            blob_name: Name/path of the uploaded file stored in Azure Blob Storage.
        """
        payload = json.dumps({"job_id": job_id, "blob_name": blob_name})

        if self.is_local:
            logger.info(f"[LOCAL QUEUE] Event sent to '{self.queue_name}': {payload}")
            print(f"\n---> [LOCAL QUEUE] Event sent to '{self.queue_name}': {payload}\n")
            return

        from azure.servicebus import ServiceBusMessage

        # Context manager ensures sender teardown and socket disposal after dispatch
        with self.client.get_queue_sender(queue_name=self.queue_name) as sender:
            sender.send_messages(ServiceBusMessage(payload))