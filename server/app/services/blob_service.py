"""server/app/services/blob_service.py

Azure Blob Storage integration service.
Handles initialization, container provisioning, and chunked streaming of
binary file payloads (such as FastAPI's SpooledTemporaryFile) directly to
Azure Blob Storage without loading the full file into system RAM.
"""

from typing import BinaryIO
from azure.storage.blob import BlobServiceClient
from app.config import settings


class BlobService:
    """Service wrapper for Azure Storage Blob SDK operations."""

    def __init__(self):
        """Initialize Azure Blob Storage client and verify target container.

        Reads connection credentials from application settings. If credentials
        are absent or fail during initialization, self.client remains None,
        falling back to local mock references.
        """
        self.conn_str = settings.AZURE_STORAGE_CONNECTION_STRING
        self.container_name = settings.BLOB_CONTAINER_NAME
        self.client = None

        if self.conn_str:
            try:
                self.client = BlobServiceClient.from_connection_string(self.conn_str)
                container_client = self.client.get_container_client(self.container_name)
                if not container_client.exists():
                    container_client.create_container()
            except Exception:
                # Silently catch initialization errors (e.g., offline credentials,
                # network timeouts) to allow local fallback execution.
                pass

    def upload_stream(self, blob_name: str, stream: BinaryIO) -> str:
        """Stream an open binary file directly to Azure Blob Storage.

        Args:
            blob_name: Target path/filename in the destination container.
            stream: Readable binary stream (e.g., file-like object, SpooledTemporaryFile).

        Returns:
            str: Public or internal HTTPS URL of the uploaded blob if client
                 is connected, otherwise a pseudo URI formatted as
                 `local://{container_name}/{blob_name}`.
        """
        if self.client:
            blob_client = self.client.get_blob_client(
                container=self.container_name, 
                blob=blob_name
            )
            blob_client.upload_blob(stream, overwrite=True)
            return blob_client.url

        return f"local://{self.container_name}/{blob_name}"