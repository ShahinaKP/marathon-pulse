"""server/app/services/blob_service.py

Azure Blob Storage integration. Streams the uploaded file to a blob without
loading it into RAM. There is deliberately NO local fallback: if storage is
not configured or unreachable the upload fails loudly instead of returning a
success for a file that was never stored.
"""

from functools import lru_cache
from typing import BinaryIO

from azure.storage.blob import BlobServiceClient

from app.config import settings


@lru_cache(maxsize=1)
def _container_client():
    """Create (once per process) the container client and ensure the container exists."""
    if not settings.AZURE_STORAGE_CONNECTION_STRING:
        raise RuntimeError("AZURE_STORAGE_CONNECTION_STRING is not set.")
    service = BlobServiceClient.from_connection_string(settings.AZURE_STORAGE_CONNECTION_STRING)
    container = service.get_container_client(settings.BLOB_CONTAINER_NAME)
    if not container.exists():
        container.create_container()
    return container


class BlobService:
    """Thin wrapper over the Azure Blob SDK (synchronous - call from a worker thread)."""

    def upload_stream(self, blob_name: str, stream: BinaryIO) -> None:
        """Upload an open binary stream to `blob_name`. Raises on any failure."""
        _container_client().upload_blob(name=blob_name, data=stream, overwrite=True)
