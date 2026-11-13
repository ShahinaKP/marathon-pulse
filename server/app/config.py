"""Application configuration settings.

Centralizes environment variable management. The API runs locally during
development, but file storage, the queue and the database are all in Azure:
- Azure Database for PostgreSQL (DATABASE_URL, include ?sslmode=require)
- Azure Blob Storage (raw uploaded files)
- Azure Storage Queue (job notifications that trigger the worker)
"""
import os

from dotenv import load_dotenv

load_dotenv()


class Settings:
    DATABASE_URL: str = os.getenv(
        "DATABASE_URL",
        "postgresql://postgres:postgres@localhost:5432/marathon_pulse",
    )
    AZURE_STORAGE_CONNECTION_STRING: str = os.getenv("AZURE_STORAGE_CONNECTION_STRING", "")
    BLOB_CONTAINER_NAME: str = os.getenv("BLOB_CONTAINER_NAME", "excel-uploads")
    QUEUE_NAME: str = os.getenv("QUEUE_NAME", "excel-jobs")
    MAX_UPLOAD_MB: int = int(os.getenv("MAX_UPLOAD_MB", "50"))


settings = Settings()