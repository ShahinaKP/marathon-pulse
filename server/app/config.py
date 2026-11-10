"""Application configuration settings.

Centralizes environment variable management and application constants.
Loads connection strings and configuration for:
- PostgreSQL database
- Azure Blob Storage (file uploads)
- Azure Service Bus (asynchronous job queuing)
"""
import os
from dotenv import load_dotenv

load_dotenv()


class Settings:
    DATABASE_URL: str = os.getenv(
        "DATABASE_URL", 
        "postgresql://postgres:postgres@localhost:5432/marathon_pulse"
    )
    AZURE_STORAGE_CONNECTION_STRING: str = os.getenv("AZURE_STORAGE_CONNECTION_STRING", "")
    SERVICEBUS_CONNECTION_STRING: str = os.getenv("SERVICEBUS_CONNECTION_STRING", "local")
    BLOB_CONTAINER_NAME: str = "excel-uploads"
    SERVICEBUS_QUEUE_NAME: str = "excel-jobs"


settings = Settings()