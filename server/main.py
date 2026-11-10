"""server/main.py

Application entry point.
Initializes the FastAPI service, configures Cross-Origin Resource Sharing (CORS)
for the frontend client, and mounts controller sub-routers.
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.controllers import health_controller, upload_controller

# Initialize primary FastAPI application instance with OpenAPI metadata
app = FastAPI(
    title="Marathon Pulse API",
    version="1.0.0",
    description="Batch ingestion engine for marathon runner dataset registrations.",
)

# Configure Cross-Origin Resource Sharing (CORS) for Angular client
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:4200"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register route controllers
app.include_router(health_controller.router)
app.include_router(upload_controller.router)


if __name__ == "__main__":
    import uvicorn

    # Run local development ASGI server with live reloading
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)
