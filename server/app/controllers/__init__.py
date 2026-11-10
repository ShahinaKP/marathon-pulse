"""HTTP Route Controllers."""

from app.controllers.health_controller import router as health_router
from app.controllers.upload_controller import router as upload_router

__all__ = ["health_router", "upload_router"]