"""server/app/controllers/health_controller.py

Health check controller module.
Exposes diagnostic endpoints to verify service availability, operational health,
and API liveness for load balancers, orchestrators, and monitoring services.
"""

from datetime import datetime, timezone
from typing import Any, Dict

from fastapi import APIRouter

router = APIRouter(tags=["Health"])


@router.get(
    "/health",
    summary="Service Health Check",
    response_description="Returns the current operational status of the service.",
)
def health_check() -> Dict[str, Any]:
    """Verify that the API service is alive and responsive.

    Returns:
        dict: A payload containing current health status, the service identifier,
              and an ISO-8601 UTC timestamp.
    """
    return {
        "status": "healthy",
        "service": "marathon-pulse-api",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }