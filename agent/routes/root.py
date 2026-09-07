from __future__ import annotations

from fastapi import APIRouter

from services import health_service


router = APIRouter()

router.add_api_route(
    "/api/", health_service.root, methods=["GET"], include_in_schema=False
)
router.add_api_route("/api/health", health_service.health, methods=["GET"])
router.add_api_route(
    "/api/livez", health_service.livez, methods=["GET"], include_in_schema=False
)
router.add_api_route("/api/readyz", health_service.readyz, methods=["GET"])
