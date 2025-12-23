from __future__ import annotations

from fastapi import APIRouter

from services import health_service


router = APIRouter()

router.add_api_route(
    "/", health_service.root, methods=["GET"], include_in_schema=False
)
router.add_api_route("/v1/health", health_service.health, methods=["GET"])
router.add_api_route(
    "/livez", health_service.livez, methods=["GET"], include_in_schema=False
)
router.add_api_route("/readyz", health_service.readyz, methods=["GET"])
