from __future__ import annotations

from fastapi import APIRouter

from services import looks_service


router = APIRouter()

router.add_api_route(
    "/v1/looks/generate", looks_service.looks_generate, methods=["POST"]
)
router.add_api_route("/v1/looks/packs", looks_service.looks_packs, methods=["GET"])
router.add_api_route(
    "/v1/looks/apply_random",
    looks_service.looks_apply_random,
    methods=["POST"],
)
