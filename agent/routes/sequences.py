from __future__ import annotations

from fastapi import APIRouter

from services import sequences_service


router = APIRouter()

router.add_api_route(
    "/v1/sequences/list", sequences_service.sequences_list, methods=["GET"]
)
router.add_api_route(
    "/v1/sequences/generate",
    sequences_service.sequences_generate,
    methods=["POST"],
)
router.add_api_route(
    "/v1/sequences/status", sequences_service.sequences_status, methods=["GET"]
)
router.add_api_route(
    "/v1/sequences/preview", sequences_service.sequences_preview, methods=["GET"]
)
router.add_api_route(
    "/v1/sequences/preview/cache",
    sequences_service.sequences_preview_cache,
    methods=["GET"],
)
router.add_api_route(
    "/v1/sequences/preview/purge",
    sequences_service.sequences_preview_purge,
    methods=["POST"],
)
router.add_api_route(
    "/v1/sequences/play", sequences_service.sequences_play, methods=["POST"]
)
router.add_api_route(
    "/v1/sequences/stop", sequences_service.sequences_stop, methods=["POST"]
)
