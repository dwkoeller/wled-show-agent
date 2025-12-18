from __future__ import annotations

from fastapi import APIRouter

from services import segments_service


router = APIRouter()

router.add_api_route(
    "/v1/segments/layout", segments_service.segments_layout, methods=["GET"]
)
router.add_api_route(
    "/v1/segments/orientation",
    segments_service.segments_orientation,
    methods=["GET"],
)
