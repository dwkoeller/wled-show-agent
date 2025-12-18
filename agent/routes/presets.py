from __future__ import annotations

from fastapi import APIRouter

from services import presets_service


router = APIRouter()

router.add_api_route(
    "/v1/presets/import_from_pack",
    presets_service.presets_import,
    methods=["POST"],
)
