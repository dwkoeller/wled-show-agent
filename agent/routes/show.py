from __future__ import annotations

from fastapi import APIRouter

from services import show_service


router = APIRouter()

router.add_api_route(
    "/v1/show/config/load", show_service.show_config_load, methods=["POST"]
)
router.add_api_route(
    "/v1/xlights/import_networks",
    show_service.xlights_import_networks,
    methods=["POST"],
)
router.add_api_route(
    "/v1/xlights/import_project",
    show_service.xlights_import_project,
    methods=["POST"],
)
router.add_api_route(
    "/v1/xlights/import_sequence",
    show_service.xlights_import_sequence,
    methods=["POST"],
)
