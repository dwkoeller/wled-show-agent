from __future__ import annotations

from fastapi import APIRouter

from services import app_state
from utils.fastapi_utils import asyncify


router = APIRouter()

router.add_api_route(
    "/v1/show/config/load", asyncify(app_state.show_config_load), methods=["POST"]
)
router.add_api_route(
    "/v1/xlights/import_networks",
    asyncify(app_state.xlights_import_networks),
    methods=["POST"],
)
router.add_api_route(
    "/v1/xlights/import_project",
    asyncify(app_state.xlights_import_project),
    methods=["POST"],
)
router.add_api_route(
    "/v1/xlights/import_sequence",
    asyncify(app_state.xlights_import_sequence),
    methods=["POST"],
)
