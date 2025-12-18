from __future__ import annotations

from fastapi import APIRouter

from services import app_state
from utils.fastapi_utils import asyncify


router = APIRouter()

router.add_api_route(
    "/v1/looks/generate", asyncify(app_state.looks_generate), methods=["POST"]
)
router.add_api_route(
    "/v1/looks/packs", asyncify(app_state.looks_packs), methods=["GET"]
)
router.add_api_route(
    "/v1/looks/apply_random",
    asyncify(app_state.looks_apply_random),
    methods=["POST"],
)
