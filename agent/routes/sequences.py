from __future__ import annotations

from fastapi import APIRouter

from services import app_state
from utils.fastapi_utils import asyncify


router = APIRouter()

router.add_api_route(
    "/v1/sequences/list", asyncify(app_state.sequences_list), methods=["GET"]
)
router.add_api_route(
    "/v1/sequences/generate",
    asyncify(app_state.sequences_generate),
    methods=["POST"],
)
router.add_api_route(
    "/v1/sequences/status", asyncify(app_state.sequences_status), methods=["GET"]
)
router.add_api_route(
    "/v1/sequences/play", asyncify(app_state.sequences_play), methods=["POST"]
)
router.add_api_route(
    "/v1/sequences/stop", asyncify(app_state.sequences_stop), methods=["POST"]
)
