from __future__ import annotations

from fastapi import APIRouter

from services import app_state
from utils.fastapi_utils import asyncify


router = APIRouter()

router.add_api_route("/v1/wled/info", asyncify(app_state.wled_info), methods=["GET"])
router.add_api_route("/v1/wled/state", asyncify(app_state.wled_state), methods=["GET"])
router.add_api_route(
    "/v1/wled/segments", asyncify(app_state.wled_segments), methods=["GET"]
)
router.add_api_route(
    "/v1/wled/state", asyncify(app_state.wled_apply_state), methods=["POST"]
)
