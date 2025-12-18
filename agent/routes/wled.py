from __future__ import annotations

from fastapi import APIRouter

from services import wled_service


router = APIRouter()

router.add_api_route("/v1/wled/info", wled_service.wled_info, methods=["GET"])
router.add_api_route("/v1/wled/state", wled_service.wled_state, methods=["GET"])
router.add_api_route("/v1/wled/segments", wled_service.wled_segments, methods=["GET"])
router.add_api_route("/v1/wled/state", wled_service.wled_apply_state, methods=["POST"])
