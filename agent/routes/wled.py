from __future__ import annotations

from fastapi import APIRouter

from services import wled_service


router = APIRouter()

router.add_api_route("/api/wled/info", wled_service.wled_info, methods=["GET"])
router.add_api_route("/api/wled/state", wled_service.wled_state, methods=["GET"])
router.add_api_route("/api/wled/segments", wled_service.wled_segments, methods=["GET"])
router.add_api_route("/api/wled/presets", wled_service.wled_presets, methods=["GET"])
router.add_api_route("/api/wled/effects", wled_service.wled_effects, methods=["GET"])
router.add_api_route("/api/wled/palettes", wled_service.wled_palettes, methods=["GET"])
router.add_api_route("/api/wled/state", wled_service.wled_apply_state, methods=["POST"])
