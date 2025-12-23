from __future__ import annotations

from fastapi import APIRouter

from services import ledfx_service


router = APIRouter()

router.add_api_route("/v1/ledfx/status", ledfx_service.ledfx_status, methods=["GET"])
router.add_api_route(
    "/v1/ledfx/fleet", ledfx_service.ledfx_fleet_summary, methods=["GET"]
)
router.add_api_route(
    "/v1/ledfx/virtuals", ledfx_service.ledfx_virtuals, methods=["GET"]
)
router.add_api_route("/v1/ledfx/scenes", ledfx_service.ledfx_scenes, methods=["GET"])
router.add_api_route("/v1/ledfx/effects", ledfx_service.ledfx_effects, methods=["GET"])
router.add_api_route(
    "/v1/ledfx/scene/activate",
    ledfx_service.ledfx_scene_activate,
    methods=["POST"],
)
router.add_api_route(
    "/v1/ledfx/scene/deactivate",
    ledfx_service.ledfx_scene_deactivate,
    methods=["POST"],
)
router.add_api_route(
    "/v1/ledfx/virtual/effect",
    ledfx_service.ledfx_virtual_effect,
    methods=["POST"],
)
router.add_api_route(
    "/v1/ledfx/virtual/brightness",
    ledfx_service.ledfx_virtual_brightness,
    methods=["POST"],
)
router.add_api_route("/v1/ledfx/request", ledfx_service.ledfx_proxy, methods=["POST"])
