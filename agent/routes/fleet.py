from __future__ import annotations

from fastapi import APIRouter

from services import app_state
from services import fleet_service
from utils.fastapi_utils import asyncify


router = APIRouter()

router.add_api_route(
    "/v1/fleet/peers", asyncify(fleet_service.fleet_peers), methods=["GET"]
)
router.add_api_route(
    "/v1/fleet/invoke", asyncify(fleet_service.fleet_invoke), methods=["POST"]
)
router.add_api_route(
    "/v1/fleet/apply_random_look",
    asyncify(fleet_service.fleet_apply_random_look),
    methods=["POST"],
)
router.add_api_route(
    "/v1/fleet/stop_all", asyncify(fleet_service.fleet_stop_all), methods=["POST"]
)
router.add_api_route(
    "/v1/fleet/sequences/status",
    asyncify(app_state.fleet_sequences_status),
    methods=["GET"],
)
router.add_api_route(
    "/v1/fleet/sequences/start",
    asyncify(app_state.fleet_sequences_start),
    methods=["POST"],
)
router.add_api_route(
    "/v1/fleet/sequences/stop",
    asyncify(app_state.fleet_sequences_stop),
    methods=["POST"],
)
