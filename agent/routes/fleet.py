from __future__ import annotations

from fastapi import APIRouter

from services import fleet_service
from services import fleet_sequences_service


router = APIRouter()

router.add_api_route("/v1/fleet/peers", fleet_service.fleet_peers, methods=["GET"])
router.add_api_route("/v1/fleet/invoke", fleet_service.fleet_invoke, methods=["POST"])
router.add_api_route(
    "/v1/fleet/apply_random_look",
    fleet_service.fleet_apply_random_look,
    methods=["POST"],
)
router.add_api_route(
    "/v1/fleet/stop_all", fleet_service.fleet_stop_all, methods=["POST"]
)
router.add_api_route(
    "/v1/fleet/sequences/status",
    fleet_sequences_service.fleet_sequences_status,
    methods=["GET"],
)
router.add_api_route(
    "/v1/fleet/sequences/start",
    fleet_sequences_service.fleet_sequences_start,
    methods=["POST"],
)
router.add_api_route(
    "/v1/fleet/sequences/stop",
    fleet_sequences_service.fleet_sequences_stop,
    methods=["POST"],
)
