from __future__ import annotations

from fastapi import APIRouter

from services import fleet_service
from services import fleet_sequences_service
from services import fleet_orchestration_service


router = APIRouter()

router.add_api_route("/v1/fleet/peers", fleet_service.fleet_peers, methods=["GET"])
router.add_api_route("/v1/fleet/status", fleet_service.fleet_status, methods=["GET"])
router.add_api_route("/v1/fleet/health", fleet_service.fleet_health, methods=["GET"])
router.add_api_route(
    "/v1/fleet/overrides", fleet_service.fleet_overrides_list, methods=["GET"]
)
router.add_api_route(
    "/v1/fleet/overrides/export",
    fleet_service.fleet_overrides_export,
    methods=["GET"],
)
router.add_api_route(
    "/v1/fleet/overrides/template",
    fleet_service.fleet_overrides_template,
    methods=["GET"],
)
router.add_api_route(
    "/v1/fleet/overrides/import",
    fleet_service.fleet_overrides_import,
    methods=["POST"],
)
router.add_api_route(
    "/v1/fleet/overrides/{agent_id}",
    fleet_service.fleet_override_update,
    methods=["PUT"],
)
router.add_api_route(
    "/v1/fleet/overrides/{agent_id}",
    fleet_service.fleet_override_delete,
    methods=["DELETE"],
)
router.add_api_route(
    "/v1/fleet/history", fleet_service.fleet_history, methods=["GET"]
)
router.add_api_route(
    "/v1/fleet/history/export",
    fleet_service.fleet_history_export,
    methods=["GET"],
)
router.add_api_route(
    "/v1/fleet/history/retention",
    fleet_service.fleet_history_retention_status,
    methods=["GET"],
)
router.add_api_route(
    "/v1/fleet/history/retention",
    fleet_service.fleet_history_retention_cleanup,
    methods=["POST"],
)
router.add_api_route("/v1/fleet/resolve", fleet_service.fleet_resolve, methods=["POST"])
router.add_api_route("/v1/fleet/invoke", fleet_service.fleet_invoke, methods=["POST"])
router.add_api_route(
    "/v1/fleet/apply_random_look",
    fleet_service.fleet_apply_random_look,
    methods=["POST"],
)
router.add_api_route(
    "/v1/fleet/crossfade", fleet_service.fleet_crossfade, methods=["POST"]
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
    "/v1/fleet/sequences/start_staggered",
    fleet_sequences_service.fleet_sequences_start_staggered,
    methods=["POST"],
)
router.add_api_route(
    "/v1/fleet/sequences/stop",
    fleet_sequences_service.fleet_sequences_stop,
    methods=["POST"],
)
router.add_api_route(
    "/v1/fleet/orchestration/status",
    fleet_orchestration_service.fleet_orchestration_status,
    methods=["GET"],
)
router.add_api_route(
    "/v1/fleet/orchestration/start",
    fleet_orchestration_service.fleet_orchestration_start,
    methods=["POST"],
)
router.add_api_route(
    "/v1/fleet/orchestration/stop",
    fleet_orchestration_service.fleet_orchestration_stop,
    methods=["POST"],
)
