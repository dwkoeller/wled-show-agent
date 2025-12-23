from __future__ import annotations

from fastapi import APIRouter

from services import orchestration_service


router = APIRouter()

router.add_api_route(
    "/v1/orchestration/status",
    orchestration_service.orchestration_status,
    methods=["GET"],
)
router.add_api_route(
    "/v1/orchestration/runs",
    orchestration_service.orchestration_runs,
    methods=["GET"],
)
router.add_api_route(
    "/v1/orchestration/runs/export",
    orchestration_service.orchestration_runs_export,
    methods=["GET"],
)
router.add_api_route(
    "/v1/orchestration/retention",
    orchestration_service.orchestration_retention_status,
    methods=["GET"],
)
router.add_api_route(
    "/v1/orchestration/retention",
    orchestration_service.orchestration_retention_cleanup,
    methods=["POST"],
)
router.add_api_route(
    "/v1/orchestration/runs/{run_id}",
    orchestration_service.orchestration_run_detail,
    methods=["GET"],
)
router.add_api_route(
    "/v1/orchestration/runs/{run_id}/steps/export",
    orchestration_service.orchestration_run_steps_export,
    methods=["GET"],
)
router.add_api_route(
    "/v1/orchestration/runs/{run_id}/peers/export",
    orchestration_service.orchestration_run_peers_export,
    methods=["GET"],
)
router.add_api_route(
    "/v1/orchestration/presets",
    orchestration_service.orchestration_presets,
    methods=["GET"],
)
router.add_api_route(
    "/v1/orchestration/presets",
    orchestration_service.orchestration_presets_upsert,
    methods=["POST"],
)
router.add_api_route(
    "/v1/orchestration/presets/export",
    orchestration_service.orchestration_presets_export,
    methods=["GET"],
)
router.add_api_route(
    "/v1/orchestration/presets/import",
    orchestration_service.orchestration_presets_import,
    methods=["POST"],
)
router.add_api_route(
    "/v1/orchestration/presets/{preset_id}",
    orchestration_service.orchestration_presets_delete,
    methods=["DELETE"],
)
router.add_api_route(
    "/v1/orchestration/start",
    orchestration_service.orchestration_start,
    methods=["POST"],
)
router.add_api_route(
    "/v1/orchestration/stop",
    orchestration_service.orchestration_stop,
    methods=["POST"],
)
router.add_api_route(
    "/v1/orchestration/blackout",
    orchestration_service.orchestration_blackout,
    methods=["POST"],
)
router.add_api_route(
    "/v1/orchestration/crossfade",
    orchestration_service.orchestration_crossfade,
    methods=["POST"],
)
