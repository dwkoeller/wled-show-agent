from __future__ import annotations

from fastapi import APIRouter

from services import scheduler_service


router = APIRouter()

router.add_api_route(
    "/v1/scheduler/status",
    scheduler_service.scheduler_status,
    methods=["GET"],
)
router.add_api_route(
    "/v1/scheduler/config",
    scheduler_service.scheduler_get_config,
    methods=["GET"],
)
router.add_api_route(
    "/v1/scheduler/config",
    scheduler_service.scheduler_set_config,
    methods=["POST"],
)
router.add_api_route(
    "/v1/scheduler/start", scheduler_service.scheduler_start, methods=["POST"]
)
router.add_api_route(
    "/v1/scheduler/stop", scheduler_service.scheduler_stop, methods=["POST"]
)
router.add_api_route(
    "/v1/scheduler/run_once",
    scheduler_service.scheduler_run_once,
    methods=["POST"],
)
