from __future__ import annotations

from fastapi import APIRouter

from services import scheduler_service
from utils.fastapi_utils import asyncify


router = APIRouter()

router.add_api_route(
    "/v1/scheduler/status",
    asyncify(scheduler_service.scheduler_status),
    methods=["GET"],
)
router.add_api_route(
    "/v1/scheduler/config",
    asyncify(scheduler_service.scheduler_get_config),
    methods=["GET"],
)
router.add_api_route(
    "/v1/scheduler/config",
    asyncify(scheduler_service.scheduler_set_config),
    methods=["POST"],
)
router.add_api_route(
    "/v1/scheduler/start", asyncify(scheduler_service.scheduler_start), methods=["POST"]
)
router.add_api_route(
    "/v1/scheduler/stop", asyncify(scheduler_service.scheduler_stop), methods=["POST"]
)
router.add_api_route(
    "/v1/scheduler/run_once",
    asyncify(scheduler_service.scheduler_run_once),
    methods=["POST"],
)
