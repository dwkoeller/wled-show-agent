from __future__ import annotations

from fastapi import APIRouter

from services import app_state
from utils.fastapi_utils import asyncify


router = APIRouter()

router.add_api_route(
    "/v1/scheduler/status", asyncify(app_state.scheduler_status), methods=["GET"]
)
router.add_api_route(
    "/v1/scheduler/config",
    asyncify(app_state.scheduler_get_config),
    methods=["GET"],
)
router.add_api_route(
    "/v1/scheduler/config",
    asyncify(app_state.scheduler_set_config),
    methods=["POST"],
)
router.add_api_route(
    "/v1/scheduler/start", asyncify(app_state.scheduler_start), methods=["POST"]
)
router.add_api_route(
    "/v1/scheduler/stop", asyncify(app_state.scheduler_stop), methods=["POST"]
)
router.add_api_route(
    "/v1/scheduler/run_once",
    asyncify(app_state.scheduler_run_once),
    methods=["POST"],
)
