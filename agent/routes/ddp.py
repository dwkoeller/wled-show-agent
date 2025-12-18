from __future__ import annotations

from fastapi import APIRouter

from services import app_state
from utils.fastapi_utils import asyncify


router = APIRouter()

router.add_api_route(
    "/v1/ddp/patterns", asyncify(app_state.ddp_patterns), methods=["GET"]
)
router.add_api_route("/v1/ddp/status", asyncify(app_state.ddp_status), methods=["GET"])
router.add_api_route("/v1/ddp/start", asyncify(app_state.ddp_start), methods=["POST"])
router.add_api_route("/v1/ddp/stop", asyncify(app_state.ddp_stop), methods=["POST"])
