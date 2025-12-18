from __future__ import annotations

from fastapi import APIRouter

from services import app_state
from utils.fastapi_utils import asyncify


router = APIRouter()

router.add_api_route(
    "/v1/auth/config", asyncify(app_state.auth_config), methods=["GET"]
)
router.add_api_route("/v1/auth/login", asyncify(app_state.auth_login), methods=["POST"])
router.add_api_route(
    "/v1/auth/logout", asyncify(app_state.auth_logout), methods=["POST"]
)
router.add_api_route("/v1/auth/me", asyncify(app_state.auth_me), methods=["GET"])
