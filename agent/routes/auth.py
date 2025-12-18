from __future__ import annotations

from fastapi import APIRouter

from services import auth_service
from utils.fastapi_utils import asyncify


router = APIRouter()

router.add_api_route(
    "/v1/auth/config", asyncify(auth_service.auth_config), methods=["GET"]
)
router.add_api_route(
    "/v1/auth/login", asyncify(auth_service.auth_login), methods=["POST"]
)
router.add_api_route(
    "/v1/auth/logout", asyncify(auth_service.auth_logout), methods=["POST"]
)
router.add_api_route("/v1/auth/me", asyncify(auth_service.auth_me), methods=["GET"])
