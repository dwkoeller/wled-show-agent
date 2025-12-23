from __future__ import annotations

from fastapi import APIRouter

from services import auth_service


router = APIRouter()

router.add_api_route(
    "/v1/auth/config", auth_service.auth_config, methods=["GET"]
)
router.add_api_route(
    "/v1/auth/login", auth_service.auth_login, methods=["POST"]
)
router.add_api_route(
    "/v1/auth/logout", auth_service.auth_logout, methods=["POST"]
)
router.add_api_route("/v1/auth/me", auth_service.auth_me, methods=["GET"])
router.add_api_route("/v1/auth/users", auth_service.auth_users, methods=["GET"])
router.add_api_route(
    "/v1/auth/users", auth_service.auth_user_create, methods=["POST"]
)
router.add_api_route(
    "/v1/auth/users/{username}",
    auth_service.auth_user_update,
    methods=["PUT"],
)
router.add_api_route(
    "/v1/auth/users/{username}",
    auth_service.auth_user_delete,
    methods=["DELETE"],
)
router.add_api_route(
    "/v1/auth/sessions", auth_service.auth_sessions, methods=["GET"]
)
router.add_api_route(
    "/v1/auth/sessions/revoke",
    auth_service.auth_sessions_revoke,
    methods=["POST"],
)
router.add_api_route(
    "/v1/auth/login_attempts", auth_service.auth_login_attempts, methods=["GET"]
)
router.add_api_route(
    "/v1/auth/login_attempts/clear",
    auth_service.auth_login_attempts_clear,
    methods=["POST"],
)
router.add_api_route(
    "/v1/auth/api_keys", auth_service.auth_api_keys, methods=["GET"]
)
router.add_api_route(
    "/v1/auth/api_keys", auth_service.auth_api_key_create, methods=["POST"]
)
router.add_api_route(
    "/v1/auth/api_keys/revoke",
    auth_service.auth_api_key_revoke,
    methods=["POST"],
)
router.add_api_route(
    "/v1/auth/password/change",
    auth_service.auth_password_change,
    methods=["POST"],
)
router.add_api_route(
    "/v1/auth/password/reset_request",
    auth_service.auth_password_reset_request,
    methods=["POST"],
)
router.add_api_route(
    "/v1/auth/password/reset",
    auth_service.auth_password_reset,
    methods=["POST"],
)
