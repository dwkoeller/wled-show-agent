from __future__ import annotations

from fastapi import APIRouter

from services import audit_service


router = APIRouter()

router.add_api_route("/v1/audit/logs", audit_service.audit_logs, methods=["GET"])
router.add_api_route(
    "/v1/audit/logs/export",
    audit_service.audit_logs_export,
    methods=["GET"],
)
router.add_api_route(
    "/v1/audit/retention",
    audit_service.audit_retention_status,
    methods=["GET"],
)
router.add_api_route(
    "/v1/audit/retention",
    audit_service.audit_retention_cleanup,
    methods=["POST"],
)
