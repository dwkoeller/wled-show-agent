from __future__ import annotations

from fastapi import APIRouter

from services import audit_service


router = APIRouter()

router.add_api_route("/api/audit/logs", audit_service.audit_logs, methods=["GET"])
router.add_api_route(
    "/api/audit/logs/export",
    audit_service.audit_logs_export,
    methods=["GET"],
)
router.add_api_route(
    "/api/audit/retention",
    audit_service.audit_retention_status,
    methods=["GET"],
)
router.add_api_route(
    "/api/audit/retention",
    audit_service.audit_retention_cleanup,
    methods=["POST"],
)
