from __future__ import annotations

from fastapi import APIRouter

from services import backup_service


router = APIRouter()

router.add_api_route(
    "/v1/backup/export",
    backup_service.backup_export,
    methods=["GET"],
)
router.add_api_route(
    "/v1/backup/import",
    backup_service.backup_import,
    methods=["POST"],
)
