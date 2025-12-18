from __future__ import annotations

from fastapi import APIRouter

from services import ddp_service


router = APIRouter()

router.add_api_route("/v1/ddp/patterns", ddp_service.ddp_patterns, methods=["GET"])
router.add_api_route("/v1/ddp/status", ddp_service.ddp_status, methods=["GET"])
router.add_api_route("/v1/ddp/start", ddp_service.ddp_start, methods=["POST"])
router.add_api_route("/v1/ddp/stop", ddp_service.ddp_stop, methods=["POST"])
