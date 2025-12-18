from __future__ import annotations

from fastapi import APIRouter

from services import fseq_service


router = APIRouter()

router.add_api_route("/v1/fseq/export", fseq_service.fseq_export, methods=["POST"])
