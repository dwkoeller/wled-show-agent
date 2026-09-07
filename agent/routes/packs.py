from __future__ import annotations

from fastapi import APIRouter

from services import packs_service


router = APIRouter()

router.add_api_route(
    "/api/packs/ingest", packs_service.packs_ingest, methods=["PUT"]
)
