from __future__ import annotations

from fastapi import APIRouter

from services import packs_service
from utils.fastapi_utils import asyncify


router = APIRouter()

router.add_api_route(
    "/v1/packs/ingest", asyncify(packs_service.packs_ingest), methods=["PUT"]
)
