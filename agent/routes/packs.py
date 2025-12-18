from __future__ import annotations

from fastapi import APIRouter

from services import app_state
from utils.fastapi_utils import asyncify


router = APIRouter()

router.add_api_route(
    "/v1/packs/ingest", asyncify(app_state.packs_ingest), methods=["PUT"]
)
