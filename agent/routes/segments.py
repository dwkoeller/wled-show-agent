from __future__ import annotations

from fastapi import APIRouter

from services import app_state
from utils.fastapi_utils import asyncify


router = APIRouter()

router.add_api_route(
    "/v1/segments/layout", asyncify(app_state.segments_layout), methods=["GET"]
)
router.add_api_route(
    "/v1/segments/orientation",
    asyncify(app_state.segments_orientation),
    methods=["GET"],
)
