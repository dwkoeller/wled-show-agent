from __future__ import annotations

from fastapi import APIRouter

from services import app_state
from utils.fastapi_utils import asyncify


router = APIRouter()

router.add_api_route(
    "/v1/audio/analyze", asyncify(app_state.audio_analyze), methods=["POST"]
)
