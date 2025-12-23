from __future__ import annotations

from fastapi import APIRouter

from services import voice_service


router = APIRouter()

router.add_api_route(
    "/v1/voice/transcribe", voice_service.voice_transcribe, methods=["POST"]
)
router.add_api_route(
    "/v1/voice/command", voice_service.voice_command, methods=["POST"]
)
