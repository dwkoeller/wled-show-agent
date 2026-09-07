from __future__ import annotations

from fastapi import APIRouter

from services import audio_service


router = APIRouter()

router.add_api_route("/api/audio/analyze", audio_service.audio_analyze, methods=["POST"])
router.add_api_route("/api/audio/waveform", audio_service.audio_waveform, methods=["GET"])
router.add_api_route(
    "/api/audio/waveform/cache", audio_service.audio_waveform_cache, methods=["GET"]
)
router.add_api_route(
    "/api/audio/waveform/purge", audio_service.audio_waveform_purge, methods=["POST"]
)
