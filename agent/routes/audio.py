from __future__ import annotations

from fastapi import APIRouter

from services import audio_service


router = APIRouter()

router.add_api_route("/v1/audio/analyze", audio_service.audio_analyze, methods=["POST"])
