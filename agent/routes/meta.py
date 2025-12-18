from __future__ import annotations

from fastapi import APIRouter

from services import metadata_service
from utils.fastapi_utils import asyncify


router = APIRouter()

router.add_api_route(
    "/v1/meta/packs", asyncify(metadata_service.meta_packs), methods=["GET"]
)
router.add_api_route(
    "/v1/meta/sequences", asyncify(metadata_service.meta_sequences), methods=["GET"]
)
router.add_api_route(
    "/v1/meta/audio_analyses",
    asyncify(metadata_service.meta_audio_analyses),
    methods=["GET"],
)
router.add_api_route(
    "/v1/meta/last_applied",
    asyncify(metadata_service.meta_last_applied),
    methods=["GET"],
)
