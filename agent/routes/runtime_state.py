from __future__ import annotations

from fastapi import APIRouter

from services import runtime_state_service


router = APIRouter()

router.add_api_route(
    "/v1/runtime_state", runtime_state_service.runtime_state, methods=["GET"]
)
