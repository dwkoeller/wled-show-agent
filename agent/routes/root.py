from __future__ import annotations

from fastapi import APIRouter

from services import app_state
from utils.fastapi_utils import asyncify


router = APIRouter()

router.add_api_route(
    "/", asyncify(app_state.root), methods=["GET"], include_in_schema=False
)
router.add_api_route("/v1/health", asyncify(app_state.health), methods=["GET"])
