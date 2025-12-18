from __future__ import annotations

from fastapi import APIRouter

from services import app_state
from utils.fastapi_utils import asyncify


router = APIRouter()

router.add_api_route("/v1/a2a/card", asyncify(app_state.a2a_card), methods=["GET"])
router.add_api_route("/v1/a2a/invoke", asyncify(app_state.a2a_invoke), methods=["POST"])
