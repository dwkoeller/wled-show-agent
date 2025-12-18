from __future__ import annotations

from fastapi import APIRouter

from services import a2a_service


router = APIRouter()

router.add_api_route("/v1/a2a/card", a2a_service.a2a_card, methods=["GET"])
router.add_api_route("/v1/a2a/invoke", a2a_service.a2a_invoke, methods=["POST"])
