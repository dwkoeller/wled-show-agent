from __future__ import annotations

from fastapi import APIRouter

from services import misc_service


router = APIRouter()

router.add_api_route("/v1/go_crazy", misc_service.go_crazy, methods=["POST"])
