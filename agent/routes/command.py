from __future__ import annotations

from fastapi import APIRouter

from services import command_service


router = APIRouter()

router.add_api_route("/v1/command", command_service.command, methods=["POST"])
