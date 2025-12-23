from __future__ import annotations

from fastapi import APIRouter

from services import mqtt_service


router = APIRouter()

router.add_api_route("/v1/mqtt/status", mqtt_service.mqtt_status, methods=["GET"])
