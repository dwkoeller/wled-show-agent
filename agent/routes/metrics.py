from __future__ import annotations

from fastapi import APIRouter

from services import metrics_service


router = APIRouter()

router.add_api_route("/v1/metrics", metrics_service.metrics, methods=["GET"])
