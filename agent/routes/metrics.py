from __future__ import annotations

from fastapi import APIRouter

from services import metrics_service


router = APIRouter()

router.add_api_route("/v1/metrics", metrics_service.metrics, methods=["GET"])
router.add_api_route(
    "/v1/metrics/history", metrics_service.metrics_history, methods=["GET"]
)
router.add_api_route(
    "/v1/metrics/history/export",
    metrics_service.metrics_history_export,
    methods=["GET"],
)
router.add_api_route(
    "/v1/metrics/history/retention",
    metrics_service.metrics_history_retention_status,
    methods=["GET"],
)
router.add_api_route(
    "/v1/metrics/history/retention",
    metrics_service.metrics_history_retention_cleanup,
    methods=["POST"],
)
