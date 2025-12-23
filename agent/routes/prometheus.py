from __future__ import annotations

from fastapi import APIRouter

from services import prometheus_metrics


router = APIRouter()

router.add_api_route(
    "/metrics",
    prometheus_metrics.metrics_endpoint_with_state,
    methods=["GET"],
    include_in_schema=False,
)
