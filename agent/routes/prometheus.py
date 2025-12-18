from __future__ import annotations

from fastapi import APIRouter

from services import prometheus_metrics
from utils.fastapi_utils import asyncify


router = APIRouter()

router.add_api_route(
    "/metrics",
    asyncify(prometheus_metrics.metrics_endpoint_with_state),
    methods=["GET"],
    include_in_schema=False,
)
