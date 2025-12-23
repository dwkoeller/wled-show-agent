from __future__ import annotations

from fastapi import APIRouter

from services import events_service


router = APIRouter()

router.add_api_route(
    "/v1/events",
    events_service.events_stream,
    methods=["GET"],
)

router.add_api_route(
    "/v1/events/history",
    events_service.events_history,
    methods=["GET"],
)

router.add_api_route(
    "/v1/events/stats",
    events_service.events_stats,
    methods=["GET"],
)
router.add_api_route(
    "/v1/events/retention",
    events_service.events_retention_status,
    methods=["GET"],
)
router.add_api_route(
    "/v1/events/retention",
    events_service.events_retention_cleanup,
    methods=["POST"],
)

router.add_api_route(
    "/v1/events/history/export",
    events_service.events_history_export,
    methods=["GET"],
)
