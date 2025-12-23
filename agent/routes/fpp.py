from __future__ import annotations

from fastapi import APIRouter

from services import fpp_service


router = APIRouter()

router.add_api_route("/v1/fpp/status", fpp_service.fpp_status, methods=["GET"])
router.add_api_route("/v1/fpp/discover", fpp_service.fpp_discover, methods=["GET"])
router.add_api_route("/v1/fpp/playlists", fpp_service.fpp_playlists, methods=["GET"])
router.add_api_route(
    "/v1/fpp/playlists/sync", fpp_service.fpp_playlists_sync, methods=["POST"]
)
router.add_api_route(
    "/v1/fpp/playlists/import", fpp_service.fpp_playlists_import, methods=["POST"]
)
router.add_api_route(
    "/v1/fpp/playlist/start", fpp_service.fpp_start_playlist, methods=["POST"]
)
router.add_api_route(
    "/v1/fpp/playlist/stop", fpp_service.fpp_stop_playlist, methods=["POST"]
)
router.add_api_route(
    "/v1/fpp/event/trigger", fpp_service.fpp_trigger_event, methods=["POST"]
)
router.add_api_route("/v1/fpp/request", fpp_service.fpp_proxy, methods=["POST"])
router.add_api_route(
    "/v1/fpp/upload_file", fpp_service.fpp_upload_file, methods=["POST"]
)
router.add_api_route(
    "/v1/fpp/export/fleet_sequence_start_script",
    fpp_service.export_fleet_sequence_start_script,
    methods=["POST"],
)
router.add_api_route(
    "/v1/fpp/export/fleet_stop_all_script",
    fpp_service.export_fleet_stop_all_script,
    methods=["POST"],
)
router.add_api_route(
    "/v1/fpp/export/event_script",
    fpp_service.export_event_script,
    methods=["POST"],
)
