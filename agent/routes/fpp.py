from __future__ import annotations

from fastapi import APIRouter

from services import app_state
from utils.fastapi_utils import asyncify


router = APIRouter()

router.add_api_route("/v1/fpp/status", asyncify(app_state.fpp_status), methods=["GET"])
router.add_api_route(
    "/v1/fpp/discover", asyncify(app_state.fpp_discover), methods=["GET"]
)
router.add_api_route(
    "/v1/fpp/playlists", asyncify(app_state.fpp_playlists), methods=["GET"]
)
router.add_api_route(
    "/v1/fpp/playlist/start", asyncify(app_state.fpp_start_playlist), methods=["POST"]
)
router.add_api_route(
    "/v1/fpp/playlist/stop", asyncify(app_state.fpp_stop_playlist), methods=["POST"]
)
router.add_api_route(
    "/v1/fpp/event/trigger", asyncify(app_state.fpp_trigger_event), methods=["POST"]
)
router.add_api_route("/v1/fpp/request", asyncify(app_state.fpp_proxy), methods=["POST"])
router.add_api_route(
    "/v1/fpp/upload_file", asyncify(app_state.fpp_upload_file), methods=["POST"]
)
router.add_api_route(
    "/v1/fpp/export/fleet_sequence_start_script",
    asyncify(app_state.export_fleet_sequence_start_script),
    methods=["POST"],
)
router.add_api_route(
    "/v1/fpp/export/fleet_stop_all_script",
    asyncify(app_state.export_fleet_stop_all_script),
    methods=["POST"],
)
