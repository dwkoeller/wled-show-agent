from __future__ import annotations

from fastapi import APIRouter

from services import app_state
from utils.fastapi_utils import asyncify


router = APIRouter()

router.add_api_route("/v1/files/list", asyncify(app_state.files_list), methods=["GET"])
router.add_api_route(
    "/v1/files/download", asyncify(app_state.files_download), methods=["GET"]
)
router.add_api_route(
    "/v1/files/upload", asyncify(app_state.files_upload), methods=["PUT"]
)
router.add_api_route(
    "/v1/files/delete", asyncify(app_state.files_delete), methods=["DELETE"]
)
