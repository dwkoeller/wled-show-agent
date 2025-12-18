from __future__ import annotations

from fastapi import APIRouter

from services import files_service
from utils.fastapi_utils import asyncify


router = APIRouter()

router.add_api_route(
    "/v1/files/list", asyncify(files_service.files_list), methods=["GET"]
)
router.add_api_route(
    "/v1/files/download", asyncify(files_service.files_download), methods=["GET"]
)
router.add_api_route(
    "/v1/files/upload", asyncify(files_service.files_upload), methods=["PUT"]
)
router.add_api_route(
    "/v1/files/delete", asyncify(files_service.files_delete), methods=["DELETE"]
)
