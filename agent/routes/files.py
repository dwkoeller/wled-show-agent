from __future__ import annotations

from fastapi import APIRouter

from services import files_service


router = APIRouter()

router.add_api_route(
    "/v1/files/list", files_service.files_list, methods=["GET"]
)
router.add_api_route(
    "/v1/files/download", files_service.files_download, methods=["GET"]
)
router.add_api_route(
    "/v1/files/upload", files_service.files_upload, methods=["PUT"]
)
router.add_api_route(
    "/v1/files/upload",
    files_service.files_upload_multipart,
    methods=["POST"],
)
router.add_api_route(
    "/v1/files/delete", files_service.files_delete, methods=["DELETE"]
)
router.add_api_route(
    "/v1/files/delete_dir", files_service.files_delete_dir, methods=["DELETE"]
)
