from __future__ import annotations

from fastapi import APIRouter

from services import metadata_service


router = APIRouter()

router.add_api_route(
    "/api/meta/packs", metadata_service.meta_packs, methods=["GET"]
)
router.add_api_route(
    "/api/meta/sequences", metadata_service.meta_sequences, methods=["GET"]
)
router.add_api_route(
    "/api/meta/audio_analyses",
    metadata_service.meta_audio_analyses,
    methods=["GET"],
)
router.add_api_route(
    "/api/meta/show_configs",
    metadata_service.meta_show_configs,
    methods=["GET"],
)
router.add_api_route(
    "/api/meta/fseq_exports",
    metadata_service.meta_fseq_exports,
    methods=["GET"],
)
router.add_api_route(
    "/api/meta/fpp_scripts",
    metadata_service.meta_fpp_scripts,
    methods=["GET"],
)
router.add_api_route(
    "/api/meta/last_applied",
    metadata_service.meta_last_applied,
    methods=["GET"],
)
router.add_api_route(
    "/api/meta/reconcile",
    metadata_service.meta_reconcile,
    methods=["POST"],
)
router.add_api_route(
    "/api/meta/precompute",
    metadata_service.meta_precompute,
    methods=["POST"],
)
router.add_api_route(
    "/api/meta/reconcile/status",
    metadata_service.meta_reconcile_status,
    methods=["GET"],
)
router.add_api_route(
    "/api/meta/reconcile/history",
    metadata_service.meta_reconcile_history,
    methods=["GET"],
)
router.add_api_route(
    "/api/meta/reconcile/cancel",
    metadata_service.meta_reconcile_cancel,
    methods=["POST"],
)
router.add_api_route(
    "/api/meta/retention",
    metadata_service.meta_retention_status,
    methods=["GET"],
)
router.add_api_route(
    "/api/meta/retention",
    metadata_service.meta_retention_cleanup,
    methods=["POST"],
)
