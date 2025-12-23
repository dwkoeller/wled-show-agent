from __future__ import annotations

from fastapi import APIRouter

from services import jobs_service


router = APIRouter()

router.add_api_route("/v1/jobs", jobs_service.jobs_list, methods=["GET"])
router.add_api_route(
    "/v1/jobs/retention",
    jobs_service.jobs_retention_status,
    methods=["GET"],
)
router.add_api_route(
    "/v1/jobs/retention",
    jobs_service.jobs_retention_cleanup,
    methods=["POST"],
)
router.add_api_route("/v1/jobs/{job_id}", jobs_service.jobs_get, methods=["GET"])
router.add_api_route(
    "/v1/jobs/{job_id}/cancel", jobs_service.jobs_cancel, methods=["POST"]
)
router.add_api_route(
    "/v1/jobs/looks/generate",
    jobs_service.jobs_looks_generate,
    methods=["POST"],
)
router.add_api_route(
    "/v1/jobs/audio/analyze",
    jobs_service.jobs_audio_analyze,
    methods=["POST"],
)
router.add_api_route(
    "/v1/jobs/xlights/import_project",
    jobs_service.jobs_xlights_import_project,
    methods=["POST"],
)
router.add_api_route(
    "/v1/jobs/xlights/import_networks",
    jobs_service.jobs_xlights_import_networks,
    methods=["POST"],
)
router.add_api_route(
    "/v1/jobs/xlights/import_sequence",
    jobs_service.jobs_xlights_import_sequence,
    methods=["POST"],
)
router.add_api_route(
    "/v1/jobs/sequences/generate",
    jobs_service.jobs_sequences_generate,
    methods=["POST"],
)
router.add_api_route(
    "/v1/jobs/fseq/export", jobs_service.jobs_fseq_export, methods=["POST"]
)
