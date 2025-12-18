from __future__ import annotations

from fastapi import APIRouter

from services import app_state
from utils.fastapi_utils import asyncify


router = APIRouter()

router.add_api_route("/v1/jobs", asyncify(app_state.jobs_list), methods=["GET"])
router.add_api_route("/v1/jobs/{job_id}", asyncify(app_state.jobs_get), methods=["GET"])
router.add_api_route(
    "/v1/jobs/{job_id}/cancel", asyncify(app_state.jobs_cancel), methods=["POST"]
)
router.add_api_route(
    "/v1/jobs/stream", asyncify(app_state.jobs_stream), methods=["GET"]
)
router.add_api_route(
    "/v1/jobs/looks/generate",
    asyncify(app_state.jobs_looks_generate),
    methods=["POST"],
)
router.add_api_route(
    "/v1/jobs/audio/analyze",
    asyncify(app_state.jobs_audio_analyze),
    methods=["POST"],
)
router.add_api_route(
    "/v1/jobs/xlights/import_project",
    asyncify(app_state.jobs_xlights_import_project),
    methods=["POST"],
)
router.add_api_route(
    "/v1/jobs/xlights/import_networks",
    asyncify(app_state.jobs_xlights_import_networks),
    methods=["POST"],
)
router.add_api_route(
    "/v1/jobs/xlights/import_sequence",
    asyncify(app_state.jobs_xlights_import_sequence),
    methods=["POST"],
)
router.add_api_route(
    "/v1/jobs/sequences/generate",
    asyncify(app_state.jobs_sequences_generate),
    methods=["POST"],
)
router.add_api_route(
    "/v1/jobs/fseq/export", asyncify(app_state.jobs_fseq_export), methods=["POST"]
)
