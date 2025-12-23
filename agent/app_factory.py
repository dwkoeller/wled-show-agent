from __future__ import annotations

import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from config.constants import APP_TITLE, APP_VERSION
from routes import (
    a2a,
    backup,
    audit,
    auth,
    audio,
    command,
    ddp,
    events,
    fleet,
    files,
    fpp,
    fseq,
    jobs,
    ledfx,
    looks,
    meta,
    metrics,
    misc,
    mqtt,
    orchestration,
    packs,
    presets,
    prometheus,
    root,
    runtime_state,
    scheduler,
    segments,
    sequences,
    show,
    voice,
    wled,
)
from services.auth_service import auth_middleware
from services.blocking_service import BlockingQueueFull
from services import app_state
from services.prometheus_metrics import PrometheusMetricsMiddleware
from services.rate_limit_service import rate_limit_middleware
from utils.request_id import RequestIdMiddleware


def _as_bool(val: str | None, default: bool) -> bool:
    if val is None:
        return default
    s = str(val).strip().lower()
    if s in ("1", "true", "yes", "y", "on"):
        return True
    if s in ("0", "false", "no", "n", "off"):
        return False
    return default


def _as_csv(val: str | None) -> list[str]:
    if val is None:
        return []
    out: list[str] = []
    for part in str(val).split(","):
        p = part.strip()
        if not p:
            continue
        out.append(p)
    return out


@asynccontextmanager
async def lifespan(app: FastAPI):  # type: ignore[no-untyped-def]
    # Start/stop background services. Keep best-effort so shutdown never hangs.
    try:
        await app_state.startup(app)
        yield
    finally:
        try:
            await app_state.shutdown(app)
        except Exception:
            pass


def create_app() -> FastAPI:
    app = FastAPI(title=APP_TITLE, version=APP_VERSION, lifespan=lifespan)

    @app.exception_handler(BlockingQueueFull)
    async def _blocking_queue_full_handler(request, exc):  # type: ignore[no-untyped-def]
        _ = request
        _ = exc
        return JSONResponse(
            status_code=503,
            content={"detail": "Server busy. Try again shortly."},
        )

    app.add_middleware(RequestIdMiddleware)
    app.add_middleware(PrometheusMetricsMiddleware)

    cors_origins = _as_csv(os.environ.get("CORS_ALLOW_ORIGINS"))
    cors_origin_regex = (
        os.environ.get("CORS_ALLOW_ORIGIN_REGEX") or ""
    ).strip() or None
    cors_allow_credentials = _as_bool(
        os.environ.get("CORS_ALLOW_CREDENTIALS"), default=True
    )
    if cors_origins or cors_origin_regex:
        # Starlette forbids allow_credentials with wildcard origins.
        if any(o == "*" for o in cors_origins) and cors_allow_credentials:
            cors_allow_credentials = False
            cors_origins = ["*"]
        app.add_middleware(
            CORSMiddleware,
            allow_origins=cors_origins,
            allow_origin_regex=cors_origin_regex,
            allow_credentials=cors_allow_credentials,
            allow_methods=["*"],
            allow_headers=["*"],
        )

    # Auth middleware is optional (AUTH_ENABLED).
    app.middleware("http")(auth_middleware)
    app.middleware("http")(rate_limit_middleware)

    app.include_router(root.router)
    app.include_router(backup.router)
    app.include_router(audit.router)
    app.include_router(auth.router)
    app.include_router(wled.router)
    app.include_router(segments.router)
    app.include_router(ddp.router)
    app.include_router(looks.router)
    app.include_router(presets.router)
    app.include_router(packs.router)
    app.include_router(sequences.router)
    app.include_router(fseq.router)
    app.include_router(audio.router)
    app.include_router(misc.router)
    app.include_router(command.router)
    app.include_router(fpp.router)
    app.include_router(ledfx.router)
    app.include_router(show.router)
    app.include_router(a2a.router)
    app.include_router(fleet.router)
    app.include_router(files.router)
    app.include_router(runtime_state.router)
    app.include_router(scheduler.router)
    app.include_router(meta.router)
    app.include_router(metrics.router)
    app.include_router(prometheus.router)
    app.include_router(jobs.router)
    app.include_router(mqtt.router)
    app.include_router(voice.router)
    app.include_router(orchestration.router)
    app.include_router(events.router)

    return app
