from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any, Optional

import httpx
from fastapi import HTTPException, Request

from config import Settings
from rate_limiter import AsyncCooldown


@dataclass
class AppState:
    settings: Settings
    started_at: float

    # WLED runtime config (derived on startup).
    segment_ids: list[int] = field(default_factory=list)
    wled_cooldown: AsyncCooldown | None = None

    # WLED clients/services.
    wled: Any = None  # AsyncWLEDClient
    wled_mapper: Any = None  # WLEDMapper

    # Domain services.
    looks: Any = None  # LookService
    importer: Any = None  # PresetImporter
    ddp: Any = None  # DDPStreamer
    sequences: Any = None  # SequenceService
    fleet_sequences: Any = None  # FleetSequenceService
    orchestrator: Any = None  # OrchestrationService
    fleet_orchestrator: Any = None  # FleetOrchestrationService
    director: Any = None  # Optional OpenAI director

    # Runtime state snapshot settings.
    runtime_state_path: str = ""
    kv_runtime_state_key: str = "runtime_state"

    # Optional async DB service (SQLModel / AsyncSession).
    db: Any = None

    # Runtime services (populated by startup).
    jobs: Any = None  # AsyncJobManager
    scheduler: Any = None  # SchedulerService-like
    peers: dict[str, Any] | None = None
    blocking: Any = None  # BlockingService
    ddp_blocking: Any = None  # BlockingService
    cpu_pool: Any = None  # ProcessService

    # Shared async HTTP client for peer fanout.
    peer_http: Optional[httpx.AsyncClient] = None

    # Optional MQTT bridge.
    mqtt: Any = None

    # Server-sent events (UI refresh).
    events: Any = None

    # Main event loop (used for sync<->async bridges).
    loop: Optional[asyncio.AbstractEventLoop] = None

    # Background maintenance tasks (e.g., DB retention, reconciler).
    maintenance_tasks: list[Any] = field(default_factory=list)

    # Back-compat: legacy single maintenance task.
    maintenance_task: Any = None

    # Reconcile control (for status/cancel).
    reconcile_task: Any = None
    reconcile_cancel_event: asyncio.Event | None = None
    reconcile_run_id: int | None = None

    # Metrics retention bookkeeping.
    metrics_history_retention_last: dict[str, Any] | None = None
    audit_log_retention_last: dict[str, Any] | None = None
    event_log_retention_last: dict[str, Any] | None = None
    job_retention_last: dict[str, Any] | None = None
    scheduler_events_retention_last: dict[str, Any] | None = None
    pack_ingests_retention_last: dict[str, Any] | None = None
    sequence_meta_retention_last: dict[str, Any] | None = None
    audio_analyses_retention_last: dict[str, Any] | None = None
    show_configs_retention_last: dict[str, Any] | None = None
    fseq_exports_retention_last: dict[str, Any] | None = None
    fpp_scripts_retention_last: dict[str, Any] | None = None
    orchestration_runs_retention_last: dict[str, Any] | None = None
    agent_history_retention_last: dict[str, Any] | None = None

    def uptime_s(self) -> float:
        return max(0.0, time.time() - float(self.started_at))


def get_state(request: Request) -> AppState:
    st = getattr(request.app.state, "wsa", None)
    if st is None:
        raise HTTPException(status_code=503, detail="Service not initialized")
    return st
