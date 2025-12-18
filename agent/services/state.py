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
    wled_sync: Any = None  # AsyncWLEDClientSyncAdapter (thread use)
    wled_mapper: Any = None  # WLEDMapper

    # Domain services.
    looks: Any = None  # LookService
    importer: Any = None  # PresetImporter
    ddp: Any = None  # DDPStreamer
    sequences: Any = None  # SequenceService
    fleet_sequences: Any = None  # FleetSequenceService
    director: Any = None  # Optional OpenAI director

    # Runtime state snapshot settings.
    runtime_state_path: str = ""
    kv_runtime_state_key: str = "runtime_state"

    # Optional async DB service (SQLModel / AsyncSession).
    db: Any = None

    # Runtime services (populated by startup).
    jobs: Any = None  # JobManager-like
    scheduler: Any = None  # SchedulerService-like
    peers: dict[str, Any] | None = None

    # Shared async HTTP client for peer fanout.
    peer_http: Optional[httpx.AsyncClient] = None

    # Main event loop (used for sync<->async bridges).
    loop: Optional[asyncio.AbstractEventLoop] = None

    # Background maintenance tasks (e.g., DB retention, reconciler).
    maintenance_tasks: list[Any] = field(default_factory=list)

    # Back-compat: legacy single maintenance task.
    maintenance_task: Any = None

    def uptime_s(self) -> float:
        return max(0.0, time.time() - float(self.started_at))


def get_state(request: Request) -> AppState:
    st = getattr(request.app.state, "wsa", None)
    if st is None:
        raise HTTPException(status_code=503, detail="Service not initialized")
    return st
