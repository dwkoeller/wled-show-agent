from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any, Optional

import httpx
from fastapi import HTTPException, Request

from config import Settings


@dataclass
class AppState:
    settings: Settings
    started_at: float

    # Optional async DB service (SQLModel / AsyncSession).
    db: Any = None

    # Runtime services (populated by startup).
    jobs: Any = None
    scheduler: Any = None
    peers: dict[str, Any] | None = None

    # Shared async HTTP client for peer fanout.
    peer_http: Optional[httpx.AsyncClient] = None

    # Main event loop (used for sync<->async bridges).
    loop: Optional[asyncio.AbstractEventLoop] = None

    # Background maintenance tasks (e.g., DB retention).
    maintenance_task: Any = None

    def uptime_s(self) -> float:
        return max(0.0, time.time() - float(self.started_at))


def get_state(request: Request) -> AppState:
    st = getattr(request.app.state, "wsa", None)
    if st is None:
        raise HTTPException(status_code=503, detail="Service not initialized")
    return st
