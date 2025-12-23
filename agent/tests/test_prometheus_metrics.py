from __future__ import annotations

from types import SimpleNamespace

import pytest
from starlette.applications import Starlette
from starlette.requests import Request

from services.prometheus_metrics import metrics_endpoint_with_state


class _Events:
    async def stats(self) -> dict[str, int]:
        return {"subscribers": 3, "history": 7, "max_history": 100}


@pytest.mark.anyio
async def test_metrics_sse_stats() -> None:
    app = Starlette()
    app.state.wsa = SimpleNamespace(db=None, events=_Events())
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/metrics",
        "headers": [],
        "app": app,
    }
    request = Request(scope)
    resp = await metrics_endpoint_with_state(request)
    body = resp.body.decode("utf-8")
    assert "wsa_sse_clients" in body
    assert "wsa_sse_history_size" in body
    assert "wsa_sse_history_max" in body
    assert "wsa_sse_clients 3" in body
    assert "wsa_sse_spool_bytes" in body
    assert "wsa_sse_spool_events" in body
    assert "wsa_sse_spool_dropped_total" in body
