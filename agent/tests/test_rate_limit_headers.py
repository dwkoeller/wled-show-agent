from __future__ import annotations

from types import SimpleNamespace

from fastapi import FastAPI
from starlette.testclient import TestClient

from services import rate_limit_service


def test_rate_limit_headers_present() -> None:
    rate_limit_service._LIMITER = None
    rate_limit_service._SETTINGS_CACHE = None
    rate_limit_service._LIMIT_RPM = None
    rate_limit_service._LIMIT_BURST = None

    settings = SimpleNamespace(
        rate_limit_enabled=True,
        rate_limit_requests_per_minute=1,
        rate_limit_burst=1,
        rate_limit_scope="ip",
        rate_limit_exempt_paths=(),
        rate_limit_bucket_ttl_s=60.0,
        rate_limit_cleanup_interval_s=5.0,
        rate_limit_trust_proxy_headers=False,
    )

    app = FastAPI()
    app.state.wsa = SimpleNamespace(settings=settings)
    app.middleware("http")(rate_limit_service.rate_limit_middleware)

    @app.get("/test")
    async def _test():  # type: ignore[no-untyped-def]
        return {"ok": True}

    client = TestClient(app)
    first = client.get("/test")
    assert first.status_code == 200
    assert "X-RateLimit-Limit" in first.headers
    assert "X-RateLimit-Remaining" in first.headers

    second = client.get("/test")
    assert second.status_code == 429
    assert "Retry-After" in second.headers
    assert "X-RateLimit-Limit" in second.headers
    assert "X-RateLimit-Remaining" in second.headers
