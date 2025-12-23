from __future__ import annotations

import asyncio
import math
import time
from dataclasses import dataclass
from typing import Dict, Iterable

from fastapi import Request
from fastapi.responses import JSONResponse

from config import load_settings
from utils.rate_limit_metrics import REGISTRY as RATE_METRICS


@dataclass
class _Bucket:
    tokens: float
    updated_at: float
    last_seen: float


class RateLimiter:
    def __init__(
        self,
        *,
        rate_per_s: float,
        burst: int,
        bucket_ttl_s: float,
        cleanup_interval_s: float,
    ) -> None:
        self._rate_per_s = max(0.0, float(rate_per_s))
        self._burst = max(1, int(burst))
        self._bucket_ttl_s = max(1.0, float(bucket_ttl_s))
        self._cleanup_interval_s = max(1.0, float(cleanup_interval_s))
        self._buckets: Dict[str, _Bucket] = {}
        self._lock = asyncio.Lock()
        self._last_cleanup = 0.0

    async def check(self, key: str, *, cost: float = 1.0) -> tuple[bool, float | None, int]:
        now = time.monotonic()
        async with self._lock:
            bucket = self._buckets.get(key)
            if bucket is None:
                bucket = _Bucket(tokens=float(self._burst), updated_at=now, last_seen=now)
                self._buckets[key] = bucket

            elapsed = max(0.0, now - float(bucket.updated_at))
            if self._rate_per_s > 0:
                bucket.tokens = min(
                    float(self._burst),
                    float(bucket.tokens) + (elapsed * float(self._rate_per_s)),
                )
            bucket.updated_at = now
            bucket.last_seen = now

            retry_after: float | None = None
            if bucket.tokens >= float(cost):
                bucket.tokens -= float(cost)
                allowed = True
            else:
                allowed = False
                if self._rate_per_s > 0:
                    retry_after = (float(cost) - float(bucket.tokens)) / float(
                        self._rate_per_s
                    )

            if now - self._last_cleanup >= self._cleanup_interval_s:
                self._cleanup(now)
                self._last_cleanup = now

            remaining = max(0, int(math.floor(bucket.tokens)))
            return allowed, retry_after, remaining

    def _cleanup(self, now: float) -> None:
        ttl = float(self._bucket_ttl_s)
        if ttl <= 0:
            return
        expired = [k for k, b in self._buckets.items() if now - b.last_seen > ttl]
        for k in expired:
            self._buckets.pop(k, None)


_SETTINGS_CACHE = None
_LIMITER: RateLimiter | None = None
_LIMIT_RPM: int | None = None
_LIMIT_BURST: int | None = None


def _get_settings(request: Request):
    st = getattr(request.app.state, "wsa", None)
    if st is not None:
        return st.settings
    global _SETTINGS_CACHE
    if _SETTINGS_CACHE is not None:
        return _SETTINGS_CACHE
    try:
        _SETTINGS_CACHE = load_settings()
    except Exception:
        _SETTINGS_CACHE = None
    return _SETTINGS_CACHE


def _client_ip(request: Request, trust_proxy: bool) -> str:
    if trust_proxy:
        raw = request.headers.get("x-forwarded-for") or ""
        if raw:
            return raw.split(",")[0].strip() or "unknown"
    try:
        return request.client.host if request.client else "unknown"
    except Exception:
        return "unknown"


def _build_exempt_prefixes(extra: Iterable[str] | None) -> tuple[str, ...]:
    defaults = (
        "/ui",
        "/readyz",
        "/livez",
        "/v1/health",
        "/metrics",
        "/v1/metrics",
        "/v1/events",
        "/openapi.json",
        "/docs",
        "/favicon.ico",
    )
    out = list(defaults)
    for p in extra or []:
        s = str(p or "").strip()
        if not s:
            continue
        out.append(s)
    return tuple(out)


async def rate_limit_middleware(request: Request, call_next):  # type: ignore[no-untyped-def]
    settings = _get_settings(request)
    if settings is None or not getattr(settings, "rate_limit_enabled", False):
        return await call_next(request)

    if request.method.upper() == "OPTIONS":
        return await call_next(request)

    path = request.url.path or ""
    exempt = _build_exempt_prefixes(
        getattr(settings, "rate_limit_exempt_paths", None)
    )
    if any(path.startswith(pfx) for pfx in exempt):
        return await call_next(request)

    global _LIMITER
    if _LIMITER is None:
        rpm = int(getattr(settings, "rate_limit_requests_per_minute", 600))
        rate_per_s = float(rpm) / 60.0
        global _LIMIT_RPM, _LIMIT_BURST
        _LIMIT_RPM = rpm
        _LIMITER = RateLimiter(
            rate_per_s=rate_per_s,
            burst=int(getattr(settings, "rate_limit_burst", 120)),
            bucket_ttl_s=float(getattr(settings, "rate_limit_bucket_ttl_s", 600.0)),
            cleanup_interval_s=float(
                getattr(settings, "rate_limit_cleanup_interval_s", 30.0)
            ),
        )
        _LIMIT_BURST = int(getattr(settings, "rate_limit_burst", 120))

    scope = str(getattr(settings, "rate_limit_scope", "ip")).strip().lower() or "ip"
    trust_proxy = bool(getattr(settings, "rate_limit_trust_proxy_headers", False))
    ip = _client_ip(request, trust_proxy)
    key = ip if scope == "ip" else f"{ip}:{path}"

    allowed, retry_after, remaining = await _LIMITER.check(key)
    try:
        RATE_METRICS.observe(
            scope=scope,
            decision="allowed" if allowed else "blocked",
        )
    except Exception:
        pass
    limit_hdr = str(_LIMIT_RPM or 0)
    remaining_hdr = str(max(0, int(remaining)))
    if not allowed:
        headers = {
            "Retry-After": str(
                max(1, int(math.ceil(float(retry_after or 1.0))))
            ),
            "X-RateLimit-Limit": limit_hdr,
            "X-RateLimit-Remaining": remaining_hdr,
        }
        return JSONResponse(
            status_code=429,
            content={"detail": "Too many requests. Slow down."},
            headers=headers,
        )

    response = await call_next(request)
    try:
        response.headers["X-RateLimit-Limit"] = limit_hdr
        response.headers["X-RateLimit-Remaining"] = remaining_hdr
        if _LIMIT_BURST is not None:
            response.headers["X-RateLimit-Burst"] = str(_LIMIT_BURST)
    except Exception:
        pass
    return response
