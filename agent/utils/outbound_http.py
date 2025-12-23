from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

import httpx

from utils.outbound_metrics import REGISTRY as OUTBOUND_METRICS


@dataclass(frozen=True)
class RetryPolicy:
    attempts: int = 2
    backoff_base_s: float = 0.15
    backoff_max_s: float = 1.0
    retry_status_codes: tuple[int, ...] = (408, 425, 429, 500, 502, 503, 504)


def _classify_exc(e: Exception) -> str:
    if isinstance(e, httpx.TimeoutException):
        return "timeout"
    if isinstance(e, httpx.TransportError):
        return "network"
    return "error"


def _classify_status(code: int) -> str:
    sc = int(code)
    if 400 <= sc < 500:
        return "http_4xx"
    if 500 <= sc < 600:
        return "http_5xx"
    return "http_error"


async def request_with_retry(
    *,
    client: httpx.AsyncClient,
    method: str,
    url: str,
    target_kind: str,
    target: str,
    timeout_s: float,
    retry: RetryPolicy | None = None,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, Any]] = None,
    json_body: Any = None,
    data: Any = None,
    files: Any = None,
) -> httpx.Response:
    """
    Best-effort outbound request wrapper with retries/backoff + Prometheus metrics.

    - Uses per-attempt timeout (`timeout_s`).
    - Retries on network/timeout exceptions and common transient HTTP status codes.
    - Records success/failure + latency metrics by (target_kind,target,method).
    """
    pol = retry or RetryPolicy()
    attempts = max(1, int(pol.attempts))
    m = str(method).upper()
    start = time.perf_counter()

    last_exc: Exception | None = None
    last_resp: httpx.Response | None = None

    for attempt in range(1, attempts + 1):
        try:
            resp = await client.request(
                method=m,
                url=str(url),
                params=params,
                json=json_body,
                data=data,
                files=files,
                headers=headers,
                timeout=float(timeout_s),
            )
            last_resp = resp

            if resp.status_code < 400:
                OUTBOUND_METRICS.observe_success(
                    target_kind=target_kind,
                    target=target,
                    method=m,
                    duration_s=max(0.0, time.perf_counter() - start),
                )
                return resp

            # Retry transient errors.
            if resp.status_code in pol.retry_status_codes and attempt < attempts:
                OUTBOUND_METRICS.observe_retry(
                    target_kind=target_kind,
                    target=target,
                    method=m,
                )
                try:
                    await resp.aclose()
                except Exception:
                    pass
                backoff = min(
                    float(pol.backoff_max_s),
                    float(pol.backoff_base_s) * (2.0 ** float(attempt - 1)),
                )
                # Full jitter.
                await asyncio.sleep(random.random() * max(0.0, backoff))
                continue

            OUTBOUND_METRICS.observe_failure(
                target_kind=target_kind,
                target=target,
                method=m,
                reason=_classify_status(int(resp.status_code)),
                duration_s=max(0.0, time.perf_counter() - start),
            )
            return resp
        except Exception as e:
            last_exc = e
            reason = _classify_exc(e)
            if attempt < attempts and reason in ("timeout", "network"):
                OUTBOUND_METRICS.observe_retry(
                    target_kind=target_kind,
                    target=target,
                    method=m,
                )
                backoff = min(
                    float(pol.backoff_max_s),
                    float(pol.backoff_base_s) * (2.0 ** float(attempt - 1)),
                )
                await asyncio.sleep(random.random() * max(0.0, backoff))
                continue

            OUTBOUND_METRICS.observe_failure(
                target_kind=target_kind,
                target=target,
                method=m,
                reason=reason,
                duration_s=max(0.0, time.perf_counter() - start),
            )
            raise

    # Should never happen, but keep sane behavior.
    if last_resp is not None:
        return last_resp
    if last_exc is not None:
        raise last_exc
    raise RuntimeError("request_with_retry failed without response or exception")


def retry_policy_from_settings(settings: Any) -> RetryPolicy:
    """
    Build a RetryPolicy from settings with safe defaults.
    """
    try:
        attempts = int(getattr(settings, "outbound_retry_attempts", 2))
    except Exception:
        attempts = 2
    try:
        backoff_base_s = float(getattr(settings, "outbound_retry_backoff_base_s", 0.15))
    except Exception:
        backoff_base_s = 0.15
    try:
        backoff_max_s = float(getattr(settings, "outbound_retry_backoff_max_s", 1.0))
    except Exception:
        backoff_max_s = 1.0
    try:
        raw_codes = getattr(settings, "outbound_retry_status_codes", None)
        codes = tuple(int(x) for x in (raw_codes or ())) if raw_codes else ()
    except Exception:
        codes = ()
    if not codes:
        codes = (408, 425, 429, 500, 502, 503, 504)
    return RetryPolicy(
        attempts=max(1, int(attempts)),
        backoff_base_s=max(0.0, float(backoff_base_s)),
        backoff_max_s=max(0.0, float(backoff_max_s)),
        retry_status_codes=tuple(codes),
    )
