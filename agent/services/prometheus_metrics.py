from __future__ import annotations

import threading
import time
from dataclasses import dataclass
import inspect
from typing import Dict, Iterable, Tuple

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import PlainTextResponse
from starlette.types import ASGIApp

from config.constants import APP_VERSION, SERVICE_NAME
from utils.outbound_metrics import REGISTRY as OUTBOUND_REGISTRY


_REQ_COUNT_KEY = Tuple[str, str, str]  # (method, route, status_code)
_REQ_SUM_KEY = Tuple[str, str]  # (method, route)


@dataclass(frozen=True)
class RequestMetric:
    method: str
    route: str
    status_code: str


class PrometheusMetrics:
    def __init__(self) -> None:
        self._started_at = time.time()
        self._lock = threading.Lock()
        self._requests_total: Dict[_REQ_COUNT_KEY, int] = {}
        self._request_duration_sum_s: Dict[_REQ_SUM_KEY, float] = {}
        self._request_duration_count: Dict[_REQ_SUM_KEY, int] = {}

    @property
    def started_at(self) -> float:
        return float(self._started_at)

    def observe_request(
        self,
        *,
        method: str,
        route: str,
        status_code: int,
        duration_s: float,
    ) -> None:
        m = str(method).upper()
        r = str(route or "/")
        sc = str(int(status_code))
        dur = max(0.0, float(duration_s))

        with self._lock:
            self._requests_total[(m, r, sc)] = (
                self._requests_total.get((m, r, sc), 0) + 1
            )
            self._request_duration_sum_s[(m, r)] = (
                self._request_duration_sum_s.get((m, r), 0.0) + dur
            )
            self._request_duration_count[(m, r)] = (
                self._request_duration_count.get((m, r), 0) + 1
            )

    def _iter_sorted(
        self, d: Dict[Tuple[str, ...], float | int]
    ) -> Iterable[Tuple[Tuple[str, ...], float | int]]:
        for k in sorted(d.keys()):
            yield k, d[k]

    def render(self) -> str:
        now = time.time()
        uptime_s = max(0.0, now - self._started_at)

        with self._lock:
            req_total = dict(self._requests_total)
            dur_sum = dict(self._request_duration_sum_s)
            dur_count = dict(self._request_duration_count)

        lines: list[str] = []

        lines.append("# HELP wsa_build_info Build and version info.")
        lines.append("# TYPE wsa_build_info gauge")
        lines.append(
            f'wsa_build_info{{service="{SERVICE_NAME}",version="{APP_VERSION}"}} 1'
        )

        lines.append("# HELP wsa_uptime_seconds Process uptime in seconds.")
        lines.append("# TYPE wsa_uptime_seconds gauge")
        lines.append(f"wsa_uptime_seconds {uptime_s:.3f}")

        lines.append("# HELP wsa_http_requests_total Total HTTP requests.")
        lines.append("# TYPE wsa_http_requests_total counter")
        for (method, route, status_code), count in self._iter_sorted(req_total):
            lines.append(
                f'wsa_http_requests_total{{method="{method}",route="{route}",status_code="{status_code}"}} {int(count)}'
            )

        lines.append(
            "# HELP wsa_http_request_duration_seconds HTTP request duration summary."
        )
        lines.append("# TYPE wsa_http_request_duration_seconds summary")
        for (method, route), count in self._iter_sorted(dur_count):
            s = float(dur_sum.get((method, route), 0.0))
            lines.append(
                f'wsa_http_request_duration_seconds_count{{method="{method}",route="{route}"}} {int(count)}'
            )
            lines.append(
                f'wsa_http_request_duration_seconds_sum{{method="{method}",route="{route}"}} {s:.6f}'
            )

        return "\n".join(lines) + "\n"


REGISTRY = PrometheusMetrics()


class PrometheusMetricsMiddleware(BaseHTTPMiddleware):
    def __init__(
        self,
        app: ASGIApp,
        *,
        registry: PrometheusMetrics = REGISTRY,
        skip_paths: Tuple[str, ...] = ("/metrics",),
    ) -> None:
        super().__init__(app)
        self._registry = registry
        self._skip_paths = tuple(str(p) for p in (skip_paths or ()))

    async def dispatch(self, request: Request, call_next):  # type: ignore[no-untyped-def]
        start = time.perf_counter()
        response = await call_next(request)
        dur_s = max(0.0, time.perf_counter() - start)

        path = request.url.path or "/"
        if path in self._skip_paths:
            return response

        route_obj = request.scope.get("route")
        route = getattr(route_obj, "path", None) or path
        self._registry.observe_request(
            method=request.method,
            route=str(route),
            status_code=int(response.status_code),
            duration_s=float(dur_s),
        )
        return response


def metrics_endpoint() -> PlainTextResponse:
    return PlainTextResponse(
        REGISTRY.render(),
        media_type="text/plain; version=0.0.4; charset=utf-8",
    )


async def metrics_endpoint_with_state(request: Request) -> PlainTextResponse:
    """
    Prometheus exposition that includes request metrics plus basic scheduler/job gauges.
    """
    lines: list[str] = [
        REGISTRY.render().rstrip("\n"),
        OUTBOUND_REGISTRY.render().rstrip("\n"),
    ]

    st = getattr(request.app.state, "wsa", None)
    if st is not None:
        # DB configured?
        lines.append("# HELP wsa_db_enabled Whether DATABASE_URL is configured.")
        lines.append("# TYPE wsa_db_enabled gauge")
        lines.append(
            f"wsa_db_enabled {1 if getattr(st, 'db', None) is not None else 0}"
        )

        # Jobs (in-memory) by status.
        jobs = getattr(st, "jobs", None)
        if jobs is not None and hasattr(jobs, "status_counts"):
            try:
                counts = jobs.status_counts()
            except Exception:
                counts = {}
            lines.append("# HELP wsa_jobs_status_total In-memory jobs by status.")
            lines.append("# TYPE wsa_jobs_status_total gauge")
            for status, count in sorted(counts.items()):
                s = str(status).replace('"', '\\"')
                lines.append(f'wsa_jobs_status_total{{status="{s}"}} {int(count)}')

        # Scheduler status.
        sched = getattr(st, "scheduler", None)
        if sched is not None and hasattr(sched, "status"):
            try:
                sst_res = sched.status()
                sst = await sst_res if inspect.isawaitable(sst_res) else sst_res
            except Exception:
                sst = None
            if isinstance(sst, dict):
                running = 1 if bool(sst.get("running")) else 0
                in_window = 1 if bool(sst.get("in_window")) else 0
                last_action_at = sst.get("last_action_at")
                last_error = sst.get("last_error")

                lines.append(
                    "# HELP wsa_scheduler_running Whether scheduler thread is running."
                )
                lines.append("# TYPE wsa_scheduler_running gauge")
                lines.append(f"wsa_scheduler_running {running}")

                lines.append(
                    "# HELP wsa_scheduler_in_window Whether current time is within the configured window."
                )
                lines.append("# TYPE wsa_scheduler_in_window gauge")
                lines.append(f"wsa_scheduler_in_window {in_window}")

                if last_action_at is not None:
                    try:
                        ts = float(last_action_at)
                    except Exception:
                        ts = None
                    if ts is not None:
                        lines.append(
                            "# HELP wsa_scheduler_last_action_timestamp_seconds Last scheduler action time."
                        )
                        lines.append(
                            "# TYPE wsa_scheduler_last_action_timestamp_seconds gauge"
                        )
                        lines.append(
                            f"wsa_scheduler_last_action_timestamp_seconds {ts:.3f}"
                        )

                lines.append(
                    "# HELP wsa_scheduler_last_error Whether the scheduler has a last_error set."
                )
                lines.append("# TYPE wsa_scheduler_last_error gauge")
                lines.append(f"wsa_scheduler_last_error {1 if last_error else 0}")

    body = "\n".join(lines).rstrip("\n") + "\n"
    return PlainTextResponse(
        body,
        media_type="text/plain; version=0.0.4; charset=utf-8",
    )
