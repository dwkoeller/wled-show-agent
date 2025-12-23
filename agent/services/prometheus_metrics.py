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
from services.audit_logger import log_event
from utils.outbound_metrics import REGISTRY as OUTBOUND_REGISTRY
from utils.rate_limit_metrics import REGISTRY as RATE_LIMIT_REGISTRY


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
        RATE_LIMIT_REGISTRY.render().rstrip("\n"),
    ]

    st = getattr(request.app.state, "wsa", None)
    if st is not None:
        # DB configured?
        lines.append("# HELP wsa_db_enabled Whether DATABASE_URL is configured.")
        lines.append("# TYPE wsa_db_enabled gauge")
        lines.append(
            f"wsa_db_enabled {1 if getattr(st, 'db', None) is not None else 0}"
        )

        events = getattr(st, "events", None)
        if events is not None and hasattr(events, "stats"):
            try:
                stats = await events.stats()
                lines.append(
                    "# HELP wsa_sse_clients Active server-sent event subscribers."
                )
                lines.append("# TYPE wsa_sse_clients gauge")
                lines.append(f"wsa_sse_clients {int(stats.get('subscribers', 0))}")

                lines.append(
                    "# HELP wsa_sse_history_size In-memory SSE history size."
                )
                lines.append("# TYPE wsa_sse_history_size gauge")
                lines.append(f"wsa_sse_history_size {int(stats.get('history', 0))}")

                lines.append(
                    "# HELP wsa_sse_history_max Configured max SSE history size."
                )
                lines.append("# TYPE wsa_sse_history_max gauge")
                lines.append(f"wsa_sse_history_max {int(stats.get('max_history', 0))}")
            except Exception:
                pass
        try:
            from services.events_service import get_spool_stats

            spool = await get_spool_stats(st)
            lines.append("# HELP wsa_sse_spool_bytes SSE spool queued bytes.")
            lines.append("# TYPE wsa_sse_spool_bytes gauge")
            lines.append(f"wsa_sse_spool_bytes {int(spool.get('queued_bytes', 0))}")

            lines.append("# HELP wsa_sse_spool_events SSE spool queued events.")
            lines.append("# TYPE wsa_sse_spool_events gauge")
            lines.append(f"wsa_sse_spool_events {int(spool.get('queued_events', 0))}")

            lines.append("# HELP wsa_sse_spool_dropped_total SSE spool dropped events.")
            lines.append("# TYPE wsa_sse_spool_dropped_total counter")
            lines.append(
                f"wsa_sse_spool_dropped_total {int(spool.get('dropped', 0))}"
            )
        except Exception:
            pass

        # Fleet presence (SQL heartbeats).
        db = getattr(st, "db", None)
        if db is not None and hasattr(db, "list_agent_heartbeats"):
            try:
                now = time.time()
                s = getattr(st, "settings", None)
                stale_after_s = float(getattr(s, "fleet_stale_after_s", 30.0))
                rows = await db.list_agent_heartbeats(limit=500)
                online = 0
                for r in rows:
                    try:
                        age = now - float((r or {}).get("updated_at") or 0.0)
                        if age <= stale_after_s:
                            online += 1
                    except Exception:
                        continue
                lines.append("# HELP wsa_fleet_agents_total Agents in heartbeat table.")
                lines.append("# TYPE wsa_fleet_agents_total gauge")
                lines.append(f"wsa_fleet_agents_total {int(len(rows))}")

                lines.append(
                    "# HELP wsa_fleet_agents_online Agents with a fresh heartbeat."
                )
                lines.append("# TYPE wsa_fleet_agents_online gauge")
                lines.append(f"wsa_fleet_agents_online {int(online)}")
            except Exception:
                pass

        # Blocking worker pools (backpressure).
        blocking = getattr(st, "blocking", None)
        if blocking is not None and hasattr(blocking, "stats"):
            try:
                stats = await blocking.stats()
                lines.append(
                    "# HELP wsa_blocking_inflight Blocking tasks in-flight."
                )
                lines.append("# TYPE wsa_blocking_inflight gauge")
                lines.append(f"wsa_blocking_inflight {int(stats.inflight)}")
                lines.append(
                    "# HELP wsa_blocking_max_workers Blocking pool max workers."
                )
                lines.append("# TYPE wsa_blocking_max_workers gauge")
                lines.append(f"wsa_blocking_max_workers {int(stats.max_workers)}")
                lines.append(
                    "# HELP wsa_blocking_max_queue Blocking pool max queue depth."
                )
                lines.append("# TYPE wsa_blocking_max_queue gauge")
                lines.append(f"wsa_blocking_max_queue {int(stats.max_queue)}")
            except Exception:
                pass

        ddp_blocking = getattr(st, "ddp_blocking", None)
        if ddp_blocking is not None and hasattr(ddp_blocking, "stats"):
            try:
                stats = await ddp_blocking.stats()
                lines.append(
                    "# HELP wsa_ddp_blocking_inflight DDP blocking tasks in-flight."
                )
                lines.append("# TYPE wsa_ddp_blocking_inflight gauge")
                lines.append(f"wsa_ddp_blocking_inflight {int(stats.inflight)}")
                lines.append(
                    "# HELP wsa_ddp_blocking_max_workers DDP blocking pool max workers."
                )
                lines.append("# TYPE wsa_ddp_blocking_max_workers gauge")
                lines.append(
                    f"wsa_ddp_blocking_max_workers {int(stats.max_workers)}"
                )
                lines.append(
                    "# HELP wsa_ddp_blocking_max_queue DDP blocking pool max queue depth."
                )
                lines.append("# TYPE wsa_ddp_blocking_max_queue gauge")
                lines.append(f"wsa_ddp_blocking_max_queue {int(stats.max_queue)}")
            except Exception:
                pass

        cpu_pool = getattr(st, "cpu_pool", None)
        if cpu_pool is not None and hasattr(cpu_pool, "stats"):
            try:
                stats = await cpu_pool.stats()
                lines.append("# HELP wsa_cpu_pool_inflight CPU pool tasks in-flight.")
                lines.append("# TYPE wsa_cpu_pool_inflight gauge")
                lines.append(f"wsa_cpu_pool_inflight {int(stats.inflight)}")
                lines.append("# HELP wsa_cpu_pool_max_workers CPU pool max workers.")
                lines.append("# TYPE wsa_cpu_pool_max_workers gauge")
                lines.append(f"wsa_cpu_pool_max_workers {int(stats.max_workers)}")
                lines.append("# HELP wsa_cpu_pool_max_queue CPU pool max queue depth.")
                lines.append("# TYPE wsa_cpu_pool_max_queue gauge")
                lines.append(f"wsa_cpu_pool_max_queue {int(stats.max_queue)}")
            except Exception:
                pass

        # MQTT bridge status.
        try:
            settings = getattr(st, "settings", None)
            mqtt_enabled = bool(
                getattr(settings, "mqtt_enabled", False)
                and getattr(settings, "mqtt_url", None)
            )
            lines.append("# HELP wsa_mqtt_enabled Whether MQTT bridge is configured.")
            lines.append("# TYPE wsa_mqtt_enabled gauge")
            lines.append(f"wsa_mqtt_enabled {1 if mqtt_enabled else 0}")
        except Exception:
            mqtt_enabled = False

        mqtt = getattr(st, "mqtt", None)
        if mqtt is not None and hasattr(mqtt, "status"):
            try:
                ms = await mqtt.status()
                lines.append("# HELP wsa_mqtt_running Whether MQTT bridge task is running.")
                lines.append("# TYPE wsa_mqtt_running gauge")
                lines.append(f"wsa_mqtt_running {1 if ms.get('running') else 0}")
                lines.append("# HELP wsa_mqtt_connected Whether MQTT is connected.")
                lines.append("# TYPE wsa_mqtt_connected gauge")
                lines.append(f"wsa_mqtt_connected {1 if ms.get('connected') else 0}")

                counters = ms.get("counters") or {}
                lines.append(
                    "# HELP wsa_mqtt_messages_received_total MQTT messages received."
                )
                lines.append("# TYPE wsa_mqtt_messages_received_total counter")
                lines.append(
                    f"wsa_mqtt_messages_received_total {int(counters.get('messages_received') or 0)}"
                )
                lines.append(
                    "# HELP wsa_mqtt_actions_ok_total MQTT actions processed successfully."
                )
                lines.append("# TYPE wsa_mqtt_actions_ok_total counter")
                lines.append(
                    f"wsa_mqtt_actions_ok_total {int(counters.get('actions_ok') or 0)}"
                )
                lines.append(
                    "# HELP wsa_mqtt_actions_failed_total MQTT actions failed."
                )
                lines.append("# TYPE wsa_mqtt_actions_failed_total counter")
                lines.append(
                    f"wsa_mqtt_actions_failed_total {int(counters.get('actions_failed') or 0)}"
                )

                last_error = ms.get("last_error")
                lines.append(
                    "# HELP wsa_mqtt_last_error Whether the MQTT bridge has a last_error set."
                )
                lines.append("# TYPE wsa_mqtt_last_error gauge")
                lines.append(f"wsa_mqtt_last_error {1 if last_error else 0}")

                last_error_at = ms.get("last_error_at")
                if last_error_at is not None:
                    try:
                        ts = float(last_error_at)
                    except Exception:
                        ts = None
                    if ts is not None:
                        lines.append(
                            "# HELP wsa_mqtt_last_error_timestamp_seconds Last MQTT error time."
                        )
                        lines.append(
                            "# TYPE wsa_mqtt_last_error_timestamp_seconds gauge"
                        )
                        lines.append(
                            f"wsa_mqtt_last_error_timestamp_seconds {ts:.3f}"
                        )
            except Exception:
                pass

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

            if hasattr(jobs, "queue_stats"):
                try:
                    qstats = jobs.queue_stats()
                except Exception:
                    qstats = {}
                if isinstance(qstats, dict):
                    lines.append("# HELP wsa_jobs_queue_depth Queued jobs depth.")
                    lines.append("# TYPE wsa_jobs_queue_depth gauge")
                    lines.append(
                        f"wsa_jobs_queue_depth {int(qstats.get('size') or 0)}"
                    )
                    lines.append("# HELP wsa_jobs_queue_max Queue capacity.")
                    lines.append("# TYPE wsa_jobs_queue_max gauge")
                    lines.append(f"wsa_jobs_queue_max {int(qstats.get('max') or 0)}")
                    lines.append("# HELP wsa_jobs_workers Configured job workers.")
                    lines.append("# TYPE wsa_jobs_workers gauge")
                    lines.append(
                        f"wsa_jobs_workers {int(qstats.get('workers') or 0)}"
                    )

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
                leader = 1 if bool(sst.get("leader")) else 0
                eligible = 1 if bool(sst.get("eligible")) else 0
                last_action_at = sst.get("last_action_at")
                last_error = sst.get("last_error")

                lines.append(
                    "# HELP wsa_scheduler_running Whether scheduler is running."
                )
                lines.append("# TYPE wsa_scheduler_running gauge")
                lines.append(f"wsa_scheduler_running {running}")

                lines.append(
                    "# HELP wsa_scheduler_in_window Whether current time is within the configured window."
                )
                lines.append("# TYPE wsa_scheduler_in_window gauge")
                lines.append(f"wsa_scheduler_in_window {in_window}")

                lines.append(
                    "# HELP wsa_scheduler_eligible Whether this node can be scheduler leader."
                )
                lines.append("# TYPE wsa_scheduler_eligible gauge")
                lines.append(f"wsa_scheduler_eligible {eligible}")

                lines.append("# HELP wsa_scheduler_leader Whether this node is leader.")
                lines.append("# TYPE wsa_scheduler_leader gauge")
                lines.append(f"wsa_scheduler_leader {leader}")

                lease = sst.get("lease") if isinstance(sst.get("lease"), dict) else None
                if lease:
                    owner = str(lease.get("owner_id") or "").strip() or None
                    expires_at = lease.get("expires_at")
                    if owner:
                        o = owner.replace('"', '\\"')
                        lines.append(
                            "# HELP wsa_scheduler_lease_owner_info Current scheduler lease owner."
                        )
                        lines.append("# TYPE wsa_scheduler_lease_owner_info gauge")
                        lines.append(
                            f'wsa_scheduler_lease_owner_info{{owner_id="{o}"}} 1'
                        )
                    if expires_at is not None:
                        try:
                            ts = float(expires_at)
                        except Exception:
                            ts = None
                        if ts is not None:
                            lines.append(
                                "# HELP wsa_scheduler_lease_expires_timestamp_seconds Scheduler lease expiry time."
                            )
                            lines.append(
                                "# TYPE wsa_scheduler_lease_expires_timestamp_seconds gauge"
                            )
                            lines.append(
                                f"wsa_scheduler_lease_expires_timestamp_seconds {ts:.3f}"
                            )

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

        # DDP streaming metrics.
        ddp = getattr(st, "ddp", None)
        if ddp is not None:
            try:
                dst = await ddp.status()
                running = 1 if bool(getattr(dst, "running", False)) else 0
                lines.append("# HELP wsa_ddp_running Whether DDP streaming is running.")
                lines.append("# TYPE wsa_ddp_running gauge")
                lines.append(f"wsa_ddp_running {running}")
            except Exception:
                pass
            try:
                m = await ddp.metrics()
                lines.append(
                    "# HELP wsa_ddp_frames_sent_total Total DDP frames sent."
                )
                lines.append("# TYPE wsa_ddp_frames_sent_total counter")
                lines.append(
                    f"wsa_ddp_frames_sent_total {int(getattr(m, 'frames_sent_total', 0))}"
                )

                lines.append(
                    "# HELP wsa_ddp_frames_dropped_total Total DDP frames dropped for backpressure."
                )
                lines.append("# TYPE wsa_ddp_frames_dropped_total counter")
                lines.append(
                    f"wsa_ddp_frames_dropped_total {int(getattr(m, 'frames_dropped_total', 0))}"
                )

                lines.append(
                    "# HELP wsa_ddp_frame_overruns_total Frames that exceeded the target frame period."
                )
                lines.append("# TYPE wsa_ddp_frame_overruns_total counter")
                lines.append(
                    f"wsa_ddp_frame_overruns_total {int(getattr(m, 'frame_overruns_total', 0))}"
                )

                lines.append(
                    "# HELP wsa_ddp_frame_compute_seconds Frame compute+send duration summary."
                )
                lines.append("# TYPE wsa_ddp_frame_compute_seconds summary")
                lines.append(
                    f"wsa_ddp_frame_compute_seconds_count {int(getattr(m, 'frame_compute_seconds_count', 0))}"
                )
                lines.append(
                    f"wsa_ddp_frame_compute_seconds_sum {float(getattr(m, 'frame_compute_seconds_sum', 0.0)):.6f}"
                )

                lines.append(
                    "# HELP wsa_ddp_frame_lag_seconds Frame lag (lateness vs schedule) summary."
                )
                lines.append("# TYPE wsa_ddp_frame_lag_seconds summary")
                lines.append(
                    f"wsa_ddp_frame_lag_seconds_count {int(getattr(m, 'frame_lag_seconds_count', 0))}"
                )
                lines.append(
                    f"wsa_ddp_frame_lag_seconds_sum {float(getattr(m, 'frame_lag_seconds_sum', 0.0)):.6f}"
                )

                lines.append(
                    "# HELP wsa_ddp_frame_lag_seconds_max Max observed frame lag."
                )
                lines.append("# TYPE wsa_ddp_frame_lag_seconds_max gauge")
                lines.append(
                    f"wsa_ddp_frame_lag_seconds_max {float(getattr(m, 'max_frame_lag_s', 0.0)):.6f}"
                )

                last_compute = getattr(m, "last_frame_compute_s", None)
                if last_compute is not None:
                    lines.append(
                        "# HELP wsa_ddp_last_frame_compute_seconds Last frame compute+send duration."
                    )
                    lines.append("# TYPE wsa_ddp_last_frame_compute_seconds gauge")
                    lines.append(
                        f"wsa_ddp_last_frame_compute_seconds {float(last_compute):.6f}"
                    )

                last_lag = getattr(m, "last_frame_lag_s", None)
                if last_lag is not None:
                    lines.append(
                        "# HELP wsa_ddp_last_frame_lag_seconds Last frame lag value."
                    )
                    lines.append("# TYPE wsa_ddp_last_frame_lag_seconds gauge")
                    lines.append(
                        f"wsa_ddp_last_frame_lag_seconds {float(last_lag):.6f}"
                    )
            except Exception:
                pass

    body = "\n".join(lines).rstrip("\n") + "\n"
    st = getattr(request.app.state, "wsa", None)
    if st is not None:
        try:
            await log_event(
                st,
                action="metrics.prometheus",
                ok=True,
                payload={"format": "prometheus"},
                request=request,
            )
        except Exception:
            pass
    return PlainTextResponse(
        body,
        media_type="text/plain; version=0.0.4; charset=utf-8",
    )
