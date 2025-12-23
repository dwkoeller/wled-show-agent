from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Dict, Iterable, Tuple


_FAIL_KEY = Tuple[str, str, str, str]  # (target_kind, target, method, reason)
_DUR_KEY = Tuple[str, str, str]  # (target_kind, target, method)
_RETRY_KEY = Tuple[str, str, str]  # (target_kind, target, method)


@dataclass(frozen=True)
class OutboundFailureMetric:
    target_kind: str
    target: str
    method: str
    reason: str


class OutboundPrometheusMetrics:
    def __init__(self) -> None:
        self._started_at = time.time()
        self._lock = threading.Lock()
        self._failures_total: Dict[_FAIL_KEY, int] = {}
        self._request_duration_sum_s: Dict[_DUR_KEY, float] = {}
        self._request_duration_count: Dict[_DUR_KEY, int] = {}
        self._retries_total: Dict[_RETRY_KEY, int] = {}

    def observe_success(
        self,
        *,
        target_kind: str,
        target: str,
        method: str,
        duration_s: float,
    ) -> None:
        tk = str(target_kind)
        t = str(target)
        m = str(method).upper()
        dur = max(0.0, float(duration_s))
        with self._lock:
            self._request_duration_sum_s[(tk, t, m)] = (
                self._request_duration_sum_s.get((tk, t, m), 0.0) + dur
            )
            self._request_duration_count[(tk, t, m)] = (
                self._request_duration_count.get((tk, t, m), 0) + 1
            )

    def observe_failure(
        self,
        *,
        target_kind: str,
        target: str,
        method: str,
        reason: str,
        duration_s: float,
    ) -> None:
        tk = str(target_kind)
        t = str(target)
        m = str(method).upper()
        r = str(reason)
        dur = max(0.0, float(duration_s))
        with self._lock:
            self._failures_total[(tk, t, m, r)] = (
                self._failures_total.get((tk, t, m, r), 0) + 1
            )
            self._request_duration_sum_s[(tk, t, m)] = (
                self._request_duration_sum_s.get((tk, t, m), 0.0) + dur
            )
            self._request_duration_count[(tk, t, m)] = (
                self._request_duration_count.get((tk, t, m), 0) + 1
            )

    def observe_retry(
        self,
        *,
        target_kind: str,
        target: str,
        method: str,
    ) -> None:
        tk = str(target_kind)
        t = str(target)
        m = str(method).upper()
        with self._lock:
            self._retries_total[(tk, t, m)] = self._retries_total.get((tk, t, m), 0) + 1

    def _iter_sorted(
        self, d: Dict[Tuple[str, ...], float | int]
    ) -> Iterable[Tuple[Tuple[str, ...], float | int]]:
        for k in sorted(d.keys()):
            yield k, d[k]

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            failures = dict(self._failures_total)
            dur_sum = dict(self._request_duration_sum_s)
            dur_count = dict(self._request_duration_count)
            retries = dict(self._retries_total)

        total_failures = sum(int(v) for v in failures.values())
        total_retries = sum(int(v) for v in retries.values())
        by_kind: Dict[str, Dict[str, float | int]] = {}

        for (target_kind, _target, _method, _reason), count in failures.items():
            k = str(target_kind)
            slot = by_kind.setdefault(k, {"failures": 0, "retries": 0, "avg_latency_s": 0.0})
            slot["failures"] = int(slot.get("failures", 0)) + int(count)

        for (target_kind, _target, _method), count in retries.items():
            k = str(target_kind)
            slot = by_kind.setdefault(k, {"failures": 0, "retries": 0, "avg_latency_s": 0.0})
            slot["retries"] = int(slot.get("retries", 0)) + int(count)

        latency_by_kind: Dict[str, float] = {}
        counts_by_kind: Dict[str, int] = {}
        for (target_kind, _target, _method), count in dur_count.items():
            k = str(target_kind)
            counts_by_kind[k] = counts_by_kind.get(k, 0) + int(count)
        for (target_kind, _target, _method), total in dur_sum.items():
            k = str(target_kind)
            latency_by_kind[k] = latency_by_kind.get(k, 0.0) + float(total)
        for k in set(list(counts_by_kind.keys()) + list(latency_by_kind.keys())):
            denom = float(counts_by_kind.get(k, 0) or 0)
            avg = float(latency_by_kind.get(k, 0.0)) / denom if denom > 0 else 0.0
            slot = by_kind.setdefault(k, {"failures": 0, "retries": 0, "avg_latency_s": 0.0})
            slot["avg_latency_s"] = float(avg)

        return {
            "failures_total": int(total_failures),
            "retries_total": int(total_retries),
            "by_target_kind": by_kind,
        }

    def render(self) -> str:
        with self._lock:
            failures = dict(self._failures_total)
            dur_sum = dict(self._request_duration_sum_s)
            dur_count = dict(self._request_duration_count)
            retries = dict(self._retries_total)

        lines: list[str] = []

        lines.append("# HELP wsa_outbound_failures_total Outbound request failures.")
        lines.append("# TYPE wsa_outbound_failures_total counter")
        for (target_kind, target, method, reason), count in self._iter_sorted(failures):
            tk = str(target_kind).replace('"', '\\"')
            t = str(target).replace('"', '\\"')
            m = str(method).replace('"', '\\"')
            r = str(reason).replace('"', '\\"')
            lines.append(
                f'wsa_outbound_failures_total{{target_kind="{tk}",target="{t}",method="{m}",reason="{r}"}} {int(count)}'
            )

        lines.append(
            "# HELP wsa_outbound_request_duration_seconds Outbound request duration summary."
        )
        lines.append("# TYPE wsa_outbound_request_duration_seconds summary")
        for (target_kind, target, method), count in self._iter_sorted(dur_count):
            tk = str(target_kind).replace('"', '\\"')
            t = str(target).replace('"', '\\"')
            m = str(method).replace('"', '\\"')
            s = float(dur_sum.get((target_kind, target, method), 0.0))
            lines.append(
                f'wsa_outbound_request_duration_seconds_count{{target_kind="{tk}",target="{t}",method="{m}"}} {int(count)}'
            )
            lines.append(
                f'wsa_outbound_request_duration_seconds_sum{{target_kind="{tk}",target="{t}",method="{m}"}} {s:.6f}'
            )

        lines.append("# HELP wsa_outbound_retries_total Outbound retries performed.")
        lines.append("# TYPE wsa_outbound_retries_total counter")
        for (target_kind, target, method), count in self._iter_sorted(retries):
            tk = str(target_kind).replace('"', '\\"')
            t = str(target).replace('"', '\\"')
            m = str(method).replace('"', '\\"')
            lines.append(
                f'wsa_outbound_retries_total{{target_kind="{tk}",target="{t}",method="{m}"}} {int(count)}'
            )

        return "\n".join(lines) + "\n"


REGISTRY = OutboundPrometheusMetrics()
