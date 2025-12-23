from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Dict, Iterable, Tuple


_RATE_KEY = Tuple[str, str]  # (scope, decision)


@dataclass(frozen=True)
class RateLimitMetric:
    scope: str
    decision: str


class RateLimitPrometheusMetrics:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._requests_total: Dict[_RATE_KEY, int] = {}

    def observe(self, *, scope: str, decision: str) -> None:
        s = str(scope or "ip")
        d = str(decision or "allowed")
        with self._lock:
            self._requests_total[(s, d)] = self._requests_total.get((s, d), 0) + 1

    def _iter_sorted(
        self, d: Dict[Tuple[str, ...], int]
    ) -> Iterable[Tuple[Tuple[str, ...], int]]:
        for k in sorted(d.keys()):
            yield k, d[k]

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            totals = dict(self._requests_total)
        summary: Dict[str, int] = {}
        by_scope: Dict[str, Dict[str, int]] = {}
        for (scope, decision), count in totals.items():
            sc = str(scope)
            dc = str(decision)
            summary[dc] = summary.get(dc, 0) + int(count)
            slot = by_scope.setdefault(sc, {})
            slot[dc] = slot.get(dc, 0) + int(count)
        return {"totals": summary, "by_scope": by_scope}

    def render(self) -> str:
        with self._lock:
            totals = dict(self._requests_total)

        lines: list[str] = []
        lines.append("# HELP wsa_rate_limit_requests_total Rate limit decisions.")
        lines.append("# TYPE wsa_rate_limit_requests_total counter")
        for (scope, decision), count in self._iter_sorted(totals):
            sc = str(scope).replace('"', '\\"')
            dc = str(decision).replace('"', '\\"')
            lines.append(
                f'wsa_rate_limit_requests_total{{scope="{sc}",decision="{dc}"}} {int(count)}'
            )

        return "\n".join(lines) + "\n"


REGISTRY = RateLimitPrometheusMetrics()
