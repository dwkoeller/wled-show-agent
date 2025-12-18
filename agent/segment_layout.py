from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence

from wled_client import WLEDClient


@dataclass(frozen=True)
class SegmentRange:
    """A segment range in global pixel coordinates (start inclusive, stop exclusive)."""

    id: int
    start: int
    stop: int

    @property
    def length(self) -> int:
        return max(0, int(self.stop) - int(self.start))


@dataclass(frozen=True)
class SegmentLayout:
    """Derived segment layout for a WLED device."""

    led_count: int
    segments: List[SegmentRange]
    kind: str  # e.g. "quarters", "equal", "unknown"

    def ordered_ids(self) -> List[int]:
        return [s.id for s in self.segments]

    def id_to_order(self) -> Dict[int, int]:
        return {s.id: i for i, s in enumerate(self.segments)}

    def segment_for_index(self, idx: int) -> Optional[int]:
        """Return segment ID containing global LED index, if known."""
        i = int(idx)
        for s in self.segments:
            if s.start <= i < s.stop:
                return s.id
        return None

    def order_for_index(self, idx: int) -> Optional[int]:
        sid = self.segment_for_index(idx)
        if sid is None:
            return None
        return self.id_to_order().get(sid)

    def local_index(self, idx: int) -> Optional[int]:
        i = int(idx)
        for s in self.segments:
            if s.start <= i < s.stop:
                return i - s.start
        return None


def _coerce_int(x: object, default: int) -> int:
    try:
        return int(x)
    except Exception:
        return default


def _fallback_equal_layout(led_count: int, segment_ids: Sequence[int]) -> SegmentLayout:
    if not segment_ids:
        return SegmentLayout(
            led_count=led_count,
            segments=[SegmentRange(id=0, start=0, stop=led_count)],
            kind="equal",
        )

    n = len(segment_ids)
    if led_count <= 0:
        segs = [SegmentRange(id=int(sid), start=0, stop=0) for sid in segment_ids]
        return SegmentLayout(
            led_count=led_count, segments=segs, kind=("quarters" if n == 4 else "equal")
        )

    base_len = led_count // n
    segs: List[SegmentRange] = []
    start = 0
    for i, sid in enumerate(segment_ids):
        stop = start + base_len
        if i == n - 1:
            stop = led_count
        segs.append(SegmentRange(id=int(sid), start=start, stop=stop))
        start = stop

    return SegmentLayout(
        led_count=led_count, segments=segs, kind=("quarters" if n == 4 else "equal")
    )


def fetch_segment_layout(
    wled: WLEDClient,
    *,
    segment_ids: Optional[Sequence[int]] = None,
    refresh: bool = True,
) -> SegmentLayout:
    """Best-effort: fetch segment bounds from WLED /json/state; fall back to equal partitions."""

    # LED count
    led_count = 0
    try:
        led_count = int(wled.device_info().led_count)
    except Exception:
        led_count = 0

    wanted = [int(x) for x in (segment_ids or [])]
    wanted_set = set(wanted)

    segs_raw: List[Dict[str, object]] = []
    try:
        segs = wled.get_segments(refresh=refresh)
        for s in segs:
            if isinstance(s, dict):
                segs_raw.append(s)
    except Exception:
        segs_raw = []

    parsed: List[SegmentRange] = []
    for s in segs_raw:
        sid = _coerce_int(s.get("id"), -1)
        if sid < 0:
            continue
        if wanted and sid not in wanted_set:
            continue

        start = _coerce_int(s.get("start"), 0)
        stop = _coerce_int(s.get("stop"), 0)
        if stop <= 0:
            ln = _coerce_int(s.get("len"), 0)
            if ln > 0:
                stop = start + ln

        if led_count > 0:
            start = max(0, min(led_count, start))
            stop = max(0, min(led_count, stop))
        if stop <= start:
            continue

        parsed.append(SegmentRange(id=sid, start=start, stop=stop))

    if not parsed:
        return _fallback_equal_layout(led_count, wanted or [0])

    parsed.sort(key=lambda r: (r.start, r.id))

    # infer layout kind (mostly for debugging / UI)
    kind = "unknown"
    if len(parsed) == 4:
        lengths = [s.length for s in parsed]
        avg = sum(lengths) / max(1, len(lengths))
        if avg > 0:
            ok_len = all(abs(l - avg) / avg <= 0.10 for l in lengths)
            contiguous = True
            for i in range(1, len(parsed)):
                if parsed[i].start != parsed[i - 1].stop:
                    contiguous = False
                    break
            if ok_len:
                kind = "quarters" if contiguous else "equal"
    else:
        kind = "equal"

    return SegmentLayout(led_count=led_count, segments=parsed, kind=kind)
