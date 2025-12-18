from __future__ import annotations

import io
import statistics
import zipfile
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


class XlightsSequenceImportError(RuntimeError):
    pass


@dataclass(frozen=True)
class XlightsTimingTrack:
    """
    A best-effort representation of an xLights timing track.

    xLights `.xsq` formats vary between versions; this importer focuses on extracting ordered
    timing marks (seconds) which can be used as a beat grid for sequence generation.
    """

    name: str
    marks_s: List[float]
    raw: Dict[str, Any]


def _read_xsq_xml_text(path: str) -> str:
    p = Path(path)
    if not p.is_file():
        raise XlightsSequenceImportError("xLights .xsq file not found")

    data = p.read_bytes()

    # Some `.xsq` files are plain XML, others may be a ZIP container.
    if data[:2] == b"PK":
        try:
            zf = zipfile.ZipFile(io.BytesIO(data))
        except Exception as e:
            raise XlightsSequenceImportError(f"Failed to open .xsq ZIP: {e}")
        with zf:
            names = [
                n
                for n in zf.namelist()
                if not n.endswith("/")
                and (n.lower().endswith(".xml") or n.lower().endswith(".xsq"))
            ]
            if not names:
                raise XlightsSequenceImportError("No XML entries found inside .xsq ZIP")
            # Prefer a top-level XML file; otherwise pick the first candidate.
            names.sort(key=lambda n: ("/" in n, len(n), n))
            try:
                raw = zf.read(names[0])
            except Exception as e:
                raise XlightsSequenceImportError(
                    f"Failed to read {names[0]} from .xsq ZIP: {e}"
                )
            return raw.decode("utf-8", errors="ignore")

    return data.decode("utf-8", errors="ignore")


def _parse_number_list(text: str) -> List[float]:
    s = (text or "").strip()
    if not s:
        return []

    # Allow common separators.
    s = s.replace(";", ",").replace("\n", ",").replace("\r", ",").replace("\t", ",")
    parts = [p.strip() for p in s.split(",")]

    out: List[float] = []
    for p in parts:
        if not p:
            continue
        # Ignore non-numeric tokens (best-effort).
        try:
            out.append(float(p))
        except Exception:
            continue
    return out


def _as_seconds(values: List[float]) -> Tuple[List[float], str]:
    if not values:
        return [], "unknown"
    mx = max(values)
    # Heuristic: xLights stores most timings in milliseconds.
    if mx > 1000.0:
        return [v / 1000.0 for v in values], "ms"
    return list(values), "s"


def _clean_track_name(name: str) -> str:
    n = (name or "").strip()
    if not n:
        return ""
    return " ".join(n.split())


def _extract_candidate_lists(
    elem: ET.Element,
) -> Iterable[Tuple[str, str, List[float]]]:
    attrs = {str(k): str(v) for k, v in (elem.attrib or {}).items()}
    name = _clean_track_name(
        attrs.get("name")
        or attrs.get("Name")
        or attrs.get("track")
        or attrs.get("Track")
        or elem.tag
    )

    # Candidate sources: element text + all attribute values.
    sources: List[Tuple[str, str]] = []
    if elem.text and elem.text.strip():
        sources.append(("text", elem.text))
    for k, v in attrs.items():
        if v and any(ch.isdigit() for ch in v) and ("," in v or "\n" in v or ";" in v):
            sources.append((f"attr:{k}", v))

    for src_kind, src_val in sources:
        vals = _parse_number_list(src_val)
        if len(vals) >= 4:
            yield name, src_kind, vals


def parse_xlights_xsq_timing_tracks(xml_text: str) -> List[XlightsTimingTrack]:
    """
    Best-effort timing track extraction from xLights `.xsq`.

    This intentionally avoids importing xLights effect data; it only tries to recover a beat/timing grid.
    """
    try:
        root = ET.fromstring(xml_text)
    except Exception as e:
        raise XlightsSequenceImportError(f"Failed to parse .xsq XML: {e}")

    found: Dict[str, XlightsTimingTrack] = {}

    for elem in root.iter():
        for track_name, src_kind, vals in _extract_candidate_lists(elem):
            marks, unit = _as_seconds(vals)
            # Deduplicate + sort; require monotonicity (after sort).
            uniq = sorted({float(x) for x in marks if x >= 0.0})
            if len(uniq) < 4:
                continue
            if uniq[-1] <= uniq[0]:
                continue
            key = (track_name or elem.tag).strip()
            prev = found.get(key)
            if prev is None or len(uniq) > len(prev.marks_s):
                found[key] = XlightsTimingTrack(
                    name=key,
                    marks_s=uniq,
                    raw={"tag": elem.tag, "source": src_kind, "unit": unit},
                )

    return sorted(found.values(), key=lambda t: (-len(t.marks_s), t.name.lower()))


def _estimate_bpm(
    marks_s: List[float], *, min_bpm: int = 20, max_bpm: int = 400
) -> float:
    if len(marks_s) < 3:
        return 0.0
    intervals = [marks_s[i] - marks_s[i - 1] for i in range(1, len(marks_s))]
    bpms: List[float] = []
    for dt in intervals:
        if dt <= 0:
            continue
        bpm = 60.0 / dt
        if float(min_bpm) <= bpm <= float(max_bpm):
            bpms.append(bpm)
    if not bpms:
        return 0.0
    return float(statistics.median(bpms))


def import_xlights_xsq_timing_file(
    *, xsq_path: str, timing_track: Optional[str] = None
) -> Dict[str, Any]:
    """
    Import a timing/beat grid from an xLights `.xsq` file.

    Returns a dict compatible with the `/v1/audio/analyze` output shape (beats_s, bpm, duration_s).
    """
    xml_text = _read_xsq_xml_text(xsq_path)
    tracks = parse_xlights_xsq_timing_tracks(xml_text)
    if not tracks:
        raise XlightsSequenceImportError(
            "No timing tracks found in .xsq (no parseable numeric mark lists)."
        )

    chosen: Optional[XlightsTimingTrack] = None
    if timing_track:
        want = timing_track.strip().lower()
        for t in tracks:
            if t.name.strip().lower() == want:
                chosen = t
                break
        if chosen is None:
            names = ", ".join([t.name for t in tracks[:20]])
            raise XlightsSequenceImportError(
                f"Timing track '{timing_track}' not found. Available: {names}"
            )
    else:
        chosen = tracks[0]

    beats_s = list(chosen.marks_s)
    bpm = _estimate_bpm(beats_s)

    duration_s = 0.0
    if beats_s:
        duration_s = float(beats_s[-1])
        # Extend by one interval so the last segment has a duration.
        if len(beats_s) >= 2:
            duration_s += float(beats_s[-1] - beats_s[-2])

    return {
        "bpm": float(bpm),
        "beats_s": [float(x) for x in beats_s],
        "duration_s": float(duration_s),
        "sample_rate_hz": 0,
        "method": f"xlights_xsq_timing:{chosen.name}",
        "timing_track": chosen.name,
        "tracks_found": [
            {"name": t.name, "marks": len(t.marks_s), "raw": t.raw} for t in tracks[:50]
        ],
    }
