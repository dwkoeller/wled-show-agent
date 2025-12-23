from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Sequence, Tuple

from look_generator import LookLibraryGenerator
from pack_io import nowstamp, write_jsonl
from wled_mapper import WLEDMapper


def list_look_packs(*, data_dir: str) -> List[str]:
    looks_dir = Path(data_dir) / "looks"
    looks_dir.mkdir(parents=True, exist_ok=True)
    return sorted([p.name for p in looks_dir.glob("looks_pack_*.jsonl")])


def generate_looks_pack(
    *,
    data_dir: str,
    total_looks: int,
    themes: Sequence[str],
    brightness: int,
    seed: int,
    effects: Sequence[str],
    palettes: Sequence[str],
    segments: Sequence[Dict[str, Any]],
    include_multi_segment: bool,
    segment_ids: Sequence[int],
    max_bri: int,
    write_files: bool,
    progress_queue: Any | None = None,
    cancel_event: Any | None = None,
) -> Tuple[str, List[Dict[str, Any]], Dict[str, int]]:
    mapper = WLEDMapper()
    mapper.seed(effects=[str(x) for x in effects], palettes=[str(x) for x in palettes])

    gen = LookLibraryGenerator(
        mapper=mapper,
        seed=seed,
        effects=[str(x) for x in effects],
        palettes=[str(x) for x in palettes],
        segments=list(segments) if segments is not None else None,
    )

    def _progress_cb(cur: int, total: int, msg: str) -> None:
        if progress_queue is None:
            return
        try:
            progress_queue.put((cur, total, str(msg)))
        except Exception:
            return

    def _cancel_cb() -> bool:
        if cancel_event is None:
            return False
        try:
            return bool(cancel_event.is_set())
        except Exception:
            return False

    try:
        looks = gen.generate(
            total=total_looks,
            themes=themes,
            brightness=min(max_bri, brightness),
            include_multi_segment=include_multi_segment,
            segment_ids=list(segment_ids) if segment_ids else [0],
            progress_cb=_progress_cb if progress_queue is not None else None,
            cancel_cb=_cancel_cb if cancel_event is not None else None,
        )
    finally:
        if progress_queue is not None:
            try:
                progress_queue.put(None)
            except Exception:
                pass

    rows: List[Dict[str, Any]] = []
    theme_counts: Dict[str, int] = {}
    for look in looks:
        row = dict(look.spec)
        row["id"] = look.id
        row["name"] = look.name
        row["theme"] = look.theme
        row["tags"] = look.tags
        rows.append(row)
        theme_counts[look.theme] = theme_counts.get(look.theme, 0) + 1

    fname = f"looks_pack_{nowstamp()}.jsonl"
    if write_files:
        looks_dir = Path(data_dir) / "looks"
        looks_dir.mkdir(parents=True, exist_ok=True)
        write_jsonl(str(looks_dir / fname), rows)
    return fname, rows, theme_counts
