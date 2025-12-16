from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from look_generator import look_to_wled_state
from pack_io import read_jsonl
from rate_limiter import Cooldown
from wled_client import WLEDClient
from wled_mapper import WLEDMapper


@dataclass
class ImportResult:
    imported: int
    start_id: int
    stop_id: int
    errors: List[str]


class PresetImporter:
    def __init__(
        self,
        *,
        wled: WLEDClient,
        mapper: WLEDMapper,
        cooldown: Cooldown,
        max_bri: int,
        segment_ids: list[int] | None = None,
        replicate_to_all_segments: bool = True,
    ) -> None:
        self.wled = wled
        self.mapper = mapper
        self.cooldown = cooldown
        self.max_bri = max(1, min(255, int(max_bri)))
        self.segment_ids = list(segment_ids) if segment_ids else []
        self.replicate_to_all_segments = bool(replicate_to_all_segments)

    def import_from_pack(
        self,
        *,
        pack_path: str,
        start_id: int,
        limit: int,
        name_prefix: str = "AI",
        include_brightness: bool = True,
        save_bounds: bool = True,
    ) -> ImportResult:
        rows = read_jsonl(pack_path, limit=limit)
        errors: List[str] = []
        imported = 0
        cur_id = int(start_id)

        for row in rows:
            try:
                state = look_to_wled_state(
                    row,
                    self.mapper,
                    segment_ids=self.segment_ids,
                    replicate_to_all_segments=self.replicate_to_all_segments,
                )
                # brightness safety
                if not include_brightness:
                    state.pop("bri", None)
                else:
                    bri = int(state.get("bri", self.max_bri))
                    state["bri"] = min(self.max_bri, max(1, bri))

                name = row.get("name") or row.get("spec", {}).get("name") or f"Look {imported+1}"
                preset_name = f"{name_prefix} {name}".strip()[:48]  # keep short
                # One-call save: include state + psave
                payload = dict(state)
                payload.update({
                    "psave": cur_id,
                    "n": preset_name,
                    "ib": bool(include_brightness),
                    "sb": bool(save_bounds),
                })
                self.cooldown.wait()
                self.wled.apply_state(payload, verbose=False)
                imported += 1
                cur_id += 1
                if cur_id > 250:
                    break
            except Exception as e:
                errors.append(f"Row {imported+1}: {e}")

        stop_id = cur_id - 1
        return ImportResult(imported=imported, start_id=int(start_id), stop_id=stop_id, errors=errors)
