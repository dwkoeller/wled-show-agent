from __future__ import annotations

import os
import random
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from look_generator import LookLibraryGenerator, look_to_wled_state
from pack_io import nowstamp, read_jsonl, write_jsonl
from wled_client import WLEDClient
from wled_mapper import WLEDMapper


@dataclass
class PackSummary:
    file: str
    total: int
    themes: Dict[str, int]


class LookService:
    def __init__(
        self,
        *,
        wled: WLEDClient,
        mapper: WLEDMapper,
        data_dir: str,
        max_bri: int,
        segment_ids: Sequence[int] | None = None,
        replicate_to_all_segments: bool = True,
    ) -> None:
        self.wled = wled
        self.mapper = mapper
        self.data_dir = data_dir
        self.max_bri = max(1, min(255, int(max_bri)))
        self.segment_ids = list(segment_ids) if segment_ids else []
        self.replicate_to_all_segments = bool(replicate_to_all_segments)
        self._cache: Dict[str, List[Dict[str, Any]]] = {}
        self._cache_theme_index: Dict[str, Dict[str, List[int]]] = {}

    def _looks_dir(self) -> Path:
        d = Path(self.data_dir) / "looks"
        d.mkdir(parents=True, exist_ok=True)
        return d

    def list_packs(self) -> List[str]:
        d = self._looks_dir()
        files = sorted([p.name for p in d.glob("looks_pack_*.jsonl")])
        return files

    def latest_pack(self) -> Optional[str]:
        files = self.list_packs()
        return files[-1] if files else None

    def _pack_path(self, file: str) -> str:
        return str(self._looks_dir() / file)

    def generate_pack(
        self,
        *,
        total_looks: int,
        themes: Sequence[str],
        brightness: int,
        seed: int,
        write_files: bool = True,
        include_multi_segment: bool = True,
    ) -> PackSummary:
        gen = LookLibraryGenerator(mapper=self.mapper, seed=seed)
        looks = gen.generate(
            total=total_looks,
            themes=themes,
            brightness=min(self.max_bri, brightness),
            include_multi_segment=include_multi_segment,
            segment_ids=self.segment_ids or [0],
        )

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
            write_jsonl(self._pack_path(fname), rows)

        # cache (in-memory)
        self._cache[fname] = rows
        self._cache_theme_index[fname] = self._build_theme_index(rows)

        return PackSummary(file=fname, total=len(rows), themes=theme_counts)

    def _build_theme_index(self, rows: List[Dict[str, Any]]) -> Dict[str, List[int]]:
        idx: Dict[str, List[int]] = {}
        for i, row in enumerate(rows):
            theme = str(row.get("theme", "misc"))
            idx.setdefault(theme, []).append(i)
        return idx

    def load_pack(self, file: str) -> List[Dict[str, Any]]:
        if file in self._cache:
            return self._cache[file]
        path = self._pack_path(file)
        rows = read_jsonl(path)
        self._cache[file] = rows
        self._cache_theme_index[file] = self._build_theme_index(rows)
        return rows

    def apply_look(self, row: Dict[str, Any], *, brightness_override: Optional[int] = None) -> Dict[str, Any]:
        state = look_to_wled_state(
            row,
            self.mapper,
            brightness_override=brightness_override,
            segment_ids=self.segment_ids,
            replicate_to_all_segments=self.replicate_to_all_segments,
        )
        # safety cap
        if "bri" in state:
            state["bri"] = min(self.max_bri, max(1, int(state["bri"])))
        self.wled.apply_state(state, verbose=False)
        return {"applied": True, "name": row.get("name"), "id": row.get("id"), "theme": row.get("theme")}

    def choose_random(
        self,
        *,
        theme: Optional[str] = None,
        pack_file: Optional[str] = None,
        seed: Optional[int] = None,
    ) -> tuple[str, Dict[str, Any]]:
        pack = pack_file or self.latest_pack()
        if not pack:
            raise RuntimeError("No looks pack found. Generate one first via /v1/looks/generate or /v1/go_crazy.")
        rows = self.load_pack(pack)
        if not rows:
            raise RuntimeError("Looks pack is empty.")

        rng = random.Random(seed) if seed is not None else random.Random()
        if theme:
            idx = self._cache_theme_index.get(pack) or self._build_theme_index(rows)
            candidates = idx.get(theme, [])
            if not candidates:
                # fallback: try case-insensitive match
                theme_l = theme.strip().lower()
                for k, v in idx.items():
                    if k.strip().lower() == theme_l:
                        candidates = v
                        break
            if candidates:
                row = rows[rng.choice(candidates)]
            else:
                row = rng.choice(rows)
        else:
            row = rng.choice(rows)

        return pack, row

    def apply_random(self, *, theme: Optional[str] = None, pack_file: Optional[str] = None, brightness: Optional[int] = None, seed: Optional[int] = None) -> Dict[str, Any]:
        _, row = self.choose_random(theme=theme, pack_file=pack_file, seed=seed)
        return self.apply_look(row, brightness_override=brightness)
