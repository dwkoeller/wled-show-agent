from __future__ import annotations

import asyncio
import multiprocessing as mp
import random
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence

from look_generator import LookLibraryGenerator, look_to_wled_state
from pack_io import nowstamp, read_jsonl_async, write_jsonl
from utils.blocking import run_blocking, run_cpu_blocking
from utils.look_generate import generate_looks_pack, list_look_packs
from wled_client import AsyncWLEDClient
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
        wled: AsyncWLEDClient,
        mapper: WLEDMapper,
        data_dir: str,
        max_bri: int,
        segment_ids: Sequence[int] | None = None,
        replicate_to_all_segments: bool = True,
        blocking: Any | None = None,
        cpu_pool: Any | None = None,
    ) -> None:
        self.wled = wled
        self.mapper = mapper
        self.data_dir = data_dir
        self.max_bri = max(1, min(255, int(max_bri)))
        self.segment_ids = list(segment_ids) if segment_ids else []
        self.replicate_to_all_segments = bool(replicate_to_all_segments)
        self._blocking = blocking
        self._cpu_pool = cpu_pool
        self._cache: Dict[str, List[Dict[str, Any]]] = {}
        self._cache_theme_index: Dict[str, Dict[str, List[int]]] = {}

    def _looks_dir(self) -> Path:
        d = Path(self.data_dir) / "looks"
        d.mkdir(parents=True, exist_ok=True)
        return d

    async def list_packs(self) -> List[str]:
        return await run_cpu_blocking(
            self._cpu_pool, list_look_packs, data_dir=self.data_dir
        )

    async def latest_pack(self) -> Optional[str]:
        files = await self.list_packs()
        return files[-1] if files else None

    def _pack_path(self, file: str) -> str:
        return str(self._looks_dir() / file)

    async def generate_pack(
        self,
        *,
        total_looks: int,
        themes: Sequence[str],
        brightness: int,
        seed: int,
        write_files: bool = True,
        include_multi_segment: bool = True,
        progress_cb: Callable[[int, int, str], None] | None = None,
        cancel_cb: Callable[[], bool] | None = None,
    ) -> PackSummary:
        try:
            effects = await self.wled.get_effects(refresh=True)
            palettes = await self.wled.get_palettes(refresh=True)
            segments = await self.wled.get_segments(refresh=True)
        except Exception as e:
            raise RuntimeError(f"Failed to query WLED effect/palette lists: {e}") from e
        segments_list = list(segments or [])

        use_cpu_pool = self._cpu_pool is not None
        handled = False
        if use_cpu_pool and (progress_cb is not None or cancel_cb is not None):
            manager = None
            try:
                ctx = mp.get_context()
                manager = ctx.Manager()
            except Exception:
                manager = None
            if manager is not None:
                progress_q = manager.Queue()
                cancel_event = manager.Event()

                async def _drain_progress() -> None:
                    loop = asyncio.get_running_loop()
                    while True:
                        item = await loop.run_in_executor(None, progress_q.get)
                        if item is None:
                            break
                        try:
                            cur, total, msg = item
                        except Exception:
                            continue
                        if progress_cb is not None:
                            try:
                                progress_cb(int(cur), int(total), str(msg))
                            except Exception:
                                pass
                        if cancel_cb is not None:
                            try:
                                if cancel_cb():
                                    cancel_event.set()
                            except Exception:
                                pass

                async def _watch_cancel() -> None:
                    if cancel_cb is None:
                        return
                    while True:
                        await asyncio.sleep(0.2)
                        try:
                            if cancel_cb():
                                cancel_event.set()
                                return
                        except Exception:
                            return

                progress_task = asyncio.create_task(_drain_progress())
                cancel_task = (
                    asyncio.create_task(_watch_cancel()) if cancel_cb is not None else None
                )

                try:
                    fname, rows, theme_counts = await run_cpu_blocking(
                        self._cpu_pool,
                        generate_looks_pack,
                        data_dir=self.data_dir,
                        total_looks=total_looks,
                        themes=list(themes),
                        brightness=brightness,
                        seed=seed,
                        effects=list(effects),
                        palettes=list(palettes),
                        segments=segments_list,
                        include_multi_segment=include_multi_segment,
                        segment_ids=self.segment_ids or [0],
                        max_bri=self.max_bri,
                        write_files=write_files,
                        progress_queue=progress_q,
                        cancel_event=cancel_event,
                    )
                    handled = True
                finally:
                    try:
                        progress_q.put(None)
                    except Exception:
                        pass
                    if cancel_task is not None:
                        cancel_task.cancel()
                    try:
                        await asyncio.wait_for(progress_task, timeout=1.0)
                    except Exception:
                        progress_task.cancel()
                    try:
                        manager.shutdown()
                    except Exception:
                        pass
            else:
                use_cpu_pool = False
        if not handled and use_cpu_pool:
            fname, rows, theme_counts = await run_cpu_blocking(
                self._cpu_pool,
                generate_looks_pack,
                data_dir=self.data_dir,
                total_looks=total_looks,
                themes=list(themes),
                brightness=brightness,
                seed=seed,
                effects=list(effects),
                palettes=list(palettes),
                segments=segments_list,
                include_multi_segment=include_multi_segment,
                segment_ids=self.segment_ids or [0],
                max_bri=self.max_bri,
                write_files=write_files,
            )
            handled = True
        if not handled:
            def _run() -> tuple[str, List[Dict[str, Any]], Dict[str, int]]:
                gen = LookLibraryGenerator(
                    mapper=self.mapper,
                    seed=seed,
                    effects=list(effects),
                    palettes=list(palettes),
                    segments=segments_list,
                )
                looks = gen.generate(
                    total=total_looks,
                    themes=themes,
                    brightness=min(self.max_bri, brightness),
                    include_multi_segment=include_multi_segment,
                    segment_ids=self.segment_ids or [0],
                    progress_cb=progress_cb,
                    cancel_cb=cancel_cb,
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
                return fname, rows, theme_counts

            fname, rows, theme_counts = await run_blocking(self._blocking, _run)

        # Cache (event-loop thread only).
        self._cache[fname] = rows
        self._cache_theme_index[fname] = self._build_theme_index(rows)

        return PackSummary(file=fname, total=len(rows), themes=theme_counts)

    def _build_theme_index(self, rows: List[Dict[str, Any]]) -> Dict[str, List[int]]:
        idx: Dict[str, List[int]] = {}
        for i, row in enumerate(rows):
            theme = str(row.get("theme", "misc"))
            idx.setdefault(theme, []).append(i)
        return idx

    async def load_pack(self, file: str) -> List[Dict[str, Any]]:
        if file in self._cache:
            return self._cache[file]
        path = self._pack_path(file)
        rows = await read_jsonl_async(path)
        self._cache[file] = rows
        self._cache_theme_index[file] = self._build_theme_index(rows)
        return rows

    async def apply_look(
        self,
        row: Dict[str, Any],
        *,
        brightness_override: Optional[int] = None,
        transition_ms: Optional[int] = None,
    ) -> Dict[str, Any]:
        state = look_to_wled_state(
            row,
            self.mapper,
            brightness_override=brightness_override,
            segment_ids=self.segment_ids,
            replicate_to_all_segments=self.replicate_to_all_segments,
        )
        if transition_ms is not None:
            tt = max(0, int(round(float(transition_ms) / 100.0)))
            state["tt"] = tt
            state["transition"] = tt
        # safety cap
        if "bri" in state:
            state["bri"] = min(self.max_bri, max(1, int(state["bri"])))
        await self.wled.apply_state(state, verbose=False)
        return {
            "applied": True,
            "name": row.get("name"),
            "id": row.get("id"),
            "theme": row.get("theme"),
        }

    async def choose_random(
        self,
        *,
        theme: Optional[str] = None,
        pack_file: Optional[str] = None,
        seed: Optional[int] = None,
    ) -> tuple[str, Dict[str, Any]]:
        pack = pack_file or await self.latest_pack()
        if not pack:
            raise RuntimeError(
                "No looks pack found. Generate one first via /v1/looks/generate or /v1/go_crazy."
            )
        rows = await self.load_pack(pack)
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

    async def apply_random(
        self,
        *,
        theme: Optional[str] = None,
        pack_file: Optional[str] = None,
        brightness: Optional[int] = None,
        seed: Optional[int] = None,
        transition_ms: Optional[int] = None,
    ) -> Dict[str, Any]:
        _, row = await self.choose_random(theme=theme, pack_file=pack_file, seed=seed)
        return await self.apply_look(
            row, brightness_override=brightness, transition_ms=transition_ms
        )
