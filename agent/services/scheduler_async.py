from __future__ import annotations

import asyncio
import datetime
import time
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import HTTPException

from models.requests import FleetApplyRandomLookRequest, FleetStopAllRequest
from pack_io import read_json, write_json
from services import a2a_service, fleet_service
from services.runtime_state_service import persist_runtime_state
from services.scheduler_core import (
    KV_SCHEDULER_CONFIG_KEY,
    SchedulerConfig,
    hhmm_to_minutes,
)
from services.state import AppState


def _now_minutes_local() -> int:
    now = datetime.datetime.now()
    return (now.hour * 60) + now.minute


def _in_window(now_min: int, start_min: int, end_min: int) -> bool:
    if start_min == end_min:
        return True  # always-on window
    if start_min < end_min:
        return start_min <= now_min < end_min
    # crosses midnight
    return now_min >= start_min or now_min < end_min


class AsyncSchedulerService:
    def __init__(
        self,
        *,
        state: AppState,
        config_path: str,
        kv_key: str = KV_SCHEDULER_CONFIG_KEY,
    ) -> None:
        self._state = state
        self._config_path = str(config_path)
        self._kv_key = str(kv_key) if kv_key else ""

        self._lock = asyncio.Lock()
        self._stop = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

        self._running = False
        self._window_active = False
        self._last_action_at: float | None = None
        self._last_action: str | None = None
        self._last_error: str | None = None

        self._config = SchedulerConfig()

    async def init(self) -> None:
        cfg = await self._load_config()
        async with self._lock:
            self._config = SchedulerConfig(**cfg.model_dump())

    async def close(self) -> None:
        await self.stop()

    async def get_config(self) -> SchedulerConfig:
        async with self._lock:
            return SchedulerConfig(**self._config.model_dump())

    async def set_config(self, cfg: SchedulerConfig, *, persist: bool = True) -> None:
        async with self._lock:
            self._config = SchedulerConfig(**cfg.model_dump())
        if persist:
            await self._save_config(cfg)

    async def start(self) -> None:
        async with self._lock:
            if self._task and not self._task.done():
                self._running = True
                return
            self._stop.clear()
            self._running = True
            self._task = asyncio.create_task(self._run(), name="scheduler")

    async def stop(self) -> None:
        async with self._lock:
            task = self._task
            self._stop.set()
        if task is not None:
            try:
                task.cancel()
            except Exception:
                pass
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
        async with self._lock:
            self._task = None
            self._running = False
            self._window_active = False

    async def status(self) -> Dict[str, Any]:
        async with self._lock:
            cfg = self._config.model_dump()
            running = bool(self._running and self._task and not self._task.done())
            in_window_now = bool(self._window_active)
            last_action_at = self._last_action_at
            last_action = self._last_action
            last_error = self._last_error

        now_ts = time.time()
        next_in_s: float | None = None
        try:
            start_min = hhmm_to_minutes(str(cfg.get("start_hhmm")))
            end_min = hhmm_to_minutes(str(cfg.get("end_hhmm")))
            now_min = _now_minutes_local()
            active = _in_window(now_min, start_min, end_min)
            if cfg.get("mode") == "looks" and active and last_action_at is not None:
                interval = float(cfg.get("interval_s") or 0)
                if interval > 0:
                    next_in_s = max(0.0, (last_action_at + interval) - now_ts)
        except Exception:
            pass

        return {
            "ok": True,
            "running": running,
            "in_window": in_window_now,
            "last_action_at": last_action_at,
            "last_action": last_action,
            "last_error": last_error,
            "next_action_in_s": next_in_s,
            "config": cfg,
        }

    async def run_once(self) -> None:
        cfg = await self.get_config()
        await self._execute_action(cfg, reason="run_once")

    async def _run(self) -> None:
        while not self._stop.is_set():
            try:
                cfg = await self.get_config()
                await self._tick(cfg)
            except Exception as e:
                async with self._lock:
                    self._last_error = str(e)
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=1.0)
            except asyncio.TimeoutError:
                pass

    async def _tick(self, cfg: SchedulerConfig) -> None:
        if not cfg.enabled:
            async with self._lock:
                self._window_active = False
            return

        start_min = hhmm_to_minutes(cfg.start_hhmm)
        end_min = hhmm_to_minutes(cfg.end_hhmm)
        now_min = _now_minutes_local()
        active_now = _in_window(now_min, start_min, end_min)

        async with self._lock:
            prev_active = self._window_active
            self._window_active = active_now

        if active_now and not prev_active:
            await self._execute_action(cfg, reason="enter_window")
            return

        if (not active_now) and prev_active:
            if cfg.stop_all_on_end:
                await self._stop_all(cfg, reason="leave_window")
            return

        if not active_now:
            return

        if cfg.mode == "looks":
            interval = max(10, int(cfg.interval_s))
            async with self._lock:
                last_at = self._last_action_at
            if last_at is None or (time.time() - last_at) >= float(interval):
                await self._execute_action(cfg, reason="interval")
        elif cfg.mode == "sequence":
            await self._ensure_sequence(cfg, reason="tick")

    async def _stop_all(self, cfg: SchedulerConfig, *, reason: str) -> None:
        try:
            if cfg.scope == "fleet":
                await fleet_service.fleet_stop_all(
                    FleetStopAllRequest(
                        targets=cfg.targets,
                        include_self=cfg.include_self,
                        timeout_s=float(self._state.settings.a2a_http_timeout_s),
                    ),
                    state=self._state,
                )
            else:
                await a2a_service.actions()["stop_all"](self._state, {})

            await persist_runtime_state(
                self._state, "scheduler_stop_all", {"reason": str(reason)}
            )

            async with self._lock:
                self._last_error = None
        except Exception as e:
            async with self._lock:
                self._last_error = str(e)

    async def _execute_action(self, cfg: SchedulerConfig, *, reason: str) -> None:
        if cfg.mode == "looks":
            await self._apply_random_look(cfg, reason=str(reason))
            async with self._lock:
                self._last_action_at = time.time()
                self._last_action = f"apply_random_look({cfg.scope})"
            return

        if cfg.mode == "sequence":
            await self._ensure_sequence(cfg, reason=str(reason))
            return

    async def _apply_random_look(self, cfg: SchedulerConfig, *, reason: str) -> None:
        settings = self._state.settings
        bri = (
            min(settings.wled_max_bri, cfg.brightness)
            if cfg.brightness is not None
            else None
        )
        try:
            if cfg.scope == "fleet":
                res = await fleet_service.fleet_apply_random_look(
                    FleetApplyRandomLookRequest(
                        theme=cfg.theme,
                        brightness=bri,
                        targets=cfg.targets,
                        include_self=cfg.include_self,
                    ),
                    state=self._state,
                )
                await persist_runtime_state(
                    self._state,
                    "scheduler_apply_random_look",
                    {"scope": "fleet", "reason": reason},
                )
                if self._state.db is not None:
                    try:
                        picked = ((res or {}).get("result") or {}).get("picked") or {}
                        pack_file = ((res or {}).get("result") or {}).get("pack_file")
                        await self._state.db.set_last_applied(
                            kind="look",
                            name=str(picked.get("name") or "") or None,
                            file=str(pack_file) if pack_file else None,
                            payload={
                                "picked": dict(picked or {}),
                                "pack_file": str(pack_file) if pack_file else None,
                                "brightness_override": bri,
                                "scope": "fleet",
                                "reason": reason,
                            },
                        )
                    except Exception:
                        pass
                async with self._lock:
                    self._last_error = None
                return

            looks = getattr(self._state, "looks", None)
            if looks is None:
                raise RuntimeError("Look service not initialized")
            pack_file, row = await asyncio.to_thread(
                looks.choose_random, theme=cfg.theme
            )
            if self._state.wled_cooldown is not None:
                await self._state.wled_cooldown.wait()
            await asyncio.to_thread(looks.apply_look, row, brightness_override=bri)
            await persist_runtime_state(
                self._state,
                "scheduler_apply_random_look",
                {"scope": "local", "reason": reason},
            )
            if self._state.db is not None:
                try:
                    await self._state.db.set_last_applied(
                        kind="look",
                        name=str(row.get("name") or "") or None,
                        file=str(pack_file) if pack_file else None,
                        payload={
                            "look": dict(row or {}),
                            "pack_file": str(pack_file) if pack_file else None,
                            "brightness_override": bri,
                            "scope": "local",
                            "reason": reason,
                        },
                    )
                except Exception:
                    pass

            async with self._lock:
                self._last_error = None
        except Exception as e:
            async with self._lock:
                self._last_error = str(e)

    async def _ensure_sequence(self, cfg: SchedulerConfig, *, reason: str) -> None:
        file = (cfg.sequence_file or "").strip()
        if not file:
            async with self._lock:
                self._last_error = "sequence_file is required for mode=sequence"
            return

        try:
            if cfg.scope == "fleet":
                fleet = getattr(self._state, "fleet_sequences", None)
                if fleet is None:
                    raise RuntimeError("Fleet sequence service not initialized")
                st = await asyncio.to_thread(fleet.status)
                if (
                    (not st.running)
                    or (st.file != file)
                    or (bool(st.loop) != bool(cfg.sequence_loop))
                ):
                    await asyncio.to_thread(
                        fleet.start,
                        file=file,
                        loop=bool(cfg.sequence_loop),
                        targets=cfg.targets,
                        include_self=cfg.include_self,
                    )
                    await persist_runtime_state(
                        self._state,
                        "scheduler_start_fleet_sequence",
                        {"file": file, "reason": reason},
                    )
            else:
                seq = getattr(self._state, "sequences", None)
                if seq is None:
                    raise RuntimeError("Sequence service not initialized")
                st = await asyncio.to_thread(seq.status)
                if (
                    (not st.running)
                    or (st.file != file)
                    or (bool(st.loop) != bool(cfg.sequence_loop))
                ):
                    await asyncio.to_thread(
                        seq.play, file=file, loop=bool(cfg.sequence_loop)
                    )
                    await persist_runtime_state(
                        self._state,
                        "scheduler_play_sequence",
                        {"file": file, "reason": reason},
                    )

            if self._state.db is not None:
                try:
                    await self._state.db.set_last_applied(
                        kind="sequence",
                        name=None,
                        file=file,
                        payload={
                            "file": file,
                            "loop": bool(cfg.sequence_loop),
                            "scope": str(cfg.scope),
                            "targets": list(cfg.targets or []),
                            "include_self": bool(cfg.include_self),
                            "reason": reason,
                        },
                    )
                except Exception:
                    pass

            async with self._lock:
                self._last_action_at = time.time()
                self._last_action = f"ensure_sequence({cfg.scope})"
                self._last_error = None
        except HTTPException:
            raise
        except Exception as e:
            async with self._lock:
                self._last_error = str(e)

    async def _load_config(self) -> SchedulerConfig:
        # DB KV first (if configured).
        db = getattr(self._state, "db", None)
        if db is not None and self._kv_key:
            try:
                raw_db = await db.kv_get_json(self._kv_key)
                if raw_db:
                    return SchedulerConfig(**(raw_db or {}))
            except Exception:
                pass

        # File fallback.
        try:
            p = Path(self._config_path)
            if not p.is_file():
                return SchedulerConfig()
            raw = await asyncio.to_thread(read_json, str(p))
            cfg = SchedulerConfig(**(raw or {}))
            # Best-effort persist to DB KV.
            if db is not None and self._kv_key:
                try:
                    await db.kv_set_json(self._kv_key, cfg.model_dump())
                except Exception:
                    pass
            return cfg
        except Exception:
            return SchedulerConfig()

    async def _save_config(self, cfg: SchedulerConfig) -> None:
        db = getattr(self._state, "db", None)
        try:
            if db is not None and self._kv_key:
                try:
                    await db.kv_set_json(self._kv_key, cfg.model_dump())
                except Exception:
                    pass
            await asyncio.to_thread(write_json, self._config_path, cfg.model_dump())
        except Exception:
            return
