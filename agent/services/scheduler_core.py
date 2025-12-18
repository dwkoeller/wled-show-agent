from __future__ import annotations

import datetime
import re
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from pydantic import BaseModel, Field

from pack_io import read_json, write_json


KV_SCHEDULER_CONFIG_KEY = "scheduler_config"

StopAllCallback = Callable[["SchedulerConfig", str], None]
ApplyRandomLookCallback = Callable[["SchedulerConfig", str], None]
EnsureSequenceCallback = Callable[["SchedulerConfig", str], None]


class SchedulerConfig(BaseModel):
    enabled: bool = True
    autostart: bool = False
    start_hhmm: str = Field(default="17:00")
    end_hhmm: str = Field(default="23:00")
    mode: str = Field(default="looks", pattern="^(looks|sequence)$")
    scope: str = Field(default="fleet", pattern="^(local|fleet)$")
    interval_s: int = Field(default=300, ge=10, le=24 * 60 * 60)
    theme: Optional[str] = None
    brightness: Optional[int] = Field(default=None, ge=1, le=255)
    targets: Optional[List[str]] = None
    include_self: bool = True
    sequence_file: Optional[str] = None
    sequence_loop: bool = True
    stop_all_on_end: bool = True


def hhmm_to_minutes(value: str) -> int:
    s = (value or "").strip()
    m = re.match(r"^(\d{1,2}):(\d{2})$", s)
    if not m:
        raise ValueError("Invalid HH:MM")
    hh = int(m.group(1))
    mm = int(m.group(2))
    if hh < 0 or hh > 23 or mm < 0 or mm > 59:
        raise ValueError("Invalid HH:MM")
    return (hh * 60) + mm


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


class SchedulerService:
    def __init__(
        self,
        config_path: str,
        *,
        kv_store: Any = None,
        kv_key: str = "",
        stop_all_cb: StopAllCallback | None = None,
        apply_random_look_cb: ApplyRandomLookCallback | None = None,
        ensure_sequence_cb: EnsureSequenceCallback | None = None,
    ) -> None:
        self._config_path = str(config_path)
        self._kv_store = kv_store
        self._kv_key = str(kv_key) if kv_key else ""

        self._stop_all_cb = stop_all_cb
        self._apply_random_look_cb = apply_random_look_cb
        self._ensure_sequence_cb = ensure_sequence_cb

        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

        self._running = False
        self._window_active = False
        self._last_action_at: Optional[float] = None
        self._last_action: Optional[str] = None
        self._last_error: Optional[str] = None

        self._config = self._load_config()

    def _load_config(self) -> SchedulerConfig:
        try:
            try:
                if self._kv_store is not None and self._kv_key:
                    raw_db = self._kv_store.get_json(self._kv_key)
                    if raw_db:
                        return SchedulerConfig(**(raw_db or {}))
            except Exception:
                pass
            p = Path(self._config_path)
            if not p.is_file():
                return SchedulerConfig()
            raw = read_json(str(p))
            cfg = SchedulerConfig(**(raw or {}))
            try:
                if self._kv_store is not None and self._kv_key:
                    self._kv_store.set_json(self._kv_key, cfg.model_dump())
            except Exception:
                pass
            return cfg
        except Exception:
            return SchedulerConfig()

    def _save_config(self, cfg: SchedulerConfig) -> None:
        try:
            try:
                if self._kv_store is not None and self._kv_key:
                    self._kv_store.set_json(self._kv_key, cfg.model_dump())
            except Exception:
                pass
            write_json(self._config_path, cfg.model_dump())
        except Exception:
            return

    def get_config(self) -> SchedulerConfig:
        with self._lock:
            return SchedulerConfig(**self._config.model_dump())

    def set_config(self, cfg: SchedulerConfig, *, persist: bool = True) -> None:
        with self._lock:
            self._config = SchedulerConfig(**cfg.model_dump())
        if persist:
            self._save_config(cfg)

    def start(self) -> None:
        with self._lock:
            if self._thread and self._thread.is_alive():
                self._running = True
                return
            self._stop.clear()
            th = threading.Thread(target=self._run, name="scheduler", daemon=True)
            self._thread = th
            self._running = True
            th.start()

    def stop(self) -> None:
        with self._lock:
            th = self._thread
            self._stop.set()
        if th:
            th.join(timeout=2.5)
        with self._lock:
            self._thread = None
            self._running = False
            self._window_active = False

    def status(self) -> Dict[str, Any]:
        with self._lock:
            cfg = self._config.model_dump()
            running = bool(self._running and self._thread and self._thread.is_alive())
            in_window_now = bool(self._window_active)
            last_action_at = self._last_action_at
            last_action = self._last_action
            last_error = self._last_error

        now_ts = time.time()
        next_in_s: Optional[float] = None
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

    def run_once(self) -> None:
        cfg = self.get_config()
        self._execute_action(cfg, reason="run_once")

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                cfg = self.get_config()
                self._tick(cfg)
            except Exception as e:
                with self._lock:
                    self._last_error = str(e)
            self._stop.wait(timeout=1.0)

    def _tick(self, cfg: SchedulerConfig) -> None:
        if not cfg.enabled:
            with self._lock:
                self._window_active = False
            return

        start_min = hhmm_to_minutes(cfg.start_hhmm)
        end_min = hhmm_to_minutes(cfg.end_hhmm)
        now_min = _now_minutes_local()
        active_now = _in_window(now_min, start_min, end_min)

        with self._lock:
            prev_active = self._window_active
            self._window_active = active_now

        if active_now and not prev_active:
            self._execute_action(cfg, reason="enter_window")
            return

        if (not active_now) and prev_active:
            if cfg.stop_all_on_end:
                self._stop_all(cfg, reason="leave_window")
            return

        if not active_now:
            return

        if cfg.mode == "looks":
            interval = max(10, int(cfg.interval_s))
            with self._lock:
                last_at = self._last_action_at
            if last_at is None or (time.time() - last_at) >= float(interval):
                self._execute_action(cfg, reason="interval")
        elif cfg.mode == "sequence":
            self._ensure_sequence(cfg, reason="tick")

    def _stop_all(self, cfg: SchedulerConfig, *, reason: str) -> None:
        cb = self._stop_all_cb
        if cb is None:
            with self._lock:
                self._last_error = "Scheduler stop_all callback not configured"
            return
        try:
            cb(cfg, str(reason))
            with self._lock:
                self._last_error = None
        except Exception as e:
            with self._lock:
                self._last_error = str(e)

    def _execute_action(self, cfg: SchedulerConfig, *, reason: str) -> None:
        if cfg.mode == "looks":
            cb = self._apply_random_look_cb
            if cb is None:
                raise RuntimeError(
                    "Scheduler apply_random_look callback not configured"
                )
            cb(cfg, str(reason))
            with self._lock:
                self._last_action_at = time.time()
                self._last_action = f"apply_random_look({cfg.scope})"
                self._last_error = None
            return

        if cfg.mode == "sequence":
            self._ensure_sequence(cfg, reason=str(reason))
            return

    def _ensure_sequence(self, cfg: SchedulerConfig, *, reason: str) -> None:
        cb = self._ensure_sequence_cb
        if cb is None:
            raise RuntimeError("Scheduler ensure_sequence callback not configured")
        cb(cfg, str(reason))
        with self._lock:
            self._last_action_at = time.time()
            self._last_action = f"ensure_sequence({cfg.scope})"
            self._last_error = None
