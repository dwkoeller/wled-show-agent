from __future__ import annotations

import asyncio
import csv
import io
import json
import time
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from fastapi import Depends, HTTPException, Request
from fastapi.responses import PlainTextResponse, Response

from models.requests import (
    OrchestrationBlackoutRequest,
    OrchestrationCrossfadeRequest,
    OrchestrationPresetsImportRequest,
    OrchestrationPresetUpsertRequest,
    OrchestrationStartRequest,
    OrchestrationStep,
)
from services.audit_logger import log_event
from services.auth_service import require_a2a_auth, require_admin
from services.runtime_state_service import persist_runtime_state
from services.state import AppState, get_state


@dataclass
class OrchestrationStatus:
    running: bool
    run_id: str | None
    name: str | None
    started_at: float | None
    step_index: int
    steps_total: int
    loop: bool


def _transition_ds(transition_ms: Optional[int]) -> Optional[int]:
    if transition_ms is None:
        return None
    try:
        ms = float(transition_ms)
    except Exception:
        return None
    return max(0, int(round(ms / 100.0)))


class OrchestrationService:
    def __init__(self, *, state: AppState) -> None:
        self._state = state
        self._lock = asyncio.Lock()
        self._task: asyncio.Task[None] | None = None
        self._stop = asyncio.Event()
        self._status = OrchestrationStatus(
            running=False,
            run_id=None,
            name=None,
            started_at=None,
            step_index=0,
            steps_total=0,
            loop=False,
        )

    async def status(self) -> OrchestrationStatus:
        async with self._lock:
            return OrchestrationStatus(**self._status.__dict__)

    async def stop(self) -> OrchestrationStatus:
        async with self._lock:
            if not self._status.running:
                return OrchestrationStatus(**self._status.__dict__)
            self._stop.set()
            task = self._task

        if task is not None:
            try:
                task.cancel()
            except Exception:
                pass
            try:
                await task
            except Exception:
                pass

        async with self._lock:
            self._task = None
            self._status.running = False
            self._status.run_id = None
            self._status.name = None
            self._status.started_at = None
            self._status.step_index = 0
            self._status.steps_total = 0
            self._status.loop = False
            return OrchestrationStatus(**self._status.__dict__)

    async def start(
        self, *, name: str | None, steps: List[OrchestrationStep], loop: bool
    ) -> OrchestrationStatus:
        await self.stop()
        if not steps:
            raise RuntimeError("steps is required")

        run_id = uuid.uuid4().hex

        seq = getattr(self._state, "sequences", None)
        ddp = getattr(self._state, "ddp", None)
        if seq is not None:
            try:
                await seq.stop()
            except Exception:
                pass
        if ddp is not None:
            try:
                await ddp.stop()
            except Exception:
                pass

        self._stop.clear()

        if self._state.db is not None:
            try:
                await self._state.db.add_orchestration_run(
                    run_id=run_id,
                    scope="local",
                    name=str(name or "") or None,
                    steps_total=len(steps),
                    loop=bool(loop),
                    include_self=True,
                    payload={"steps": [s.model_dump() for s in steps]},
                )
            except Exception:
                pass

        async def _sleep_interruptible(seconds: float) -> None:
            dur_s = max(0.05, float(seconds))
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=dur_s)
            except asyncio.TimeoutError:
                return

        async def _apply_look(step: OrchestrationStep) -> None:
            looks = getattr(self._state, "looks", None)
            if looks is None:
                raise RuntimeError("Look service not initialized")
            if not isinstance(step.look, dict):
                raise ValueError("look must be an object")
            bri = step.brightness
            if bri is not None:
                bri = min(self._state.settings.wled_max_bri, max(1, int(bri)))
            if self._state.wled_cooldown is not None:
                await self._state.wled_cooldown.wait()
            await looks.apply_look(
                dict(step.look),
                brightness_override=bri,
                transition_ms=step.transition_ms,
            )
            if self._state.db is not None:
                try:
                    await self._state.db.set_last_applied(
                        kind="look",
                        name=str(step.look.get("name") or ""),
                        file=str(step.look.get("file") or ""),
                        payload={"look": dict(step.look)},
                    )
                    try:
                        from services.events_service import emit_event

                        await emit_event(
                            self._state,
                            event_type="meta",
                            data={
                                "event": "last_applied",
                                "kind": "look",
                                "name": str(step.look.get("name") or "") or None,
                                "file": str(step.look.get("file") or "") or None,
                            },
                        )
                    except Exception:
                        pass
                except Exception:
                    pass

        async def _apply_state(step: OrchestrationStep) -> None:
            if not isinstance(step.state, dict):
                raise ValueError("state must be an object")
            payload = dict(step.state)
            if "bri" in payload:
                payload["bri"] = min(
                    self._state.settings.wled_max_bri,
                    max(1, int(payload["bri"])),
                )
            if step.transition_ms is not None:
                tt = _transition_ds(step.transition_ms)
                if tt is not None:
                    payload["tt"] = tt
                    payload["transition"] = tt
            if self._state.wled_cooldown is not None:
                await self._state.wled_cooldown.wait()
            await self._state.wled.apply_state(payload, verbose=False)

        async def _apply_preset(step: OrchestrationStep) -> None:
            if step.preset_id is None:
                raise ValueError("preset_id is required for preset steps")
            payload = {"ps": int(step.preset_id)}
            if step.transition_ms is not None:
                tt = _transition_ds(step.transition_ms)
                if tt is not None:
                    payload["tt"] = tt
                    payload["transition"] = tt
            if step.brightness is not None:
                payload["bri"] = min(
                    self._state.settings.wled_max_bri, max(1, int(step.brightness))
                )
            if self._state.wled_cooldown is not None:
                await self._state.wled_cooldown.wait()
            await self._state.wled.apply_state(payload, verbose=False)

        async def _apply_blackout(step: OrchestrationStep) -> None:
            payload: Dict[str, Any] = {"on": False}
            if step.transition_ms is not None:
                tt = _transition_ds(step.transition_ms)
                if tt is not None:
                    payload["tt"] = tt
                    payload["transition"] = tt
            if self._state.wled_cooldown is not None:
                await self._state.wled_cooldown.wait()
            await self._state.wled.apply_state(payload, verbose=False)

        async def _apply_sequence(step: OrchestrationStep) -> None:
            seq = getattr(self._state, "sequences", None)
            if seq is None:
                raise RuntimeError("Sequence service not initialized")
            if not step.sequence_file:
                raise ValueError("sequence_file is required")
            if step.loop and step.duration_s is None:
                raise ValueError("duration_s is required when loop=true")
            await seq.play(file=str(step.sequence_file), loop=bool(step.loop))
            if self._state.db is not None:
                try:
                    await self._state.db.set_last_applied(
                        kind="sequence",
                        name=str(step.sequence_file),
                        file=str(step.sequence_file),
                        payload={"file": str(step.sequence_file), "loop": bool(step.loop)},
                    )
                    try:
                        from services.events_service import emit_event

                        await emit_event(
                            self._state,
                            event_type="meta",
                            data={
                                "event": "last_applied",
                                "kind": "sequence",
                                "name": str(step.sequence_file),
                                "file": str(step.sequence_file),
                                "loop": bool(step.loop),
                            },
                        )
                    except Exception:
                        pass
                except Exception:
                    pass
            if step.duration_s is None:
                while True:
                    if self._stop.is_set():
                        break
                    cur = await seq.status()
                    if not cur.running:
                        break
                    await _sleep_interruptible(0.5)
            else:
                await _sleep_interruptible(float(step.duration_s))
                try:
                    await seq.stop()
                except Exception:
                    pass

        async def _apply_ddp(step: OrchestrationStep) -> None:
            ddp = getattr(self._state, "ddp", None)
            if ddp is None:
                raise RuntimeError("DDP service not initialized")
            if not step.pattern:
                raise ValueError("pattern is required for ddp steps")
            duration_s = float(step.duration_s or 5.0)
            await ddp.start(
                pattern=str(step.pattern),
                params=dict(step.params or {}),
                duration_s=duration_s,
                brightness=int(step.brightness or 128),
                fps=float(step.fps or self._state.settings.ddp_fps_default),
            )
            await _sleep_interruptible(duration_s)
            try:
                await ddp.stop()
            except Exception:
                pass

        async def _apply_ledfx_scene(step: OrchestrationStep) -> None:
            scene_id = str(step.ledfx_scene_id or "").strip()
            if not scene_id:
                raise ValueError("ledfx_scene_id is required")
            action = str(step.ledfx_scene_action or "activate").strip().lower()
            from ledfx_client import AsyncLedFxClient
            from utils.outbound_http import retry_policy_from_settings
            from services.ledfx_service import _record_last_applied

            if not self._state.settings.ledfx_base_url:
                raise RuntimeError("LedFx is not configured; set LEDFX_BASE_URL.")
            if self._state.peer_http is None:
                raise RuntimeError("HTTP client not initialized")
            client = AsyncLedFxClient(
                base_url=self._state.settings.ledfx_base_url,
                client=self._state.peer_http,
                timeout_s=float(self._state.settings.ledfx_http_timeout_s),
                headers={k: v for (k, v) in self._state.settings.ledfx_headers},
                retry=retry_policy_from_settings(self._state.settings),
            )
            if action in ("deactivate", "stop", "off", "disable"):
                await client.deactivate_scene(scene_id)
                try:
                    await _record_last_applied(
                        self._state,
                        kind="ledfx_scene",
                        name=scene_id,
                        file=None,
                        payload={"action": "deactivate", "scene_id": scene_id},
                    )
                except Exception:
                    pass
            else:
                await client.activate_scene(scene_id)
                try:
                    await _record_last_applied(
                        self._state,
                        kind="ledfx_scene",
                        name=scene_id,
                        file=None,
                        payload={"action": "activate", "scene_id": scene_id},
                    )
                except Exception:
                    pass

        async def _apply_ledfx_effect(step: OrchestrationStep) -> None:
            effect = str(step.ledfx_effect or "").strip()
            if not effect:
                raise ValueError("ledfx_effect is required")
            from ledfx_client import AsyncLedFxClient
            from utils.outbound_http import retry_policy_from_settings
            from services.ledfx_service import _record_last_applied

            if not self._state.settings.ledfx_base_url:
                raise RuntimeError("LedFx is not configured; set LEDFX_BASE_URL.")
            if self._state.peer_http is None:
                raise RuntimeError("HTTP client not initialized")
            client = AsyncLedFxClient(
                base_url=self._state.settings.ledfx_base_url,
                client=self._state.peer_http,
                timeout_s=float(self._state.settings.ledfx_http_timeout_s),
                headers={k: v for (k, v) in self._state.settings.ledfx_headers},
                retry=retry_policy_from_settings(self._state.settings),
            )
            virtual_id = str(step.ledfx_virtual_id or "").strip()
            if not virtual_id:
                resp = await client.virtuals()
                body = resp.body
                raw = None
                if isinstance(body, dict):
                    raw = body.get("virtuals")
                    if raw is None:
                        data = body.get("data")
                        if isinstance(data, dict):
                            raw = data.get("virtuals")
                if raw is None:
                    raw = body
                ids: list[str] = []
                if isinstance(raw, dict):
                    ids = [str(k) for k in raw.keys()]
                elif isinstance(raw, list):
                    for item in raw:
                        if isinstance(item, dict):
                            candidate = item.get("id") or item.get("name")
                            if candidate:
                                ids.append(str(candidate))
                        elif item:
                            ids.append(str(item))
                ids = [x for x in ids if x.strip()]
                if len(ids) == 1:
                    virtual_id = ids[0]
                elif not ids:
                    raise ValueError("No LedFx virtuals found")
                else:
                    raise ValueError("ledfx_virtual_id is required")
            await client.set_virtual_effect(
                virtual_id=virtual_id,
                effect=effect,
                config=dict(step.ledfx_config or {}),
            )
            try:
                await _record_last_applied(
                    self._state,
                    kind="ledfx_effect",
                    name=effect,
                    file=virtual_id,
                    payload={
                        "virtual_id": virtual_id,
                        "effect": effect,
                        "config": dict(step.ledfx_config or {}),
                    },
                )
            except Exception:
                pass

        async def _apply_ledfx_brightness(step: OrchestrationStep) -> None:
            if step.ledfx_brightness is None:
                raise ValueError("ledfx_brightness is required")
            raw = float(step.ledfx_brightness)
            primary = max(0.0, raw)
            fallback: float | None = None
            if primary > 1.0:
                raw_val = min(255.0, primary)
                primary = max(0.0, min(1.0, raw_val / 255.0))
                fallback = raw_val
            from ledfx_client import AsyncLedFxClient
            from utils.outbound_http import retry_policy_from_settings
            from services.ledfx_service import _record_last_applied

            if not self._state.settings.ledfx_base_url:
                raise RuntimeError("LedFx is not configured; set LEDFX_BASE_URL.")
            if self._state.peer_http is None:
                raise RuntimeError("HTTP client not initialized")
            client = AsyncLedFxClient(
                base_url=self._state.settings.ledfx_base_url,
                client=self._state.peer_http,
                timeout_s=float(self._state.settings.ledfx_http_timeout_s),
                headers={k: v for (k, v) in self._state.settings.ledfx_headers},
                retry=retry_policy_from_settings(self._state.settings),
            )
            virtual_id = str(step.ledfx_virtual_id or "").strip()
            if not virtual_id:
                resp = await client.virtuals()
                body = resp.body
                raw_body = None
                if isinstance(body, dict):
                    raw_body = body.get("virtuals")
                    if raw_body is None:
                        data = body.get("data")
                        if isinstance(data, dict):
                            raw_body = data.get("virtuals")
                if raw_body is None:
                    raw_body = body
                ids: list[str] = []
                if isinstance(raw_body, dict):
                    ids = [str(k) for k in raw_body.keys()]
                elif isinstance(raw_body, list):
                    for item in raw_body:
                        if isinstance(item, dict):
                            candidate = item.get("id") or item.get("name")
                            if candidate:
                                ids.append(str(candidate))
                        elif item:
                            ids.append(str(item))
                ids = [x for x in ids if x.strip()]
                if len(ids) == 1:
                    virtual_id = ids[0]
                elif not ids:
                    raise ValueError("No LedFx virtuals found")
                else:
                    raise ValueError("ledfx_virtual_id is required")
            await client.set_virtual_brightness(
                virtual_id=virtual_id,
                brightness=primary,
                fallback_brightness=fallback,
            )
            try:
                await _record_last_applied(
                    self._state,
                    kind="ledfx_brightness",
                    name=str(step.ledfx_brightness),
                    file=virtual_id,
                    payload={
                        "virtual_id": virtual_id,
                        "brightness": float(step.ledfx_brightness),
                    },
                )
            except Exception:
                pass

        async def _record_step_result(
            *,
            step_index: int,
            iteration: int,
            kind: str,
            status: str,
            ok: bool,
            started_at: float,
            finished_at: float,
            error: str | None = None,
            payload: Dict[str, Any] | None = None,
        ) -> None:
            db = self._state.db
            if db is None:
                return
            try:
                await db.add_orchestration_step(
                    run_id=run_id,
                    step_index=int(step_index),
                    iteration=int(iteration),
                    kind=str(kind),
                    status=str(status),
                    ok=bool(ok),
                    started_at=float(started_at),
                    finished_at=float(finished_at),
                    error=error,
                    payload=dict(payload or {}),
                )
            except Exception:
                pass

        async def _run() -> None:
            error: str | None = None
            canceled = False
            iteration = 0
            try:
                while not self._stop.is_set():
                    for i, step in enumerate(steps):
                        async with self._lock:
                            self._status.step_index = int(i)
                            self._status.steps_total = int(len(steps))
                        step_started = time.time()
                        kind = str(step.kind or "look").strip().lower()
                        payload = {"step": step.model_dump(), "iteration": iteration}
                        if self._stop.is_set():
                            await _record_step_result(
                                step_index=i,
                                iteration=iteration,
                                kind=kind,
                                status="skipped",
                                ok=False,
                                started_at=step_started,
                                finished_at=time.time(),
                                error="stopped",
                                payload=payload,
                            )
                            break

                        duration_s = step.duration_s
                        if duration_s is None and kind in (
                            "look",
                            "state",
                            "crossfade",
                            "preset",
                            "blackout",
                            "pause",
                            "ddp",
                            "ledfx_scene",
                            "ledfx_effect",
                            "ledfx_brightness",
                        ):
                            duration_s = 5.0

                        try:
                            if kind == "look":
                                await _apply_look(step)
                                if duration_s is not None:
                                    await _sleep_interruptible(float(duration_s))
                            elif kind == "state":
                                await _apply_state(step)
                                if duration_s is not None:
                                    await _sleep_interruptible(float(duration_s))
                            elif kind == "crossfade":
                                if step.look is not None:
                                    await _apply_look(step)
                                elif step.state is not None:
                                    await _apply_state(step)
                                else:
                                    raise ValueError("crossfade requires look or state")
                                if duration_s is not None:
                                    await _sleep_interruptible(float(duration_s))
                            elif kind == "preset":
                                await _apply_preset(step)
                                if duration_s is not None:
                                    await _sleep_interruptible(float(duration_s))
                            elif kind == "sequence":
                                await _apply_sequence(step)
                            elif kind == "ddp":
                                await _apply_ddp(step)
                            elif kind == "blackout":
                                await _apply_blackout(step)
                                if duration_s is not None:
                                    await _sleep_interruptible(float(duration_s))
                            elif kind == "ledfx_scene":
                                await _apply_ledfx_scene(step)
                                if duration_s is not None:
                                    await _sleep_interruptible(float(duration_s))
                            elif kind == "ledfx_effect":
                                await _apply_ledfx_effect(step)
                                if duration_s is not None:
                                    await _sleep_interruptible(float(duration_s))
                            elif kind == "ledfx_brightness":
                                await _apply_ledfx_brightness(step)
                                if duration_s is not None:
                                    await _sleep_interruptible(float(duration_s))
                            elif kind in ("pause", "sleep"):
                                if duration_s is None:
                                    raise ValueError("duration_s is required for pause")
                                await _sleep_interruptible(float(duration_s))
                            else:
                                raise ValueError(f"Unsupported step kind: {kind}")
                            await _record_step_result(
                                step_index=i,
                                iteration=iteration,
                                kind=kind,
                                status="completed",
                                ok=True,
                                started_at=step_started,
                                finished_at=time.time(),
                                payload=payload,
                            )
                        except asyncio.CancelledError:
                            await _record_step_result(
                                step_index=i,
                                iteration=iteration,
                                kind=kind,
                                status="stopped",
                                ok=False,
                                started_at=step_started,
                                finished_at=time.time(),
                                error="cancelled",
                                payload=payload,
                            )
                            raise
                        except Exception as e:
                            await _record_step_result(
                                step_index=i,
                                iteration=iteration,
                                kind=kind,
                                status="failed",
                                ok=False,
                                started_at=step_started,
                                finished_at=time.time(),
                                error=str(e),
                                payload=payload,
                            )
                            raise

                    if not loop:
                        break
                    iteration += 1
            except asyncio.CancelledError:
                canceled = True
            except Exception as e:
                error = str(e)
            finally:
                status = "completed"
                if error:
                    status = "failed"
                elif canceled or self._stop.is_set():
                    status = "stopped"
                if self._state.db is not None:
                    try:
                        await self._state.db.update_orchestration_run(
                            run_id=run_id,
                            status=status,
                            error=error,
                            finished_at=time.time(),
                        )
                    except Exception:
                        pass
                async with self._lock:
                    self._status.running = False
                    self._status.run_id = None
                    self._status.name = None
                    self._status.loop = False
                    self._task = None

        task = asyncio.create_task(_run(), name="orchestration_runner")
        async with self._lock:
            self._status.running = True
            self._status.run_id = run_id
            self._status.name = str(name or "") or None
            self._status.started_at = time.time()
            self._status.step_index = 0
            self._status.steps_total = len(steps)
            self._status.loop = bool(loop)
            self._task = task
            return OrchestrationStatus(**self._status.__dict__)


def _require_orchestrator(state: AppState) -> OrchestrationService:
    svc = getattr(state, "orchestrator", None)
    if svc is None:
        raise HTTPException(status_code=503, detail="Orchestration service not initialized")
    return svc


async def orchestration_status(
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    svc = _require_orchestrator(state)
    st = await svc.status()
    return {"ok": True, "status": st.__dict__}


async def orchestration_runs(
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
    limit: int = 200,
    agent_id: str | None = None,
    scope: str | None = None,
    status: str | None = None,
    since: float | None = None,
    until: float | None = None,
    offset: int = 0,
) -> Dict[str, Any]:
    db = getattr(state, "db", None)
    if db is None:
        raise HTTPException(status_code=503, detail="Database not initialized")
    try:
        lim = max(1, int(limit))
        off = max(0, int(offset))
        runs = await db.list_orchestration_runs(
            limit=lim,
            agent_id=agent_id,
            scope=scope,
            status=status,
            since=since,
            until=until,
            offset=off,
        )
        count = len(runs)
        next_offset = off + count if count >= lim else None
        return {
            "ok": True,
            "runs": runs,
            "count": count,
            "limit": lim,
            "offset": off,
            "next_offset": next_offset,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def orchestration_run_detail(
    run_id: str,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
    steps_limit: int = 1000,
    steps_offset: int = 0,
    step_status: str | None = None,
    step_ok: bool | None = None,
    peers_limit: int = 2000,
    peers_offset: int = 0,
    peer_status: str | None = None,
    peer_ok: bool | None = None,
) -> Dict[str, Any]:
    db = getattr(state, "db", None)
    if db is None:
        raise HTTPException(status_code=503, detail="Database not initialized")
    try:
        run = await db.get_orchestration_run(run_id=run_id)
        if not run:
            raise HTTPException(status_code=404, detail="Run not found")
        steps_lim = max(1, int(steps_limit))
        steps_off = max(0, int(steps_offset))
        peers_lim = max(1, int(peers_limit))
        peers_off = max(0, int(peers_offset))
        steps = await db.list_orchestration_steps(
            run_id=run_id,
            limit=steps_lim,
            offset=steps_off,
            status=step_status,
            ok=step_ok,
        )
        peers = await db.list_orchestration_peer_results(
            run_id=run_id,
            limit=peers_lim,
            offset=peers_off,
            status=peer_status,
            ok=peer_ok,
        )
        steps_count = len(steps)
        peers_count = len(peers)
        steps_next = steps_off + steps_count if steps_count >= steps_lim else None
        peers_next = peers_off + peers_count if peers_count >= peers_lim else None
        return {
            "ok": True,
            "run": run,
            "steps": steps,
            "peers": peers,
            "steps_meta": {
                "count": steps_count,
                "limit": steps_lim,
                "offset": steps_off,
                "next_offset": steps_next,
                "status": step_status,
                "ok": step_ok,
            },
            "peers_meta": {
                "count": peers_count,
                "limit": peers_lim,
                "offset": peers_off,
                "next_offset": peers_next,
                "status": peer_status,
                "ok": peer_ok,
            },
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def orchestration_runs_export(
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
    limit: int = 2000,
    agent_id: str | None = None,
    scope: str | None = None,
    status: str | None = None,
    since: float | None = None,
    until: float | None = None,
    offset: int = 0,
    format: str = "csv",
) -> Response:
    db = getattr(state, "db", None)
    if db is None:
        raise HTTPException(status_code=503, detail="Database not initialized")
    try:
        runs = await db.list_orchestration_runs(
            limit=max(1, min(20000, int(limit))),
            agent_id=agent_id,
            scope=scope,
            status=status,
            since=since,
            until=until,
            offset=offset,
        )
        fmt = str(format or "csv").strip().lower()
        if fmt == "json":
            payload = json.dumps({"ok": True, "runs": runs}, indent=2)
            return Response(content=payload, media_type="application/json")

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(
            [
                "run_id",
                "agent_id",
                "created_at",
                "updated_at",
                "started_at",
                "finished_at",
                "name",
                "scope",
                "status",
                "steps_total",
                "loop",
                "include_self",
                "duration_s",
                "error",
                "payload",
            ]
        )
        for row in runs:
            payload = row.get("payload") or {}
            writer.writerow(
                [
                    row.get("run_id"),
                    row.get("agent_id"),
                    row.get("created_at"),
                    row.get("updated_at"),
                    row.get("started_at"),
                    row.get("finished_at"),
                    row.get("name"),
                    row.get("scope"),
                    row.get("status"),
                    row.get("steps_total"),
                    row.get("loop"),
                    row.get("include_self"),
                    row.get("duration_s"),
                    row.get("error"),
                    json.dumps(payload, separators=(",", ":")),
                ]
            )
        return PlainTextResponse(
            output.getvalue(),
            headers={
                "Content-Disposition": "attachment; filename=orchestration_runs.csv"
            },
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def orchestration_retention_status(
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    db = getattr(state, "db", None)
    if db is None:
        raise HTTPException(status_code=503, detail="Database not initialized")
    try:
        stats = await db.orchestration_runs_stats()
        now = time.time()
        max_rows = int(getattr(state.settings, "orchestration_runs_max_rows", 0) or 0)
        max_days = int(getattr(state.settings, "orchestration_runs_max_days", 0) or 0)
        oldest = stats.get("oldest")
        oldest_age_s = max(0.0, now - float(oldest)) if oldest else None
        excess_rows = max(0, int(stats.get("count", 0)) - max_rows) if max_rows else 0
        excess_age_s = (
            max(0.0, float(oldest_age_s) - (max_days * 86400.0))
            if max_days and oldest_age_s is not None
            else 0.0
        )
        drift = bool(excess_rows > 0 or excess_age_s > 0)
        return {
            "ok": True,
            "stats": stats,
            "settings": {
                "max_rows": max_rows,
                "max_days": max_days,
                "maintenance_interval_s": int(
                    getattr(
                        state.settings, "orchestration_runs_maintenance_interval_s", 0
                    )
                    or 0
                ),
            },
            "drift": {
                "excess_rows": int(excess_rows),
                "excess_age_s": float(excess_age_s),
                "oldest_age_s": float(oldest_age_s) if oldest_age_s is not None else None,
                "drift": drift,
            },
            "last_retention": getattr(state, "orchestration_runs_retention_last", None),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def orchestration_retention_cleanup(
    max_rows: int | None = None,
    max_days: int | None = None,
    _: Dict[str, Any] = Depends(require_admin),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    db = getattr(state, "db", None)
    if db is None:
        raise HTTPException(status_code=503, detail="Database not initialized")
    try:
        cfg_max_rows = int(
            getattr(state.settings, "orchestration_runs_max_rows", 0) or 0
        )
        cfg_max_days = int(
            getattr(state.settings, "orchestration_runs_max_days", 0) or 0
        )
        use_max_rows = max_rows if max_rows is not None else (cfg_max_rows or None)
        use_max_days = max_days if max_days is not None else (cfg_max_days or None)
        result = await db.enforce_orchestration_runs_retention(
            max_rows=use_max_rows,
            max_days=use_max_days,
        )
        state.orchestration_runs_retention_last = {
            "at": time.time(),
            "result": result,
        }
        return {"ok": True, "result": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def orchestration_run_steps_export(
    run_id: str,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
    limit: int = 5000,
    offset: int = 0,
    status: str | None = None,
    ok: bool | None = None,
    format: str = "csv",
) -> Response:
    db = getattr(state, "db", None)
    if db is None:
        raise HTTPException(status_code=503, detail="Database not initialized")
    try:
        steps = await db.list_orchestration_steps(
            run_id=run_id,
            limit=max(1, min(50000, int(limit))),
            offset=offset,
            status=status,
            ok=ok,
        )
        fmt = str(format or "csv").strip().lower()
        if fmt == "json":
            payload = json.dumps({"ok": True, "steps": steps}, indent=2)
            return Response(content=payload, media_type="application/json")

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(
            [
                "id",
                "run_id",
                "agent_id",
                "created_at",
                "updated_at",
                "started_at",
                "finished_at",
                "step_index",
                "iteration",
                "kind",
                "status",
                "ok",
                "duration_s",
                "error",
                "payload",
            ]
        )
        for row in steps:
            payload = row.get("payload") or {}
            writer.writerow(
                [
                    row.get("id"),
                    row.get("run_id"),
                    row.get("agent_id"),
                    row.get("created_at"),
                    row.get("updated_at"),
                    row.get("started_at"),
                    row.get("finished_at"),
                    row.get("step_index"),
                    row.get("iteration"),
                    row.get("kind"),
                    row.get("status"),
                    row.get("ok"),
                    row.get("duration_s"),
                    row.get("error"),
                    json.dumps(payload, separators=(",", ":")),
                ]
            )
        return PlainTextResponse(
            output.getvalue(),
            headers={
                "Content-Disposition": f"attachment; filename=orchestration_steps_{run_id}.csv"
            },
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def orchestration_run_peers_export(
    run_id: str,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
    limit: int = 10000,
    offset: int = 0,
    status: str | None = None,
    ok: bool | None = None,
    format: str = "csv",
) -> Response:
    db = getattr(state, "db", None)
    if db is None:
        raise HTTPException(status_code=503, detail="Database not initialized")
    try:
        peers = await db.list_orchestration_peer_results(
            run_id=run_id,
            limit=max(1, min(100000, int(limit))),
            offset=offset,
            status=status,
            ok=ok,
        )
        fmt = str(format or "csv").strip().lower()
        if fmt == "json":
            payload = json.dumps({"ok": True, "peers": peers}, indent=2)
            return Response(content=payload, media_type="application/json")

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(
            [
                "id",
                "run_id",
                "agent_id",
                "peer_id",
                "created_at",
                "updated_at",
                "started_at",
                "finished_at",
                "step_index",
                "iteration",
                "action",
                "status",
                "ok",
                "duration_s",
                "error",
                "payload",
            ]
        )
        for row in peers:
            payload = row.get("payload") or {}
            writer.writerow(
                [
                    row.get("id"),
                    row.get("run_id"),
                    row.get("agent_id"),
                    row.get("peer_id"),
                    row.get("created_at"),
                    row.get("updated_at"),
                    row.get("started_at"),
                    row.get("finished_at"),
                    row.get("step_index"),
                    row.get("iteration"),
                    row.get("action"),
                    row.get("status"),
                    row.get("ok"),
                    row.get("duration_s"),
                    row.get("error"),
                    json.dumps(payload, separators=(",", ":")),
                ]
            )
        return PlainTextResponse(
            output.getvalue(),
            headers={
                "Content-Disposition": f"attachment; filename=orchestration_peers_{run_id}.csv"
            },
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def orchestration_presets(
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
    limit: int = 200,
    offset: int = 0,
    scope: str | None = None,
) -> Dict[str, Any]:
    db = getattr(state, "db", None)
    if db is None:
        raise HTTPException(status_code=503, detail="Database not initialized")
    try:
        lim = max(1, int(limit))
        off = max(0, int(offset))
        presets = await db.list_orchestration_presets(
            limit=lim, offset=off, scope=scope
        )
        return {
            "ok": True,
            "presets": presets,
            "count": len(presets),
            "limit": lim,
            "offset": off,
            "next_offset": off + len(presets) if len(presets) >= lim else None,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def orchestration_presets_upsert(
    req: OrchestrationPresetUpsertRequest,
    request: Request,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    db = getattr(state, "db", None)
    if db is None:
        raise HTTPException(status_code=503, detail="Database not initialized")
    try:
        preset = await db.upsert_orchestration_preset(
            name=str(req.name or "").strip(),
            scope=str(req.scope or "local").strip() or "local",
            description=str(req.description or "") or None,
            payload=dict(req.payload or {}),
            tags=list(req.tags or []),
            version=req.version,
        )
        await log_event(
            state,
            action="orchestration.preset.upsert",
            ok=True,
            payload={"name": preset.get("name"), "scope": preset.get("scope")},
            request=request,
        )
        return {"ok": True, "preset": preset}
    except Exception as e:
        await log_event(
            state,
            action="orchestration.preset.upsert",
            ok=False,
            error=str(e),
            request=request,
        )
        raise HTTPException(status_code=500, detail=str(e))


async def orchestration_presets_export(
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
    limit: int = 2000,
    offset: int = 0,
    scope: str | None = None,
    request: Request | None = None,
) -> Dict[str, Any]:
    db = getattr(state, "db", None)
    if db is None:
        raise HTTPException(status_code=503, detail="Database not initialized")
    lim = max(1, int(limit))
    off = max(0, int(offset))
    try:
        presets = await db.list_orchestration_presets(
            limit=lim, offset=off, scope=scope
        )
        await log_event(
            state,
            action="orchestration.preset.export",
            ok=True,
            payload={"count": len(presets), "scope": scope},
            request=request,
        )
        next_offset = off + len(presets) if len(presets) >= lim else None
        return {
            "ok": True,
            "presets": presets,
            "count": len(presets),
            "limit": lim,
            "offset": off,
            "next_offset": next_offset,
        }
    except Exception as e:
        await log_event(
            state,
            action="orchestration.preset.export",
            ok=False,
            error=str(e),
            request=request,
        )
        raise HTTPException(status_code=500, detail=str(e))


async def orchestration_presets_import(
    req: OrchestrationPresetsImportRequest,
    request: Request,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    db = getattr(state, "db", None)
    if db is None:
        raise HTTPException(status_code=503, detail="Database not initialized")
    imported = 0
    errors: list[str] = []
    for preset in req.presets or []:
        try:
            await db.upsert_orchestration_preset(
                name=str(preset.name or "").strip(),
                scope=str(preset.scope or "local").strip() or "local",
                description=str(preset.description or "") or None,
                payload=dict(preset.payload or {}),
                tags=list(preset.tags or []),
                version=preset.version,
            )
            imported += 1
        except Exception as e:
            errors.append(str(e))
    await log_event(
        state,
        action="orchestration.preset.import",
        ok=not errors,
        payload={"imported": imported, "errors": len(errors)},
        request=request,
    )
    return {"ok": not errors, "imported": imported, "errors": errors}


async def orchestration_presets_delete(
    preset_id: int,
    request: Request,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    db = getattr(state, "db", None)
    if db is None:
        raise HTTPException(status_code=503, detail="Database not initialized")
    try:
        await db.delete_orchestration_preset(int(preset_id))
        await log_event(
            state,
            action="orchestration.preset.delete",
            ok=True,
            payload={"preset_id": int(preset_id)},
            request=request,
        )
        return {"ok": True}
    except Exception as e:
        await log_event(
            state,
            action="orchestration.preset.delete",
            ok=False,
            error=str(e),
            request=request,
        )
        raise HTTPException(status_code=500, detail=str(e))


async def orchestration_start(
    req: OrchestrationStartRequest,
    request: Request,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    svc = _require_orchestrator(state)
    try:
        st = await svc.start(name=req.name, steps=list(req.steps), loop=req.loop)
        await log_event(
            state,
            action="orchestration.start",
            ok=True,
            payload={"name": req.name, "steps": len(req.steps), "loop": bool(req.loop)},
            request=request,
        )
        try:
            await persist_runtime_state(
                state,
                "orchestration_start",
                {"name": req.name, "steps": len(req.steps), "loop": bool(req.loop)},
            )
        except Exception:
            pass
        return {"ok": True, "status": st.__dict__}
    except Exception as e:
        await log_event(
            state,
            action="orchestration.start",
            ok=False,
            error=str(e),
            payload={"name": req.name, "steps": len(req.steps), "loop": bool(req.loop)},
            request=request,
        )
        raise HTTPException(status_code=400, detail=str(e))


async def orchestration_stop(
    request: Request,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    svc = _require_orchestrator(state)
    st = await svc.stop()
    await log_event(state, action="orchestration.stop", ok=True, request=request)
    try:
        await persist_runtime_state(state, "orchestration_stop")
    except Exception:
        pass
    return {"ok": True, "status": st.__dict__}


async def orchestration_blackout(
    req: OrchestrationBlackoutRequest,
    request: Request,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    svc = getattr(state, "orchestrator", None)
    if svc is not None:
        try:
            await svc.stop()
        except Exception:
            pass
    try:
        from services import a2a_service

        await a2a_service.actions()["stop_all"](state, {})
    except Exception:
        pass
    payload: Dict[str, Any] = {"on": False}
    if req.transition_ms is not None:
        tt = _transition_ds(req.transition_ms)
        if tt is not None:
            payload["tt"] = tt
            payload["transition"] = tt
    if state.wled_cooldown is not None:
        await state.wled_cooldown.wait()
    try:
        res = await state.wled.apply_state(payload, verbose=False)
        await log_event(
            state,
            action="orchestration.blackout",
            ok=True,
            payload={"transition_ms": req.transition_ms},
            request=request,
        )
        try:
            await persist_runtime_state(state, "orchestration_blackout")
        except Exception:
            pass
        return {"ok": True, "result": res}
    except HTTPException as e:
        await log_event(
            state,
            action="orchestration.blackout",
            ok=False,
            error=str(getattr(e, "detail", e)),
            payload={"transition_ms": req.transition_ms},
            request=request,
        )
        raise
    except Exception as e:
        await log_event(
            state,
            action="orchestration.blackout",
            ok=False,
            error=str(e),
            payload={"transition_ms": req.transition_ms},
            request=request,
        )
        raise


async def orchestration_crossfade(
    req: OrchestrationCrossfadeRequest,
    request: Request,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    if req.look is None and req.state is None:
        raise HTTPException(status_code=400, detail="Provide look or state")
    if req.look is not None:
        looks = getattr(state, "looks", None)
        if looks is None:
            raise HTTPException(status_code=503, detail="Look service not initialized")
        bri = req.brightness
        if bri is not None:
            bri = min(state.settings.wled_max_bri, max(1, int(bri)))
        if state.wled_cooldown is not None:
            await state.wled_cooldown.wait()
        try:
            await looks.apply_look(
                dict(req.look),
                brightness_override=bri,
                transition_ms=req.transition_ms,
            )
        except Exception as e:
            await log_event(
                state,
                action="orchestration.crossfade",
                ok=False,
                error=str(e),
                payload={"kind": "look", "transition_ms": req.transition_ms},
                request=request,
            )
            raise
        if state.db is not None:
            try:
                await state.db.set_last_applied(
                    kind="look",
                    name=str(req.look.get("name") or ""),
                    file=str(req.look.get("file") or ""),
                    payload={"look": dict(req.look)},
                )
                try:
                    from services.events_service import emit_event

                    await emit_event(
                        state,
                        event_type="meta",
                        data={
                            "event": "last_applied",
                            "kind": "look",
                            "name": str(req.look.get("name") or "") or None,
                            "file": str(req.look.get("file") or "") or None,
                        },
                    )
                except Exception:
                    pass
            except Exception:
                pass
        try:
            await persist_runtime_state(state, "orchestration_crossfade")
        except Exception:
            pass
        await log_event(
            state,
            action="orchestration.crossfade",
            ok=True,
            payload={"kind": "look", "transition_ms": req.transition_ms},
            request=request,
        )
        return {"ok": True}

    payload = dict(req.state or {})
    if "bri" in payload:
        payload["bri"] = min(
            state.settings.wled_max_bri, max(1, int(payload["bri"]))
        )
    if req.transition_ms is not None:
        tt = _transition_ds(req.transition_ms)
        if tt is not None:
            payload["tt"] = tt
            payload["transition"] = tt
    if state.wled_cooldown is not None:
        await state.wled_cooldown.wait()
    try:
        res = await state.wled.apply_state(payload, verbose=False)
        await log_event(
            state,
            action="orchestration.crossfade",
            ok=True,
            payload={"kind": "state", "transition_ms": req.transition_ms},
            request=request,
        )
        try:
            await persist_runtime_state(state, "orchestration_crossfade")
        except Exception:
            pass
        return {"ok": True, "result": res}
    except HTTPException as e:
        await log_event(
            state,
            action="orchestration.crossfade",
            ok=False,
            error=str(getattr(e, "detail", e)),
            payload={"kind": "state", "transition_ms": req.transition_ms},
            request=request,
        )
        raise
    except Exception as e:
        await log_event(
            state,
            action="orchestration.crossfade",
            ok=False,
            error=str(e),
            payload={"kind": "state", "transition_ms": req.transition_ms},
            request=request,
        )
        raise
