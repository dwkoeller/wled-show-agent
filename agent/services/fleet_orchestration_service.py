from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import Depends, HTTPException, Request

from models.requests import FleetOrchestrationStartRequest, OrchestrationStep
from pack_io import read_json_async
from services import a2a_service, fleet_service
from services.audit_logger import log_event
from services.auth_service import require_a2a_auth
from services.runtime_state_service import persist_runtime_state
from services.state import AppState, get_state


@dataclass
class FleetOrchestrationStatus:
    running: bool
    run_id: str | None
    name: str | None
    started_at: float | None
    step_index: int
    steps_total: int
    loop: bool
    targets: list[str] | None
    include_self: bool


def _transition_ds(transition_ms: Optional[int]) -> Optional[int]:
    if transition_ms is None:
        return None
    try:
        ms = float(transition_ms)
    except Exception:
        return None
    return max(0, int(round(ms / 100.0)))


class FleetOrchestrationService:
    def __init__(self, *, state: AppState, max_concurrency: int = 8) -> None:
        self._state = state
        self._max_concurrency = max(1, int(max_concurrency))
        self._lock = asyncio.Lock()
        self._task: asyncio.Task[None] | None = None
        self._stop = asyncio.Event()
        self._status = FleetOrchestrationStatus(
            running=False,
            run_id=None,
            name=None,
            started_at=None,
            step_index=0,
            steps_total=0,
            loop=False,
            targets=None,
            include_self=True,
        )

    async def status(self) -> FleetOrchestrationStatus:
        async with self._lock:
            return FleetOrchestrationStatus(**self._status.__dict__)

    async def stop(self) -> FleetOrchestrationStatus:
        async with self._lock:
            if not self._status.running:
                return FleetOrchestrationStatus(**self._status.__dict__)
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
            self._status.targets = None
            self._status.include_self = True
            return FleetOrchestrationStatus(**self._status.__dict__)

    async def _sequence_duration_s(self, file: str) -> Optional[float]:
        try:
            base = Path(self._state.settings.data_dir).resolve()
            seq_path = (base / "sequences" / file).resolve()
            if base not in seq_path.parents:
                return None
            seq = await read_json_async(str(seq_path))
            steps = list((seq or {}).get("steps", []))
            dur = 0.0
            for step in steps:
                if isinstance(step, dict):
                    try:
                        dur += float(step.get("duration_s") or 0.0)
                    except Exception:
                        continue
            return dur if dur > 0 else None
        except Exception:
            return None

    async def start(
        self,
        *,
        name: str | None,
        steps: List[OrchestrationStep],
        loop: bool,
        targets: Optional[List[str]],
        include_self: bool,
        timeout_s: Optional[float],
    ) -> FleetOrchestrationStatus:
        await self.stop()
        if not steps:
            raise RuntimeError("steps is required")

        run_id = uuid.uuid4().hex

        timeout_s_val = (
            float(timeout_s)
            if timeout_s is not None
            else float(self._state.settings.a2a_http_timeout_s)
        )

        peers = await fleet_service._select_peers(self._state, targets)  # type: ignore[attr-defined]
        caps_list = await asyncio.gather(
            *[
                fleet_service._peer_supported_actions(  # type: ignore[attr-defined]
                    state=self._state, peer=p, timeout_s=timeout_s_val
                )
                for p in peers
            ],
            return_exceptions=True,
        )
        peer_caps: Dict[str, Optional[set[str]]] = {}
        for peer, caps in zip(peers, caps_list):
            pname = getattr(peer, "name", str(peer))
            if isinstance(caps, set):
                peer_caps[pname] = caps
            else:
                peer_caps[pname] = None

        def _peer_supports(peer: Any, action: str) -> bool:
            pname = getattr(peer, "name", str(peer))
            caps = peer_caps.get(pname)
            if caps is None:
                return True
            return action in caps

        self._stop.clear()

        if self._state.db is not None:
            try:
                await self._state.db.add_orchestration_run(
                    run_id=run_id,
                    scope="fleet",
                    name=str(name or "") or None,
                    steps_total=len(steps),
                    loop=bool(loop),
                    include_self=bool(include_self),
                    payload={
                        "steps": [s.model_dump() for s in steps],
                        "targets": list(targets or []),
                    },
                )
            except Exception:
                pass

        async def _sleep_interruptible(seconds: float) -> None:
            dur_s = max(0.05, float(seconds))
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=dur_s)
            except asyncio.TimeoutError:
                return

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

        async def _record_peer_result(
            *,
            step_index: int,
            iteration: int,
            peer_id: str,
            action: str,
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
                await db.add_orchestration_peer_result(
                    run_id=run_id,
                    step_index=int(step_index),
                    iteration=int(iteration),
                    peer_id=str(peer_id),
                    action=str(action),
                    status=str(status),
                    ok=bool(ok),
                    started_at=float(started_at),
                    finished_at=float(finished_at),
                    error=error,
                    payload=dict(payload or {}),
                )
            except Exception:
                pass

        sem = asyncio.Semaphore(self._max_concurrency)

        async def _invoke_peer(
            peer: Any,
            action: str,
            params: Dict[str, Any],
            *,
            step_index: int,
            iteration: int,
        ) -> Dict[str, Any]:
            pname = getattr(peer, "name", str(peer))
            base_url = str(getattr(peer, "base_url", "") or "").strip()
            if not _peer_supports(peer, action):
                now = time.time()
                await _record_peer_result(
                    step_index=step_index,
                    iteration=iteration,
                    peer_id=str(pname),
                    action=action,
                    status="skipped",
                    ok=False,
                    started_at=now,
                    finished_at=now,
                    error="unsupported_action",
                    payload={"base_url": base_url},
                )
                return {"peer": pname, "ok": False, "skipped": True}
            start = time.time()
            ok = True
            status = "completed"
            err: str | None = None
            try:
                async with sem:
                    payload = {"action": str(action), "params": dict(params or {})}
                    await fleet_service._peer_post_json(  # type: ignore[attr-defined]
                        state=self._state,
                        peer=peer,
                        path="/v1/a2a/invoke",
                        payload=payload,
                        timeout_s=timeout_s_val,
                    )
            except Exception as e:
                ok = False
                status = "failed"
                err = str(e)
            finally:
                end = time.time()
                await _record_peer_result(
                    step_index=step_index,
                    iteration=iteration,
                    peer_id=str(pname),
                    action=action,
                    status=status,
                    ok=ok,
                    started_at=start,
                    finished_at=end,
                    error=err,
                    payload={"base_url": base_url, "params": dict(params or {})},
                )
            return {"peer": pname, "ok": ok, "error": err}

        async def _invoke_local(
            action: str, params: Dict[str, Any], *, step_index: int, iteration: int
        ) -> Dict[str, Any]:
            aid = str(self._state.settings.agent_id)
            start = time.time()
            ok = True
            status = "completed"
            err: str | None = None
            fn = a2a_service.actions().get(action)
            if fn is None:
                ok = False
                status = "skipped"
                err = "unsupported_action"
            else:
                try:
                    await fn(self._state, dict(params or {}))
                except Exception as e:
                    ok = False
                    status = "failed"
                    err = str(e)
            end = time.time()
            await _record_peer_result(
                step_index=step_index,
                iteration=iteration,
                peer_id=aid,
                action=action,
                status=status,
                ok=ok,
                started_at=start,
                finished_at=end,
                error=err,
                payload={"local": True, "params": dict(params or {})},
            )
            return {"peer": aid, "ok": ok, "error": err, "local": True}

        async def _invoke_all(
            action: str, params: Dict[str, Any], *, step_index: int, iteration: int
        ) -> list[dict[str, Any]]:
            results: list[dict[str, Any]] = []
            if include_self:
                results.append(
                    await _invoke_local(
                        action, params, step_index=step_index, iteration=iteration
                    )
                )
            tasks: list[asyncio.Task[dict[str, Any]]] = []
            for peer in peers:
                tasks.append(
                    asyncio.create_task(
                        _invoke_peer(
                            peer,
                            action,
                            params,
                            step_index=step_index,
                            iteration=iteration,
                        )
                    )
                )
            if tasks:
                done = await asyncio.gather(*tasks, return_exceptions=True)
                for item in done:
                    if isinstance(item, dict):
                        results.append(item)
            return results

        async def _invoke_all_staggered(
            action: str,
            params: Dict[str, Any],
            *,
            step_index: int,
            iteration: int,
            stagger_s: float,
            start_delay_s: float,
        ) -> list[dict[str, Any]]:
            results: list[dict[str, Any]] = []

            async def _invoke_local_delayed(delay_s: float) -> dict[str, Any]:
                if delay_s > 0:
                    await asyncio.sleep(delay_s)
                return await _invoke_local(
                    action, params, step_index=step_index, iteration=iteration
                )

            async def _invoke_peer_delayed(peer: Any, delay_s: float) -> dict[str, Any]:
                if delay_s > 0:
                    await asyncio.sleep(delay_s)
                return await _invoke_peer(
                    peer,
                    action,
                    params,
                    step_index=step_index,
                    iteration=iteration,
                )

            tasks: list[asyncio.Task[dict[str, Any]]] = []
            index = 0
            if include_self:
                tasks.append(
                    asyncio.create_task(
                        _invoke_local_delayed(start_delay_s + (index * stagger_s))
                    )
                )
                index += 1
            for peer in peers:
                tasks.append(
                    asyncio.create_task(
                        _invoke_peer_delayed(
                            peer, start_delay_s + (index * stagger_s)
                        )
                    )
                )
                index += 1

            if tasks:
                done = await asyncio.gather(*tasks, return_exceptions=True)
                for item in done:
                    if isinstance(item, dict):
                        results.append(item)
            return results

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
                        step_payload: Dict[str, Any] = {
                            "step": step.model_dump(),
                            "iteration": iteration,
                            "targets": list(targets or []),
                            "include_self": bool(include_self),
                        }
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
                                payload=step_payload,
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
                            "sleep",
                            "ledfx_scene",
                            "ledfx_effect",
                            "ledfx_brightness",
                        ):
                            duration_s = 5.0

                        results: list[dict[str, Any]] = []
                        step_status = "completed"
                        step_ok = True
                        step_error: str | None = None
                        try:
                            if kind == "look":
                                if not isinstance(step.look, dict):
                                    raise ValueError("look must be an object")
                                params: Dict[str, Any] = {"look_spec": dict(step.look)}
                                if step.brightness is not None:
                                    params["brightness_override"] = min(
                                        self._state.settings.wled_max_bri,
                                        max(1, int(step.brightness)),
                                    )
                                if step.transition_ms is not None:
                                    params["transition_ms"] = int(step.transition_ms)
                                results = await _invoke_all(
                                    "apply_look_spec",
                                    params,
                                    step_index=i,
                                    iteration=iteration,
                                )
                                if duration_s is not None:
                                    await _sleep_interruptible(float(duration_s))
                            elif kind == "state":
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
                                results = await _invoke_all(
                                    "apply_state",
                                    {"state": payload},
                                    step_index=i,
                                    iteration=iteration,
                                )
                                if duration_s is not None:
                                    await _sleep_interruptible(float(duration_s))
                            elif kind == "crossfade":
                                if step.look is None and step.state is None:
                                    raise ValueError("crossfade requires look or state")
                                params: Dict[str, Any] = {}
                                if step.look is not None:
                                    if not isinstance(step.look, dict):
                                        raise ValueError("look must be an object")
                                    params["look"] = dict(step.look)
                                if step.state is not None:
                                    if not isinstance(step.state, dict):
                                        raise ValueError("state must be an object")
                                    params["state"] = dict(step.state)
                                if step.brightness is not None:
                                    params["brightness"] = min(
                                        self._state.settings.wled_max_bri,
                                        max(1, int(step.brightness)),
                                    )
                                if step.transition_ms is not None:
                                    params["transition_ms"] = int(step.transition_ms)
                                results = await _invoke_all(
                                    "crossfade",
                                    params,
                                    step_index=i,
                                    iteration=iteration,
                                )
                                if duration_s is not None:
                                    await _sleep_interruptible(float(duration_s))
                            elif kind == "preset":
                                if step.preset_id is None:
                                    raise ValueError(
                                        "preset_id is required for preset steps"
                                    )
                                params = {"preset_id": int(step.preset_id)}
                                if step.brightness is not None:
                                    params["brightness"] = int(step.brightness)
                                if step.transition_ms is not None:
                                    params["transition_ms"] = int(step.transition_ms)
                                results = await _invoke_all(
                                    "apply_preset",
                                    params,
                                    step_index=i,
                                    iteration=iteration,
                                )
                                if duration_s is not None:
                                    await _sleep_interruptible(float(duration_s))
                            elif kind == "blackout":
                                payload = {"on": False}
                                if step.transition_ms is not None:
                                    tt = _transition_ds(step.transition_ms)
                                    if tt is not None:
                                        payload["tt"] = tt
                                        payload["transition"] = tt
                                results = await _invoke_all(
                                    "apply_state",
                                    {"state": payload},
                                    step_index=i,
                                    iteration=iteration,
                                )
                                if duration_s is not None:
                                    await _sleep_interruptible(float(duration_s))
                            elif kind == "ledfx_scene":
                                scene_id = str(step.ledfx_scene_id or "").strip()
                                if not scene_id:
                                    raise ValueError("ledfx_scene_id is required")
                                action = str(step.ledfx_scene_action or "activate").strip().lower()
                                action_name = (
                                    "ledfx_deactivate_scene"
                                    if action in ("deactivate", "stop", "off", "disable")
                                    else "ledfx_activate_scene"
                                )
                                results = await _invoke_all(
                                    action_name,
                                    {"scene_id": scene_id},
                                    step_index=i,
                                    iteration=iteration,
                                )
                                if duration_s is not None:
                                    await _sleep_interruptible(float(duration_s))
                            elif kind == "ledfx_effect":
                                effect = str(step.ledfx_effect or "").strip()
                                if not effect:
                                    raise ValueError("ledfx_effect is required")
                                params: Dict[str, Any] = {"effect": effect}
                                if step.ledfx_virtual_id:
                                    params["virtual_id"] = str(step.ledfx_virtual_id)
                                if step.ledfx_config:
                                    params["config"] = dict(step.ledfx_config)
                                results = await _invoke_all(
                                    "ledfx_virtual_effect",
                                    params,
                                    step_index=i,
                                    iteration=iteration,
                                )
                                if duration_s is not None:
                                    await _sleep_interruptible(float(duration_s))
                            elif kind == "ledfx_brightness":
                                if step.ledfx_brightness is None:
                                    raise ValueError("ledfx_brightness is required")
                                params = {"brightness": float(step.ledfx_brightness)}
                                if step.ledfx_virtual_id:
                                    params["virtual_id"] = str(step.ledfx_virtual_id)
                                results = await _invoke_all(
                                    "ledfx_virtual_brightness",
                                    params,
                                    step_index=i,
                                    iteration=iteration,
                                )
                                if duration_s is not None:
                                    await _sleep_interruptible(float(duration_s))
                            elif kind == "sequence":
                                if not step.sequence_file:
                                    raise ValueError("sequence_file is required")
                                if step.loop and duration_s is None:
                                    raise ValueError(
                                        "duration_s is required when loop=true"
                                    )
                                if duration_s is None:
                                    duration_s = await self._sequence_duration_s(
                                        str(step.sequence_file)
                                    )
                                stagger_s = max(
                                    0.0, float(step.stagger_s or 0.0)
                                )
                                start_delay_s = max(
                                    0.0, float(step.start_delay_s or 0.0)
                                )
                                if stagger_s > 0.0 or start_delay_s > 0.0:
                                    results.extend(
                                        await _invoke_all_staggered(
                                            "start_sequence",
                                            {
                                                "file": str(step.sequence_file),
                                                "loop": bool(step.loop),
                                            },
                                            step_index=i,
                                            iteration=iteration,
                                            stagger_s=stagger_s,
                                            start_delay_s=start_delay_s,
                                        )
                                    )
                                else:
                                    results.extend(
                                        await _invoke_all(
                                            "start_sequence",
                                            {
                                                "file": str(step.sequence_file),
                                                "loop": bool(step.loop),
                                            },
                                            step_index=i,
                                            iteration=iteration,
                                        )
                                    )
                                if duration_s is not None:
                                    await _sleep_interruptible(float(duration_s))
                                    if step.loop:
                                        results.extend(
                                            await _invoke_all(
                                                "stop_sequence",
                                                {},
                                                step_index=i,
                                                iteration=iteration,
                                            )
                                        )
                            elif kind == "ddp":
                                if not step.pattern:
                                    raise ValueError("pattern is required for ddp steps")
                                duration = float(step.duration_s or 5.0)
                                params = {
                                    "pattern": str(step.pattern or ""),
                                    "params": dict(step.params or {}),
                                    "duration_s": duration,
                                }
                                if step.brightness is not None:
                                    params["brightness"] = int(step.brightness)
                                if step.fps is not None:
                                    params["fps"] = float(step.fps)
                                results = await _invoke_all(
                                    "start_ddp_pattern",
                                    params,
                                    step_index=i,
                                    iteration=iteration,
                                )
                                await _sleep_interruptible(duration)
                            elif kind in ("pause", "sleep"):
                                if duration_s is None:
                                    raise ValueError("duration_s is required for pause")
                                await _sleep_interruptible(float(duration_s))
                            else:
                                raise ValueError(f"Unsupported step kind: {kind}")
                        except asyncio.CancelledError:
                            step_status = "stopped"
                            step_ok = False
                            step_error = "cancelled"
                            await _record_step_result(
                                step_index=i,
                                iteration=iteration,
                                kind=kind,
                                status=step_status,
                                ok=step_ok,
                                started_at=step_started,
                                finished_at=time.time(),
                                error=step_error,
                                payload=step_payload,
                            )
                            raise
                        except Exception as e:
                            step_status = "failed"
                            step_ok = False
                            step_error = str(e)
                            await _record_step_result(
                                step_index=i,
                                iteration=iteration,
                                kind=kind,
                                status=step_status,
                                ok=step_ok,
                                started_at=step_started,
                                finished_at=time.time(),
                                error=step_error,
                                payload=step_payload,
                            )
                            raise

                        peer_total = len(results)
                        peer_failures = len([r for r in results if not r.get("ok")])
                        peer_skipped = len([r for r in results if r.get("skipped")])
                        if peer_failures > 0:
                            step_status = "partial"
                            step_ok = False
                        if (
                            peer_total == 0
                            and kind not in ("pause", "sleep")
                            and not include_self
                            and not peers
                        ):
                            step_status = "skipped"
                            step_ok = False
                            step_payload["note"] = "no_targets"
                        step_payload["peer_total"] = peer_total
                        step_payload["peer_failures"] = peer_failures
                        step_payload["peer_skipped"] = peer_skipped
                        await _record_step_result(
                            step_index=i,
                            iteration=iteration,
                            kind=kind,
                            status=step_status,
                            ok=step_ok,
                            started_at=step_started,
                            finished_at=time.time(),
                            error=step_error,
                            payload=step_payload,
                        )

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
                    self._status.targets = None
                    self._task = None

        task = asyncio.create_task(_run(), name="fleet_orchestration_runner")
        async with self._lock:
            self._status.running = True
            self._status.run_id = run_id
            self._status.name = str(name or "") or None
            self._status.started_at = time.time()
            self._status.step_index = 0
            self._status.steps_total = len(steps)
            self._status.loop = bool(loop)
            self._status.targets = list(targets) if targets else None
            self._status.include_self = bool(include_self)
            self._task = task
            return FleetOrchestrationStatus(**self._status.__dict__)


def _require_fleet_orchestrator(state: AppState) -> FleetOrchestrationService:
    svc = getattr(state, "fleet_orchestrator", None)
    if svc is None:
        raise HTTPException(
            status_code=503, detail="Fleet orchestration service not initialized"
        )
    return svc


async def fleet_orchestration_status(
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    svc = _require_fleet_orchestrator(state)
    st = await svc.status()
    return {"ok": True, "status": st.__dict__}


async def fleet_orchestration_start(
    req: FleetOrchestrationStartRequest,
    request: Request,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    svc = _require_fleet_orchestrator(state)
    try:
        st = await svc.start(
            name=req.name,
            steps=list(req.steps),
            loop=req.loop,
            targets=req.targets,
            include_self=req.include_self,
            timeout_s=req.timeout_s,
        )
        await log_event(
            state,
            action="fleet.orchestration.start",
            ok=True,
            payload={
                "name": req.name,
                "steps": len(req.steps),
                "loop": bool(req.loop),
                "targets": req.targets,
            },
            request=request,
        )
        try:
            await persist_runtime_state(
                state,
                "fleet_orchestration_start",
                {
                    "name": req.name,
                    "steps": len(req.steps),
                    "loop": bool(req.loop),
                    "targets": req.targets,
                },
            )
        except Exception:
            pass
        return {"ok": True, "status": st.__dict__}
    except Exception as e:
        await log_event(
            state,
            action="fleet.orchestration.start",
            ok=False,
            error=str(e),
            payload={
                "name": req.name,
                "steps": len(req.steps),
                "loop": bool(req.loop),
                "targets": req.targets,
            },
            request=request,
        )
        raise HTTPException(status_code=400, detail=str(e))


async def fleet_orchestration_stop(
    request: Request,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    svc = _require_fleet_orchestrator(state)
    st = await svc.stop()
    await log_event(state, action="fleet.orchestration.stop", ok=True, request=request)
    try:
        await persist_runtime_state(state, "fleet_orchestration_stop")
    except Exception:
        pass
    return {"ok": True, "status": st.__dict__}
