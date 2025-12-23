from __future__ import annotations

import asyncio
import inspect
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, List, Optional, Sequence

from pack_io import read_json_async


@dataclass
class FleetSequenceStatus:
    running: bool
    file: str | None
    started_at: float | None
    step_index: int
    steps_total: int
    loop: bool
    include_self: bool
    targets: List[str] | None


class FleetSequenceService:
    """
    Run a local sequence JSON file across a set of peers using A2A actions.

    This is designed so Falcon Player (FPP) can act as the scheduler/timebase and
    trigger a single endpoint which then orchestrates synchronized effects across devices.
    """

    def __init__(
        self,
        *,
        data_dir: str,
        peers: Dict[str, Any],
        local_invoke: Callable[[str, Dict[str, Any]], Any | Awaitable[Any]],
        peer_invoke: Callable[
            [Any, str, Dict[str, Any], float],
            Dict[str, Any] | Awaitable[Dict[str, Any]],
        ],
        peer_supported_actions: Callable[[Any, float], set[str] | Awaitable[set[str]]],
        peer_resolver: (
            Callable[[Optional[Sequence[str]], float], List[Any] | Awaitable[List[Any]]]
            | None
        ) = None,
        default_timeout_s: float,
        max_concurrency: int = 8,
    ) -> None:
        self.data_dir = data_dir
        self.peers = peers
        self.local_invoke = local_invoke
        self.peer_invoke = peer_invoke
        self.peer_supported_actions = peer_supported_actions
        self.peer_resolver = peer_resolver
        self.default_timeout_s = float(default_timeout_s)
        self.max_concurrency = max(1, int(max_concurrency))

        self._lock = asyncio.Lock()
        self._task: asyncio.Task[None] | None = None
        self._stop = asyncio.Event()
        self._status = FleetSequenceStatus(
            running=False,
            file=None,
            started_at=None,
            step_index=0,
            steps_total=0,
            loop=False,
            include_self=True,
            targets=None,
        )

    async def _maybe_await(self, v):  # type: ignore[no-untyped-def]
        return await v if inspect.isawaitable(v) else v

    def _seq_dir(self) -> Path:
        d = Path(self.data_dir) / "sequences"
        d.mkdir(parents=True, exist_ok=True)
        return d

    async def _load_steps(self, file: str) -> List[Dict[str, Any]]:
        p = (self._seq_dir() / file).resolve()
        base = self._seq_dir().resolve()
        if base not in p.parents:
            raise RuntimeError("Sequence file must be within DATA_DIR/sequences")

        seq = await read_json_async(str(p))
        steps: List[Dict[str, Any]] = list((seq or {}).get("steps", []))
        if not steps:
            raise RuntimeError("Sequence has no steps")
        return steps

    async def status(self) -> FleetSequenceStatus:
        async with self._lock:
            return FleetSequenceStatus(**self._status.__dict__)

    async def stop(self) -> FleetSequenceStatus:
        async with self._lock:
            if not self._status.running:
                return FleetSequenceStatus(**self._status.__dict__)
            self._stop.set()
            task = self._task

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
            self._status.running = False
            self._status.file = None
            self._status.started_at = None
            self._status.step_index = 0
            self._status.steps_total = 0
            self._status.loop = False
            self._status.targets = None
            return FleetSequenceStatus(**self._status.__dict__)

    async def start(
        self,
        *,
        file: str,
        loop: bool = False,
        targets: Optional[Sequence[str]] = None,
        include_self: bool = True,
        timeout_s: Optional[float] = None,
    ) -> FleetSequenceStatus:
        # Stop current run.
        await self.stop()

        steps = await self._load_steps(file)
        timeout_s_val = (
            float(timeout_s) if timeout_s is not None else self.default_timeout_s
        )

        # Resolve peer set up-front.
        selected_peers: List[Any] = []
        resolver = self.peer_resolver
        if resolver is not None:
            try:
                selected_peers = list(
                    await self._maybe_await(resolver(targets, float(timeout_s_val)))
                    or []
                )
            except Exception:
                selected_peers = []
        else:
            if targets:
                for t in targets:
                    if t in self.peers:
                        selected_peers.append(self.peers[t])
            else:
                selected_peers = list(self.peers.values())

        # Cache capabilities to avoid repeated /card calls.
        peer_caps: Dict[str, set[str]] = {}

        async def _caps(peer: Any) -> None:
            pname = getattr(peer, "name", str(peer))
            try:
                res = self.peer_supported_actions(peer, float(timeout_s_val))
                peer_caps[pname] = set(await self._maybe_await(res))
            except Exception:
                peer_caps[pname] = set()

        await asyncio.gather(
            *[_caps(p) for p in selected_peers], return_exceptions=True
        )

        self._stop.clear()

        async def _sleep_interruptible(seconds: float) -> None:
            dur_s = max(0.05, float(seconds))
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=dur_s)
            except asyncio.TimeoutError:
                return

        sem = asyncio.Semaphore(self.max_concurrency)

        async def _invoke_peer(peer: Any, action: str, params: Dict[str, Any]) -> None:
            async with sem:
                try:
                    res = self.peer_invoke(
                        peer, action, dict(params), float(timeout_s_val)
                    )
                    await self._maybe_await(res)
                except Exception:
                    return

        async def _run() -> None:
            try:
                while not self._stop.is_set():
                    for i, step in enumerate(steps):
                        async with self._lock:
                            self._status.step_index = int(i)
                            self._status.steps_total = int(len(steps))
                        if self._stop.is_set():
                            break

                        typ = str(step.get("type") or "").strip().lower()
                        dur = float(step.get("duration_s", 5))

                        # Map step types -> A2A action payloads.
                        action: Optional[str] = None
                        params: Dict[str, Any] = {}

                        if typ == "look":
                            action = "apply_look_spec"
                            params = {"look_spec": step.get("look") or {}}
                            if step.get("brightness") is not None:
                                params["brightness_override"] = int(
                                    step.get("brightness")
                                )
                        elif typ == "ddp":
                            action = "start_ddp_pattern"
                            params = {
                                "pattern": str(step.get("pattern")),
                                "params": step.get("params") or {},
                                "duration_s": dur,
                            }
                            if step.get("brightness") is not None:
                                params["brightness"] = int(step.get("brightness"))
                            if step.get("fps") is not None:
                                try:
                                    params["fps"] = float(step.get("fps"))
                                except Exception:
                                    pass

                        if action:
                            # Local first (best-effort).
                            if include_self:
                                try:
                                    res = self.local_invoke(action, dict(params))
                                    await self._maybe_await(res)
                                except Exception:
                                    pass

                            # Peers in parallel (best-effort).
                            eligible: List[Any] = []
                            for peer in selected_peers:
                                pname = getattr(peer, "name", str(peer))
                                if action in peer_caps.get(pname, set()):
                                    eligible.append(peer)

                            if eligible:
                                await asyncio.gather(
                                    *[
                                        _invoke_peer(p, action, params)
                                        for p in eligible
                                    ],
                                    return_exceptions=True,
                                )

                        await _sleep_interruptible(dur)

                    if not loop:
                        break
            finally:
                async with self._lock:
                    self._status.running = False
                    self._status.file = None
                    self._status.loop = False
                    self._task = None

        task = asyncio.create_task(_run(), name="fleet_sequence_runner")

        async with self._lock:
            self._status.running = True
            self._status.file = file
            self._status.started_at = time.time()
            self._status.step_index = 0
            self._status.steps_total = len(steps)
            self._status.loop = bool(loop)
            self._status.include_self = bool(include_self)
            self._status.targets = list(targets) if targets else None
            self._task = task
            return FleetSequenceStatus(**self._status.__dict__)
