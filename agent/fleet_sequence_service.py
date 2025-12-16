from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed

from pack_io import read_json


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
        local_invoke: Callable[[str, Dict[str, Any]], Any],
        peer_invoke: Callable[[Any, str, Dict[str, Any], float], Dict[str, Any]],
        peer_supported_actions: Callable[[Any, float], set[str]],
        default_timeout_s: float,
    ) -> None:
        self.data_dir = data_dir
        self.peers = peers
        self.local_invoke = local_invoke
        self.peer_invoke = peer_invoke
        self.peer_supported_actions = peer_supported_actions
        self.default_timeout_s = float(default_timeout_s)

        self._lock = threading.Lock()
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
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

    def _seq_dir(self) -> Path:
        d = Path(self.data_dir) / "sequences"
        d.mkdir(parents=True, exist_ok=True)
        return d

    def _load_steps(self, file: str) -> List[Dict[str, Any]]:
        p = (self._seq_dir() / file).resolve()
        base = self._seq_dir().resolve()
        if base not in p.parents:
            raise RuntimeError("Sequence file must be within DATA_DIR/sequences")
        seq = read_json(str(p))
        steps: List[Dict[str, Any]] = list((seq or {}).get("steps", []))
        if not steps:
            raise RuntimeError("Sequence has no steps")
        return steps

    def status(self) -> FleetSequenceStatus:
        with self._lock:
            return FleetSequenceStatus(**self._status.__dict__)

    def stop(self) -> FleetSequenceStatus:
        with self._lock:
            if not self._status.running:
                return self.status()
            self._stop.set()
            th = self._thread
        if th:
            th.join(timeout=2.5)
        with self._lock:
            self._status.running = False
            self._status.file = None
            self._status.started_at = None
            self._status.step_index = 0
            self._status.steps_total = 0
            self._status.loop = False
            self._status.targets = None
            self._thread = None
        return self.status()

    def start(
        self,
        *,
        file: str,
        loop: bool = False,
        targets: Optional[Sequence[str]] = None,
        include_self: bool = True,
        timeout_s: Optional[float] = None,
    ) -> FleetSequenceStatus:
        # Stop current run
        self.stop()

        steps = self._load_steps(file)
        timeout_s_val = float(timeout_s) if timeout_s is not None else self.default_timeout_s

        # Resolve peer set up-front
        selected_peers: List[Any] = []
        if targets:
            for t in targets:
                if t in self.peers:
                    selected_peers.append(self.peers[t])
        else:
            selected_peers = list(self.peers.values())

        # Cache capabilities to avoid repeated /card calls
        peer_caps: Dict[str, set[str]] = {}
        for peer in selected_peers:
            try:
                peer_caps[getattr(peer, "name", str(peer))] = self.peer_supported_actions(peer, timeout_s_val)
            except Exception:
                peer_caps[getattr(peer, "name", str(peer))] = set()

        self._stop.clear()

        def _run() -> None:
            try:
                while not self._stop.is_set():
                    for i, step in enumerate(steps):
                        with self._lock:
                            self._status.step_index = i
                            self._status.steps_total = len(steps)
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
                                params["brightness_override"] = int(step.get("brightness"))
                        elif typ == "ddp":
                            action = "start_ddp_pattern"
                            params = {"pattern": str(step.get("pattern")), "params": step.get("params") or {}, "duration_s": dur}
                            if step.get("brightness") is not None:
                                params["brightness"] = int(step.get("brightness"))
                            if step.get("fps") is not None:
                                try:
                                    params["fps"] = float(step.get("fps"))
                                except Exception:
                                    pass

                        if action:
                            # Local first (best-effort)
                            if include_self:
                                try:
                                    self.local_invoke(action, dict(params))
                                except Exception:
                                    pass

                            # Peers in parallel (best-effort)
                            eligible: List[Any] = []
                            for peer in selected_peers:
                                pname = getattr(peer, "name", str(peer))
                                if action in peer_caps.get(pname, set()):
                                    eligible.append(peer)

                            if eligible:
                                with ThreadPoolExecutor(max_workers=min(8, len(eligible))) as ex:
                                    futs = {
                                        ex.submit(self.peer_invoke, peer, action, dict(params), timeout_s_val): peer
                                        for peer in eligible
                                    }
                                    for fut in as_completed(futs):
                                        if self._stop.is_set():
                                            break
                                        try:
                                            fut.result()
                                        except Exception:
                                            continue

                        # Sleep for duration (or until stop)
                        end = time.monotonic() + max(0.05, dur)
                        while not self._stop.is_set():
                            now = time.monotonic()
                            if now >= end:
                                break
                            time.sleep(min(0.25, end - now))

                    if not loop:
                        break
            finally:
                with self._lock:
                    self._status.running = False
                    self._status.file = None
                    self._status.loop = False
                    self._thread = None

        th = threading.Thread(target=_run, name="fleet_sequence_runner", daemon=True)
        with self._lock:
            self._status.running = True
            self._status.file = file
            self._status.started_at = time.time()
            self._status.step_index = 0
            self._status.steps_total = len(steps)
            self._status.loop = bool(loop)
            self._status.include_self = bool(include_self)
            self._status.targets = list(targets) if targets else None
            self._thread = th
        th.start()
        return self.status()
