from __future__ import annotations

import asyncio
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

import httpx
from fastapi import FastAPI

from config import load_settings
from config.constants import APP_VERSION, SERVICE_NAME
from jobs import AsyncJobManager
from look_service import LookService
from pack_io import ensure_dir
from preset_importer import PresetImporter
from rate_limiter import AsyncCooldown, AsyncCooldownSyncAdapter
from ddp_sender import DDPConfig
from ddp_streamer import DDPStreamer
from geometry import TreeGeometry
from sequence_service import SequenceService
from fleet_sequence_service import FleetSequenceService
from services import a2a_service, fleet_service
from services.scheduler_async import AsyncSchedulerService
from services.state import AppState
from wled_client import AsyncWLEDClient, AsyncWLEDClientSyncAdapter
from wled_mapper import WLEDMapper


@dataclass(frozen=True)
class A2APeer:
    name: str
    base_url: str


def parse_a2a_peers(entries: list[str]) -> dict[str, A2APeer]:
    peers: dict[str, A2APeer] = {}
    for raw in entries:
        item = str(raw).strip()
        if not item:
            continue
        if "=" in item:
            name, url = item.split("=", 1)
            name = name.strip()
            url = url.strip()
        else:
            name = ""
            url = item
        if not url:
            continue
        if not (url.startswith("http://") or url.startswith("https://")):
            url = "http://" + url
        url = url.rstrip("/")
        if not name:
            try:
                from urllib.parse import urlparse

                p = urlparse(url)
                host = p.hostname or "peer"
                name = host
                if p.port:
                    name = f"{host}:{p.port}"
            except Exception:
                name = url
        peers[name] = A2APeer(name=name, base_url=url)
    return peers


_STATE_LOCK = asyncio.Lock()
_STATE_INITIALIZED = False


async def startup(app: FastAPI | None = None) -> None:
    global _STATE_INITIALIZED
    async with _STATE_LOCK:
        if _STATE_INITIALIZED:
            return

        settings = load_settings()
        ensure_dir(settings.data_dir)
        started_at = time.time()
        loop = asyncio.get_running_loop()

        # Shared async HTTP client for all outbound calls (WLED + peers).
        peer_http = httpx.AsyncClient(
            follow_redirects=True,
            headers={"User-Agent": f"{SERVICE_NAME}/{APP_VERSION}"},
            limits=httpx.Limits(max_connections=32, max_keepalive_connections=16),
        )

        # Optional async DB service (best-effort).
        db = None
        if settings.database_url:
            try:
                from services.db_service import DatabaseService

                db = DatabaseService(
                    database_url=settings.database_url,
                    agent_id=settings.agent_id,
                )
                await db.init()
            except Exception:
                db = None

        # Jobs (async core loop).
        jobs = AsyncJobManager(
            loop=loop,
            persist_path=os.path.join(settings.data_dir, "jobs", "jobs.json"),
            db=db,
        )
        await jobs.init()

        if settings.controller_kind != "wled":
            raise RuntimeError(
                "This app controls WLED devices. For ESPixelStick pixel controllers, run pixel_main:app (CONTROLLER_KIND=pixel)."
            )

        # WLED clients.
        wled = AsyncWLEDClient(
            settings.wled_tree_url,
            client=peer_http,
            timeout_s=float(settings.wled_http_timeout_s),
        )
        wled_sync = AsyncWLEDClientSyncAdapter(wled, loop=loop)
        wled_mapper = WLEDMapper(wled_sync)

        # Segment IDs: if not configured, try auto-detect; fall back to [0].
        segment_ids = list(settings.wled_segment_ids)
        if not segment_ids:
            try:
                segment_ids = await wled.get_segment_ids(refresh=True)
            except Exception:
                segment_ids = []
        if not segment_ids:
            segment_ids = [0]

        # Cooldown shared across async + thread services.
        wled_cooldown = AsyncCooldown(settings.wled_command_cooldown_ms)
        cooldown_sync = AsyncCooldownSyncAdapter(wled_cooldown, loop=loop)

        looks = LookService(
            wled=wled_sync,
            mapper=wled_mapper,
            data_dir=settings.data_dir,
            max_bri=settings.wled_max_bri,
            segment_ids=segment_ids,
            replicate_to_all_segments=settings.wled_replicate_to_all_segments,
        )
        importer = PresetImporter(
            wled=wled_sync,
            mapper=wled_mapper,
            cooldown=cooldown_sync,
            max_bri=settings.wled_max_bri,
            segment_ids=segment_ids,
            replicate_to_all_segments=settings.wled_replicate_to_all_segments,
        )

        geom = TreeGeometry(
            runs=settings.tree_runs,
            pixels_per_run=settings.tree_pixels_per_run,
            segment_len=settings.tree_segment_len,
            segments_per_run=settings.tree_segments_per_run,
        )
        ddp_cfg = DDPConfig(
            host=settings.ddp_host,
            port=settings.ddp_port,
            destination_id=settings.ddp_destination_id,
            max_pixels_per_packet=settings.ddp_max_pixels_per_packet,
        )
        ddp = DDPStreamer(
            wled=wled_sync,
            geometry=geom,
            ddp_cfg=ddp_cfg,
            fps_default=settings.ddp_fps_default,
            fps_max=settings.ddp_fps_max,
            segment_ids=segment_ids,
        )
        sequences = SequenceService(
            wled=wled_sync,
            looks=looks,
            ddp=ddp,
            data_dir=settings.data_dir,
        )

        peers = parse_a2a_peers(list(settings.a2a_peers))

        st = AppState(
            settings=settings,
            started_at=started_at,
            segment_ids=segment_ids,
            wled_cooldown=wled_cooldown,
            wled=wled,
            wled_sync=wled_sync,
            wled_mapper=wled_mapper,
            looks=looks,
            importer=importer,
            ddp=ddp,
            sequences=sequences,
            fleet_sequences=None,
            director=None,
            runtime_state_path=os.path.join(
                settings.data_dir, "state", "runtime_state.json"
            ),
            kv_runtime_state_key="runtime_state",
            db=db,
            jobs=jobs,
            scheduler=None,
            peers=dict(peers),
            peer_http=peer_http,
            loop=loop,
            maintenance_tasks=[],
        )

        timeout_default = float(settings.a2a_http_timeout_s)

        def _run_on_loop(coro, timeout_s: float | None = None):  # type: ignore[no-untyped-def]
            fut = asyncio.run_coroutine_threadsafe(coro, loop)
            return fut.result(timeout=float(timeout_s or timeout_default) + 5.0)

        # Fleet sequence coordinator (optional; safe when peers is empty too).
        try:

            def _local_invoke(action: str, params: Dict[str, Any]) -> Any:
                fn = a2a_service.actions().get(str(action))
                if fn is None:
                    raise RuntimeError(f"Unknown local action '{action}'")
                return _run_on_loop(fn(st, dict(params or {})))

            def _peer_supported_actions(peer: Any, timeout_s: float) -> set[str]:
                return _run_on_loop(
                    fleet_service._peer_supported_actions(  # type: ignore[attr-defined]
                        state=st,
                        peer=peer,
                        timeout_s=float(timeout_s),
                    ),
                    timeout_s=float(timeout_s),
                )

            def _peer_invoke(
                peer: Any, action: str, params: Dict[str, Any], timeout_s: float
            ) -> Dict[str, Any]:
                payload = {"action": str(action), "params": dict(params or {})}
                return _run_on_loop(
                    fleet_service._peer_post_json(  # type: ignore[attr-defined]
                        state=st,
                        peer=peer,
                        path="/v1/a2a/invoke",
                        payload=payload,
                        timeout_s=float(timeout_s),
                    ),
                    timeout_s=float(timeout_s),
                )

            st.fleet_sequences = FleetSequenceService(
                data_dir=settings.data_dir,
                peers=dict(peers),
                local_invoke=_local_invoke,
                peer_invoke=_peer_invoke,
                peer_supported_actions=_peer_supported_actions,
                default_timeout_s=float(settings.a2a_http_timeout_s),
            )
        except Exception:
            st.fleet_sequences = None

        # Optional OpenAI director (called via asyncio.to_thread in command_service).
        try:
            if settings.openai_api_key:
                from openai_agent import SimpleDirectorAgent

                def _tool_apply_random_look(kwargs: Dict[str, Any]) -> Any:
                    from models.requests import FleetApplyRandomLookRequest

                    req = FleetApplyRandomLookRequest(
                        theme=kwargs.get("theme"),
                        brightness=kwargs.get("brightness"),
                        seed=kwargs.get("seed"),
                        include_self=True,
                    )
                    return _run_on_loop(
                        fleet_service.fleet_apply_random_look(req, state=st),
                        timeout_s=float(settings.a2a_http_timeout_s),
                    )

                def _tool_start_ddp_pattern(kwargs: Dict[str, Any]) -> Any:
                    return _run_on_loop(
                        a2a_service.actions()["start_ddp_pattern"](
                            st, dict(kwargs or {})
                        ),
                        timeout_s=float(settings.wled_http_timeout_s),
                    )

                def _tool_stop_ddp(kwargs: Dict[str, Any]) -> Any:
                    return _run_on_loop(
                        a2a_service.actions()["stop_ddp"](st, dict(kwargs or {})),
                        timeout_s=float(settings.wled_http_timeout_s),
                    )

                def _tool_stop_all(kwargs: Dict[str, Any]) -> Any:
                    return _run_on_loop(
                        a2a_service.actions()["stop_all"](st, dict(kwargs or {})),
                        timeout_s=float(settings.wled_http_timeout_s),
                    )

                def _tool_generate_looks_pack(kwargs: Dict[str, Any]) -> Any:
                    total = int(kwargs.get("total_looks", 800))
                    themes = kwargs.get("themes") or [
                        "classic",
                        "candy_cane",
                        "icy",
                        "warm_white",
                        "rainbow",
                    ]
                    bri = int(kwargs.get("brightness", settings.wled_max_bri))
                    seed = int(kwargs.get("seed", 1337))
                    return looks.generate_pack(
                        total_looks=total,
                        themes=themes,
                        brightness=bri,
                        seed=seed,
                        write_files=True,
                        include_multi_segment=True,
                    ).__dict__

                def _tool_fleet_start_sequence(kwargs: Dict[str, Any]) -> Any:
                    svc = getattr(st, "fleet_sequences", None)
                    if svc is None:
                        return {
                            "ok": False,
                            "error": "Fleet sequences are not available.",
                        }
                    file = str(
                        kwargs.get("file") or kwargs.get("sequence_file") or ""
                    ).strip()
                    if not file:
                        return {"ok": False, "error": "Missing 'file'."}
                    targets = kwargs.get("targets")
                    if targets is not None and not isinstance(targets, list):
                        targets = None
                    include_self = bool(kwargs.get("include_self", True))
                    loop_flag = bool(kwargs.get("loop", False))
                    timeout_s = kwargs.get("timeout_s")
                    try:
                        st2 = svc.start(
                            file=file,
                            loop=loop_flag,
                            targets=(
                                [str(x) for x in (targets or [])] if targets else None
                            ),
                            include_self=include_self,
                            timeout_s=(
                                float(timeout_s) if timeout_s is not None else None
                            ),
                        )
                        return {"ok": True, "status": st2.__dict__}
                    except Exception as e:
                        return {"ok": False, "error": str(e)}

                def _tool_fleet_stop_sequence(_: Dict[str, Any]) -> Any:
                    svc = getattr(st, "fleet_sequences", None)
                    if svc is None:
                        return {
                            "ok": False,
                            "error": "Fleet sequences are not available.",
                        }
                    try:
                        st2 = svc.stop()
                        return {"ok": True, "status": st2.__dict__}
                    except Exception as e:
                        return {"ok": False, "error": str(e)}

                def _tool_fpp_start_playlist(kwargs: Dict[str, Any]) -> Any:
                    if not settings.fpp_base_url:
                        return {
                            "ok": False,
                            "error": "FPP is not configured; set FPP_BASE_URL.",
                        }
                    name = str(kwargs.get("name") or "").strip()
                    if not name:
                        return {"ok": False, "error": "Missing 'name'."}
                    repeat = bool(kwargs.get("repeat", False))

                    async def _op() -> Dict[str, Any]:
                        from fpp_client import AsyncFPPClient

                        if st.peer_http is None:
                            return {"ok": False, "error": "HTTP client not initialized"}
                        fpp = AsyncFPPClient(
                            base_url=settings.fpp_base_url,
                            client=st.peer_http,
                            timeout_s=float(settings.fpp_http_timeout_s),
                            headers={k: v for (k, v) in settings.fpp_headers},
                        )
                        resp = await fpp.start_playlist(name, repeat=repeat)
                        return {"ok": True, "fpp": resp.as_dict()}

                    return _run_on_loop(
                        _op(), timeout_s=float(settings.fpp_http_timeout_s)
                    )

                def _tool_fpp_stop_playlist(_: Dict[str, Any]) -> Any:
                    if not settings.fpp_base_url:
                        return {
                            "ok": False,
                            "error": "FPP is not configured; set FPP_BASE_URL.",
                        }

                    async def _op() -> Dict[str, Any]:
                        from fpp_client import AsyncFPPClient

                        if st.peer_http is None:
                            return {"ok": False, "error": "HTTP client not initialized"}
                        fpp = AsyncFPPClient(
                            base_url=settings.fpp_base_url,
                            client=st.peer_http,
                            timeout_s=float(settings.fpp_http_timeout_s),
                            headers={k: v for (k, v) in settings.fpp_headers},
                        )
                        resp = await fpp.stop_playlist()
                        return {"ok": True, "fpp": resp.as_dict()}

                    return _run_on_loop(
                        _op(), timeout_s=float(settings.fpp_http_timeout_s)
                    )

                def _tool_fpp_trigger_event(kwargs: Dict[str, Any]) -> Any:
                    if not settings.fpp_base_url:
                        return {
                            "ok": False,
                            "error": "FPP is not configured; set FPP_BASE_URL.",
                        }
                    try:
                        event_id = int(kwargs.get("event_id"))
                    except Exception:
                        return {"ok": False, "error": "event_id must be an integer > 0"}

                    async def _op() -> Dict[str, Any]:
                        from fpp_client import AsyncFPPClient

                        if st.peer_http is None:
                            return {"ok": False, "error": "HTTP client not initialized"}
                        fpp = AsyncFPPClient(
                            base_url=settings.fpp_base_url,
                            client=st.peer_http,
                            timeout_s=float(settings.fpp_http_timeout_s),
                            headers={k: v for (k, v) in settings.fpp_headers},
                        )
                        resp = await fpp.trigger_event(event_id)
                        return {"ok": True, "fpp": resp.as_dict()}

                    return _run_on_loop(
                        _op(), timeout_s=float(settings.fpp_http_timeout_s)
                    )

                st.director = SimpleDirectorAgent(
                    api_key=settings.openai_api_key,
                    model=settings.openai_model,
                    tools={
                        "apply_random_look": _tool_apply_random_look,
                        "start_ddp_pattern": _tool_start_ddp_pattern,
                        "stop_ddp": _tool_stop_ddp,
                        "stop_all": _tool_stop_all,
                        "generate_looks_pack": _tool_generate_looks_pack,
                        "fleet_start_sequence": _tool_fleet_start_sequence,
                        "fleet_stop_sequence": _tool_fleet_stop_sequence,
                        "fpp_start_playlist": _tool_fpp_start_playlist,
                        "fpp_stop_playlist": _tool_fpp_stop_playlist,
                        "fpp_trigger_event": _tool_fpp_trigger_event,
                    },
                )
        except Exception:
            st.director = None

        # Scheduler (async core loop).
        try:
            sched = AsyncSchedulerService(
                state=st,
                config_path=os.path.join(settings.data_dir, "show", "scheduler.json"),
            )
            await sched.init()
            st.scheduler = sched
            cfg = await sched.get_config()
            if cfg.autostart and cfg.enabled:
                await sched.start()
        except Exception:
            st.scheduler = None

        # DB maintenance: job retention.
        if db is not None and (
            settings.job_history_max_rows > 0 or settings.job_history_max_days > 0
        ):
            interval_s = float(settings.job_history_maintenance_interval_s or 3600)

            async def _retention_loop() -> None:
                while True:
                    try:
                        await db.enforce_job_retention(
                            max_rows=(
                                settings.job_history_max_rows
                                if settings.job_history_max_rows > 0
                                else None
                            ),
                            max_days=(
                                settings.job_history_max_days
                                if settings.job_history_max_days > 0
                                else None
                            ),
                        )
                    except Exception:
                        pass
                    await asyncio.sleep(max(30.0, interval_s))

            st.maintenance_tasks.append(
                asyncio.create_task(_retention_loop(), name="db_job_retention")
            )

        # Optional DB reconcile on startup.
        if db is not None and settings.db_reconcile_on_startup:
            try:
                from services.reconcile_service import reconcile_data_dir

                st.maintenance_tasks.append(
                    asyncio.create_task(
                        reconcile_data_dir(st),
                        name="db_reconcile_data_dir",
                    )
                )
            except Exception:
                pass

        if app is not None:
            app.state.wsa = st  # type: ignore[attr-defined]

        _STATE_INITIALIZED = True


async def shutdown(app: FastAPI | None = None) -> None:
    global _STATE_INITIALIZED

    st: AppState | None = None
    if app is not None:
        st = getattr(app.state, "wsa", None)

    if st is not None:
        # Stop jobs + scheduler early (before DB close).
        try:
            jobs = getattr(st, "jobs", None)
            if jobs is not None and hasattr(jobs, "shutdown"):
                await jobs.shutdown(reason="Server shutting down")
        except Exception:
            pass

        try:
            sched = getattr(st, "scheduler", None)
            if sched is not None and hasattr(sched, "stop"):
                await sched.stop()
        except Exception:
            pass

        # Cancel maintenance tasks.
        tasks = [t for t in (st.maintenance_tasks or []) if isinstance(t, asyncio.Task)]
        for t in tasks:
            try:
                t.cancel()
            except Exception:
                pass
        await asyncio.gather(*tasks, return_exceptions=True)

        # Stop thread-based services in a thread to avoid sync adapters on loop thread.
        try:
            if st.ddp is not None:
                await asyncio.to_thread(st.ddp.stop)
        except Exception:
            pass
        try:
            if st.sequences is not None:
                await asyncio.to_thread(st.sequences.stop)
        except Exception:
            pass
        try:
            if st.fleet_sequences is not None:
                await asyncio.to_thread(st.fleet_sequences.stop)
        except Exception:
            pass

        # Close DB + HTTP client.
        try:
            if st.db is not None:
                await st.db.close()
        except Exception:
            pass
        try:
            if st.peer_http is not None:
                await st.peer_http.aclose()
        except Exception:
            pass

    if app is not None:
        try:
            delattr(app.state, "wsa")
        except Exception:
            pass

    async with _STATE_LOCK:
        _STATE_INITIALIZED = False
