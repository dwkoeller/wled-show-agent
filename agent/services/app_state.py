from __future__ import annotations

import asyncio
import inspect
import os
import time
from typing import Any, Dict, Optional

import httpx
from fastapi import FastAPI

from config import load_settings
from config.constants import APP_VERSION, SERVICE_NAME
from jobs import AsyncJobManager
from look_service import LookService
from pack_io import ensure_dir
from preset_importer import PresetImporter
from rate_limiter import AsyncCooldown
from ddp_sender import DDPConfig
from ddp_streamer import DDPStreamer
from geometry import TreeGeometry
from sequence_service import SequenceService
from fleet_sequence_service import FleetSequenceService
from services import a2a_service, fleet_service, metrics_service
from services.a2a_peers_service import parse_a2a_peers
from services.blocking_service import BlockingService, ProcessService
from services.director_service import create_director
from services.events_service import EventBus, flush_event_spool
from services.scheduler_async import AsyncSchedulerService
from services.state import AppState
from utils.outbound_http import retry_policy_from_settings
from wled_client import AsyncWLEDClient
from wled_mapper import WLEDMapper


_STATE_LOCK = asyncio.Lock()
_DEFAULT_STATE: AppState | None = None


async def startup(app: FastAPI | None = None) -> None:
    global _DEFAULT_STATE
    async with _STATE_LOCK:
        if app is not None:
            if getattr(app.state, "wsa", None) is not None:
                return
        else:
            if _DEFAULT_STATE is not None:
                return

        settings = load_settings()
        ensure_dir(settings.data_dir)
        started_at = time.time()
        loop = asyncio.get_running_loop()

        blocking = BlockingService(
            max_workers=settings.blocking_max_workers,
            max_queue=settings.blocking_max_queue,
            acquire_timeout_s=settings.blocking_queue_timeout_s,
        )
        ddp_blocking = BlockingService(
            max_workers=settings.ddp_blocking_max_workers,
            max_queue=settings.ddp_blocking_max_queue,
            acquire_timeout_s=settings.ddp_blocking_queue_timeout_s,
        )
        cpu_pool = ProcessService(
            max_workers=settings.cpu_pool_max_workers,
            max_queue=settings.cpu_pool_max_queue,
            acquire_timeout_s=settings.cpu_pool_queue_timeout_s,
        )

        # Shared async HTTP client for all outbound calls (WLED + peers).
        peer_http = httpx.AsyncClient(
            follow_redirects=True,
            headers={"User-Agent": f"{SERVICE_NAME}/{APP_VERSION}"},
            limits=httpx.Limits(max_connections=32, max_keepalive_connections=16),
        )

        # Async DB service (required).
        from services.db_service import DatabaseService

        db = DatabaseService(
            database_url=settings.database_url,
            agent_id=settings.agent_id,
            migrate_on_startup=bool(getattr(settings, "db_migrate_on_startup", True)),
        )
        try:
            await db.init()
        except Exception as e:
            try:
                await peer_http.aclose()
            except Exception:
                pass
            raise RuntimeError(f"Database init failed: {e}") from e

        # Seed auth users (primary + optional extras).
        try:
            from services.auth_service import normalize_password_hash, _normalize_ip_allowlist, VALID_ROLES

            default_pwd = normalize_password_hash(str(settings.auth_password or ""))
            if default_pwd and settings.auth_totp_secret:
                await db.ensure_auth_user(
                    username=str(settings.auth_username or "").strip(),
                    password_hash=default_pwd,
                    totp_secret=str(settings.auth_totp_secret or "").strip(),
                    role=(
                        str(settings.auth_user_role or "admin")
                        if str(settings.auth_user_role or "admin") in VALID_ROLES
                        else "admin"
                    ),
                    disabled=False,
                )
            for raw in settings.auth_users or []:
                if not isinstance(raw, dict):
                    continue
                uname = str(raw.get("username") or "").strip()
                if not uname:
                    continue
                pwd_raw = raw.get("password_hash") or raw.get("password")
                totp = str(raw.get("totp_secret") or "").strip()
                if not pwd_raw or not totp:
                    continue
                role = str(raw.get("role") or "user")
                if role not in VALID_ROLES:
                    role = "user"
                await db.ensure_auth_user(
                    username=uname,
                    password_hash=normalize_password_hash(str(pwd_raw)),
                    totp_secret=totp,
                    role=role,
                    disabled=bool(raw.get("disabled") or False),
                    ip_allowlist=_normalize_ip_allowlist(raw.get("ip_allowlist")),
                )
        except Exception as e:
            raise RuntimeError(f"Auth user seed failed: {e}") from e

        # Jobs (async core loop).
        jobs = AsyncJobManager(
            loop=loop,
            max_jobs=settings.job_max_jobs,
            queue_size=settings.job_queue_size,
            worker_count=settings.job_worker_count,
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
            retry=retry_policy_from_settings(settings),
        )
        wled_mapper = WLEDMapper()

        # Segment IDs: if not configured, try auto-detect; fall back to [0].
        segment_ids = list(settings.wled_segment_ids)
        if not segment_ids:
            try:
                segment_ids = await wled.get_segment_ids(refresh=True)
            except Exception:
                segment_ids = []
        if not segment_ids:
            segment_ids = [0]

        # Seed effect/palette maps for async look application (avoid sync adapter use on loop).
        try:
            effects = await wled.get_effects(refresh=True)
            palettes = await wled.get_palettes(refresh=True)
            wled_mapper.seed(effects=list(effects), palettes=list(palettes))
        except Exception:
            pass

        # Cooldown shared across async + thread services.
        wled_cooldown = AsyncCooldown(settings.wled_command_cooldown_ms)
        looks = LookService(
            wled=wled,
            mapper=wled_mapper,
            data_dir=settings.data_dir,
            max_bri=settings.wled_max_bri,
            segment_ids=segment_ids,
            replicate_to_all_segments=settings.wled_replicate_to_all_segments,
            blocking=blocking,
            cpu_pool=cpu_pool,
        )
        importer = PresetImporter(
            wled=wled,
            mapper=wled_mapper,
            cooldown=wled_cooldown,
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
            wled=wled,
            geometry=geom,
            ddp_cfg=ddp_cfg,
            fps_default=settings.ddp_fps_default,
            fps_max=settings.ddp_fps_max,
            drop_late_frames=settings.ddp_drop_late_frames,
            max_lag_s=settings.ddp_backpressure_max_lag_s,
            segment_ids=segment_ids,
            blocking=ddp_blocking,
            cpu_pool=cpu_pool if settings.ddp_use_cpu_pool else None,
        )
        sequences = SequenceService(
            wled=wled,
            looks=looks,
            ddp=ddp,
            data_dir=settings.data_dir,
            blocking=blocking,
            cpu_pool=cpu_pool,
        )

        peers = parse_a2a_peers(list(settings.a2a_peers))

        st = AppState(
            settings=settings,
            started_at=started_at,
            segment_ids=segment_ids,
            wled_cooldown=wled_cooldown,
            wled=wled,
            wled_mapper=wled_mapper,
            looks=looks,
            importer=importer,
            ddp=ddp,
            sequences=sequences,
            fleet_sequences=None,
            orchestrator=None,
            director=None,
            runtime_state_path=os.path.join(
                settings.data_dir, "state", "runtime_state.json"
            ),
            kv_runtime_state_key="runtime_state",
            db=db,
            jobs=jobs,
            scheduler=None,
            peers=dict(peers),
            blocking=blocking,
            ddp_blocking=ddp_blocking,
            cpu_pool=cpu_pool,
            peer_http=peer_http,
            events=EventBus(),
            loop=loop,
            maintenance_tasks=[],
        )
        st.reconcile_cancel_event = asyncio.Event()

        try:
            from services.events_service import emit_event

            async def _emit_job_event(event_type: str, data: Dict[str, Any]) -> None:
                await emit_event(st, event_type=event_type, data=data)

            if hasattr(jobs, "set_event_callback"):
                jobs.set_event_callback(_emit_job_event)
        except Exception:
            pass

        try:
            from services.orchestration_service import OrchestrationService

            st.orchestrator = OrchestrationService(state=st)
        except Exception:
            st.orchestrator = None
        try:
            from services.fleet_orchestration_service import FleetOrchestrationService

            st.fleet_orchestrator = FleetOrchestrationService(state=st)
        except Exception:
            st.fleet_orchestrator = None

        # Fleet sequence coordinator (optional; safe when peers is empty too).
        try:

            async def _local_invoke(action: str, params: Dict[str, Any]) -> Any:
                fn = a2a_service.actions().get(str(action))
                if fn is None:
                    raise RuntimeError(f"Unknown local action '{action}'")
                return await fn(st, dict(params or {}))

            async def _peer_supported_actions(peer: Any, timeout_s: float) -> set[str]:
                return await fleet_service._peer_supported_actions(  # type: ignore[attr-defined]
                    state=st,
                    peer=peer,
                    timeout_s=float(timeout_s),
                )

            async def _peer_invoke(
                peer: Any, action: str, params: Dict[str, Any], timeout_s: float
            ) -> Dict[str, Any]:
                payload = {"action": str(action), "params": dict(params or {})}
                return await fleet_service._peer_post_json(  # type: ignore[attr-defined]
                    state=st,
                    peer=peer,
                    path="/v1/a2a/invoke",
                    payload=payload,
                    timeout_s=float(timeout_s),
                )

            async def _peer_resolver(
                targets: list[str] | None, timeout_s: float
            ) -> list[Any]:
                _ = timeout_s
                tgt = [str(x) for x in (targets or []) if str(x).strip()] or None
                return await fleet_service._select_peers(st, tgt)  # type: ignore[attr-defined]

            st.fleet_sequences = FleetSequenceService(
                data_dir=settings.data_dir,
                peers=dict(peers),
                local_invoke=_local_invoke,
                peer_invoke=_peer_invoke,
                peer_supported_actions=_peer_supported_actions,
                peer_resolver=_peer_resolver,
                default_timeout_s=float(settings.a2a_http_timeout_s),
            )
        except Exception:
            st.fleet_sequences = None

        # Optional OpenAI director (async; uses OpenAI tool-calling).
        try:
            st.director = create_director(state=st)
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

        # Optional MQTT automation bridge.
        if settings.mqtt_enabled and settings.mqtt_url:
            try:
                from services.mqtt_service import MQTTBridge

                st.mqtt = MQTTBridge(state=st)
                await st.mqtt.start()
            except Exception:
                st.mqtt = None

        # Fleet heartbeat: publish agent presence + capabilities into SQL (no fanout).
        if db is not None:
            hb_interval_s = max(
                5.0, float(os.environ.get("AGENT_HEARTBEAT_INTERVAL_S") or 10.0)
            )
            history_interval_s = float(getattr(settings, "agent_history_interval_s", 300))
            if history_interval_s > 0:
                history_interval_s = max(30.0, history_interval_s)
            last_history_at = 0.0

            async def _heartbeat_loop() -> None:
                nonlocal last_history_at
                while True:
                    try:
                        payload: Dict[str, Any] = {
                            "capabilities": [
                                str(c.get("action"))
                                for c in (a2a_service.CAPABILITIES or [])
                                if isinstance(c, dict) and c.get("action")
                            ],
                            "peers_configured": sorted(
                                [str(k) for k in (st.peers or {}).keys()]
                            ),
                            "ui_enabled": bool(settings.ui_enabled),
                            "auth_enabled": bool(settings.auth_enabled),
                            "openai_enabled": bool(settings.openai_api_key),
                            "fpp_enabled": bool(settings.fpp_base_url),
                            "ledfx_enabled": bool(settings.ledfx_base_url),
                        }
                        if settings.agent_base_url:
                            payload["base_url"] = str(settings.agent_base_url)
                        if settings.agent_tags:
                            payload["tags"] = [str(x) for x in settings.agent_tags]
                        try:
                            payload["status"] = await a2a_service.actions()["status"](
                                st, {}
                            )
                        except Exception:
                            pass
                        try:
                            sched = getattr(st, "scheduler", None)
                            if sched is not None and hasattr(sched, "status"):
                                payload["scheduler"] = await sched.status()
                        except Exception:
                            pass

                        await db.upsert_agent_heartbeat(
                            agent_id=str(settings.agent_id),
                            started_at=float(st.started_at),
                            name=str(settings.agent_name),
                            role=str(settings.agent_role),
                            controller_kind=str(settings.controller_kind),
                            version=str(APP_VERSION),
                            payload=payload,
                        )
                        now = time.time()
                        if history_interval_s > 0 and (
                            now - float(last_history_at) >= float(history_interval_s)
                        ):
                            try:
                                await db.add_agent_heartbeat_history(
                                    agent_id=str(settings.agent_id),
                                    updated_at=now,
                                    name=str(settings.agent_name),
                                    role=str(settings.agent_role),
                                    controller_kind=str(settings.controller_kind),
                                    version=str(APP_VERSION),
                                    base_url=str(settings.agent_base_url or "") or None,
                                    payload=payload,
                                )
                                last_history_at = now
                            except Exception:
                                pass
                    except Exception:
                        pass
                    await asyncio.sleep(float(hb_interval_s))

            st.maintenance_tasks.append(
                asyncio.create_task(_heartbeat_loop(), name="db_agent_heartbeat")
            )

            # Fleet history aggregation: snapshot all agent heartbeats into history (lease-backed).
            if history_interval_s > 0:
                agg_interval_s = float(history_interval_s)
                agg_lease_key = "wsa:maintenance:fleet_history_snapshot"
                agg_lease_ttl = max(120.0, agg_interval_s * 2.0)

                async def _fleet_history_loop() -> None:
                    while True:
                        try:
                            acquired = await db.try_acquire_lease(
                                key=str(agg_lease_key),
                                owner_id=str(settings.agent_id),
                                ttl_s=float(agg_lease_ttl),
                            )
                            if acquired:
                                rows = await db.list_agent_heartbeats(limit=2000)
                                agent_ids = [
                                    str(r.get("agent_id") or "").strip()
                                    for r in rows
                                    if isinstance(r, dict)
                                ]
                                last_map = await db.get_latest_agent_heartbeat_history_map(
                                    agent_ids=[a for a in agent_ids if a]
                                )
                                now = time.time()
                                for r in rows:
                                    if not isinstance(r, dict):
                                        continue
                                    aid = str(r.get("agent_id") or "").strip()
                                    if not aid:
                                        continue
                                    last_ts = float(last_map.get(aid) or 0.0)
                                    if last_ts and (now - last_ts) < agg_interval_s:
                                        continue
                                    payload = r.get("payload") or {}
                                    if not isinstance(payload, dict):
                                        payload = {}
                                    base_url = str(payload.get("base_url") or "").strip()
                                    await db.add_agent_heartbeat_history(
                                        agent_id=aid,
                                        updated_at=float(r.get("updated_at") or now),
                                        name=str(r.get("name") or ""),
                                        role=str(r.get("role") or ""),
                                        controller_kind=str(
                                            r.get("controller_kind") or ""
                                        ),
                                        version=str(r.get("version") or ""),
                                        base_url=base_url or None,
                                        payload=dict(payload),
                                    )
                        except Exception:
                            pass
                        await asyncio.sleep(max(30.0, agg_interval_s))

                st.maintenance_tasks.append(
                    asyncio.create_task(
                        _fleet_history_loop(),
                        name="db_fleet_history_snapshot",
                    )
                )

                async def _fleet_history_backfill() -> None:
                    try:
                        acquired = await db.try_acquire_lease(
                            key="wsa:maintenance:fleet_history_backfill",
                            owner_id=str(settings.agent_id),
                            ttl_s=300.0,
                        )
                        if not acquired:
                            return
                        rows = await db.list_agent_heartbeats(limit=2000)
                        agent_ids = [
                            str(r.get("agent_id") or "").strip()
                            for r in rows
                            if isinstance(r, dict)
                        ]
                        last_map = await db.get_latest_agent_heartbeat_history_map(
                            agent_ids=[a for a in agent_ids if a]
                        )
                        now = time.time()
                        for r in rows:
                            if not isinstance(r, dict):
                                continue
                            aid = str(r.get("agent_id") or "").strip()
                            if not aid:
                                continue
                            last_ts = float(last_map.get(aid) or 0.0)
                            hb_updated = float(r.get("updated_at") or now)
                            if last_ts and (hb_updated - last_ts) <= history_interval_s:
                                continue
                            payload = r.get("payload") or {}
                            if not isinstance(payload, dict):
                                payload = {}
                            base_url = str(payload.get("base_url") or "").strip()
                            await db.add_agent_heartbeat_history(
                                agent_id=aid,
                                created_at=hb_updated,
                                updated_at=float(r.get("updated_at") or now),
                                name=str(r.get("name") or ""),
                                role=str(r.get("role") or ""),
                                controller_kind=str(r.get("controller_kind") or ""),
                                version=str(r.get("version") or ""),
                                base_url=base_url or None,
                                payload=dict(payload),
                            )
                    except Exception:
                        return

                st.maintenance_tasks.append(
                    asyncio.create_task(
                        _fleet_history_backfill(),
                        name="db_fleet_history_backfill",
                    )
                )

                async def _fleet_history_tag_backfill_loop() -> None:
                    interval_s = float(
                        settings.agent_history_maintenance_interval_s or 3600
                    )
                    lease_key = "wsa:maintenance:fleet_history_tag_backfill"
                    lease_ttl_s = max(120.0, interval_s * 2.0)
                    while True:
                        inserted = 0
                        try:
                            acquired = await db.try_acquire_lease(
                                key=str(lease_key),
                                owner_id=str(settings.agent_id),
                                ttl_s=float(lease_ttl_s),
                            )
                            if acquired:
                                inserted = await db.backfill_agent_heartbeat_history_tags(
                                    limit=2000
                                )
                        except Exception:
                            inserted = 0
                        if inserted > 0:
                            await asyncio.sleep(5.0)
                        else:
                            await asyncio.sleep(max(30.0, interval_s))

                st.maintenance_tasks.append(
                    asyncio.create_task(
                        _fleet_history_tag_backfill_loop(),
                        name="db_fleet_history_tag_backfill",
                    )
                )

        # DB maintenance: job retention.
        if db is not None and (
            settings.job_history_max_rows > 0 or settings.job_history_max_days > 0
        ):
            interval_s = float(settings.job_history_maintenance_interval_s or 3600)

            async def _retention_loop() -> None:
                while True:
                    try:
                        result = await db.enforce_job_retention(
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
                        st.job_retention_last = {
                            "at": time.time(),
                            "result": result,
                        }
                    except Exception:
                        pass
                    await asyncio.sleep(max(30.0, interval_s))

            st.maintenance_tasks.append(
                asyncio.create_task(_retention_loop(), name="db_job_retention")
            )

        # DB maintenance: metrics history sampling.
        if db is not None and settings.metrics_history_interval_s > 0:
            interval_s = float(settings.metrics_history_interval_s or 30)

            async def _metrics_history_loop() -> None:
                while True:
                    try:
                        snapshot = await metrics_service.collect_metrics_snapshot(st)
                        jobs_count = int(
                            (snapshot.get("jobs") or {}).get("count", 0) or 0
                        )
                        scheduler = snapshot.get("scheduler") or {}
                        outbound = snapshot.get("outbound") or {}
                        spool = (snapshot.get("events") or {}).get("spool") or {}
                        await db.add_metrics_sample(
                            created_at=time.time(),
                            jobs_count=jobs_count,
                            scheduler_ok=bool(scheduler.get("ok", True)),
                            scheduler_running=bool(scheduler.get("running", False)),
                            scheduler_in_window=bool(scheduler.get("in_window", False)),
                            outbound_failures=int(
                                outbound.get("failures_total", 0) or 0
                            ),
                            outbound_retries=int(
                                outbound.get("retries_total", 0) or 0
                            ),
                            spool_dropped=int(spool.get("dropped", 0) or 0),
                            spool_queued_events=int(
                                spool.get("queued_events", 0) or 0
                            ),
                            spool_queued_bytes=int(
                                spool.get("queued_bytes", 0) or 0
                            ),
                        )
                    except Exception:
                        pass
                    await asyncio.sleep(max(5.0, interval_s))

            st.maintenance_tasks.append(
                asyncio.create_task(_metrics_history_loop(), name="db_metrics_history")
            )

        # DB maintenance: metrics history retention.
        if db is not None and (
            settings.metrics_history_max_rows > 0 or settings.metrics_history_max_days > 0
        ):
            interval_s = float(
                settings.metrics_history_maintenance_interval_s or 3600
            )

            async def _metrics_history_retention_loop() -> None:
                while True:
                    try:
                        result = await db.enforce_metrics_history_retention(
                            max_rows=(
                                settings.metrics_history_max_rows
                                if settings.metrics_history_max_rows > 0
                                else None
                            ),
                            max_days=(
                                settings.metrics_history_max_days
                                if settings.metrics_history_max_days > 0
                                else None
                            ),
                        )
                        st.metrics_history_retention_last = {
                            "at": time.time(),
                            "result": result,
                        }
                    except Exception:
                        pass
                    await asyncio.sleep(max(30.0, interval_s))

            st.maintenance_tasks.append(
                asyncio.create_task(
                    _metrics_history_retention_loop(),
                    name="db_metrics_history_retention",
                )
            )

        # DB maintenance: scheduler event retention.
        if db is not None and (
            settings.scheduler_events_max_rows > 0
            or settings.scheduler_events_max_days > 0
        ):
            interval_s = float(settings.scheduler_events_maintenance_interval_s or 3600)
            lease_key = "wsa:maintenance:scheduler_events_retention"
            lease_ttl_s = max(120.0, interval_s * 2.0)

            async def _scheduler_events_retention_loop() -> None:
                while True:
                    try:
                        acquired = await db.try_acquire_lease(
                            key=str(lease_key),
                            owner_id=str(settings.agent_id),
                            ttl_s=float(lease_ttl_s),
                        )
                        if acquired:
                            result = await db.enforce_scheduler_events_retention(
                                max_rows=(
                                    settings.scheduler_events_max_rows
                                    if settings.scheduler_events_max_rows > 0
                                    else None
                                ),
                                max_days=(
                                    settings.scheduler_events_max_days
                                    if settings.scheduler_events_max_days > 0
                                    else None
                                ),
                            )
                            st.scheduler_events_retention_last = {
                                "at": time.time(),
                                "result": result,
                            }
                    except Exception:
                        pass
                    await asyncio.sleep(max(30.0, interval_s))

            st.maintenance_tasks.append(
                asyncio.create_task(
                    _scheduler_events_retention_loop(),
                    name="db_scheduler_events_retention",
                )
            )

        # DB maintenance: SQL metadata retention (per-agent).
        if db is not None and (
            settings.pack_ingests_max_rows > 0 or settings.pack_ingests_max_days > 0
        ):
            interval_s = float(settings.pack_ingests_maintenance_interval_s or 3600)

            async def _pack_ingests_retention_loop() -> None:
                while True:
                    try:
                        result = await db.enforce_pack_ingests_retention(
                            max_rows=(
                                settings.pack_ingests_max_rows
                                if settings.pack_ingests_max_rows > 0
                                else None
                            ),
                            max_days=(
                                settings.pack_ingests_max_days
                                if settings.pack_ingests_max_days > 0
                                else None
                            ),
                        )
                        st.pack_ingests_retention_last = {
                            "at": time.time(),
                            "result": result,
                        }
                    except Exception:
                        pass
                    await asyncio.sleep(max(30.0, interval_s))

            st.maintenance_tasks.append(
                asyncio.create_task(
                    _pack_ingests_retention_loop(),
                    name="db_pack_ingests_retention",
                )
            )

        if db is not None and (
            settings.sequence_meta_max_rows > 0 or settings.sequence_meta_max_days > 0
        ):
            interval_s = float(settings.sequence_meta_maintenance_interval_s or 3600)

            async def _sequence_meta_retention_loop() -> None:
                while True:
                    try:
                        result = await db.enforce_sequence_meta_retention(
                            max_rows=(
                                settings.sequence_meta_max_rows
                                if settings.sequence_meta_max_rows > 0
                                else None
                            ),
                            max_days=(
                                settings.sequence_meta_max_days
                                if settings.sequence_meta_max_days > 0
                                else None
                            ),
                        )
                        st.sequence_meta_retention_last = {
                            "at": time.time(),
                            "result": result,
                        }
                    except Exception:
                        pass
                    await asyncio.sleep(max(30.0, interval_s))

            st.maintenance_tasks.append(
                asyncio.create_task(
                    _sequence_meta_retention_loop(),
                    name="db_sequence_meta_retention",
                )
            )

        if db is not None and (
            settings.audio_analyses_max_rows > 0 or settings.audio_analyses_max_days > 0
        ):
            interval_s = float(settings.audio_analyses_maintenance_interval_s or 3600)

            async def _audio_analyses_retention_loop() -> None:
                while True:
                    try:
                        result = await db.enforce_audio_analyses_retention(
                            max_rows=(
                                settings.audio_analyses_max_rows
                                if settings.audio_analyses_max_rows > 0
                                else None
                            ),
                            max_days=(
                                settings.audio_analyses_max_days
                                if settings.audio_analyses_max_days > 0
                                else None
                            ),
                        )
                        st.audio_analyses_retention_last = {
                            "at": time.time(),
                            "result": result,
                        }
                    except Exception:
                        pass
                    await asyncio.sleep(max(30.0, interval_s))

            st.maintenance_tasks.append(
                asyncio.create_task(
                    _audio_analyses_retention_loop(),
                    name="db_audio_analyses_retention",
                )
            )

        if db is not None and (
            settings.show_configs_max_rows > 0 or settings.show_configs_max_days > 0
        ):
            interval_s = float(settings.show_configs_maintenance_interval_s or 3600)

            async def _show_configs_retention_loop() -> None:
                while True:
                    try:
                        result = await db.enforce_show_configs_retention(
                            max_rows=(
                                settings.show_configs_max_rows
                                if settings.show_configs_max_rows > 0
                                else None
                            ),
                            max_days=(
                                settings.show_configs_max_days
                                if settings.show_configs_max_days > 0
                                else None
                            ),
                        )
                        st.show_configs_retention_last = {
                            "at": time.time(),
                            "result": result,
                        }
                    except Exception:
                        pass
                    await asyncio.sleep(max(30.0, interval_s))

            st.maintenance_tasks.append(
                asyncio.create_task(
                    _show_configs_retention_loop(),
                    name="db_show_configs_retention",
                )
            )

        if db is not None and (
            settings.fseq_exports_max_rows > 0 or settings.fseq_exports_max_days > 0
        ):
            interval_s = float(settings.fseq_exports_maintenance_interval_s or 3600)

            async def _fseq_exports_retention_loop() -> None:
                while True:
                    try:
                        result = await db.enforce_fseq_exports_retention(
                            max_rows=(
                                settings.fseq_exports_max_rows
                                if settings.fseq_exports_max_rows > 0
                                else None
                            ),
                            max_days=(
                                settings.fseq_exports_max_days
                                if settings.fseq_exports_max_days > 0
                                else None
                            ),
                        )
                        st.fseq_exports_retention_last = {
                            "at": time.time(),
                            "result": result,
                        }
                    except Exception:
                        pass
                    await asyncio.sleep(max(30.0, interval_s))

            st.maintenance_tasks.append(
                asyncio.create_task(
                    _fseq_exports_retention_loop(),
                    name="db_fseq_exports_retention",
                )
            )

        if db is not None and (
            settings.fpp_scripts_max_rows > 0 or settings.fpp_scripts_max_days > 0
        ):
            interval_s = float(settings.fpp_scripts_maintenance_interval_s or 3600)

            async def _fpp_scripts_retention_loop() -> None:
                while True:
                    try:
                        result = await db.enforce_fpp_scripts_retention(
                            max_rows=(
                                settings.fpp_scripts_max_rows
                                if settings.fpp_scripts_max_rows > 0
                                else None
                            ),
                            max_days=(
                                settings.fpp_scripts_max_days
                                if settings.fpp_scripts_max_days > 0
                                else None
                            ),
                        )
                        st.fpp_scripts_retention_last = {
                            "at": time.time(),
                            "result": result,
                        }
                    except Exception:
                        pass
                    await asyncio.sleep(max(30.0, interval_s))

            st.maintenance_tasks.append(
                asyncio.create_task(
                    _fpp_scripts_retention_loop(),
                    name="db_fpp_scripts_retention",
                )
            )

        # DB maintenance: audit log retention (global).
        if db is not None and (
            settings.audit_log_max_rows > 0 or settings.audit_log_max_days > 0
        ):
            interval_s = float(settings.audit_log_maintenance_interval_s or 3600)
            lease_key = "wsa:maintenance:audit_log_retention"
            lease_ttl_s = max(120.0, interval_s * 2.0)

            async def _audit_log_retention_loop() -> None:
                while True:
                    try:
                        acquired = await db.try_acquire_lease(
                            key=str(lease_key),
                            owner_id=str(settings.agent_id),
                            ttl_s=float(lease_ttl_s),
                        )
                        if acquired:
                            result = await db.enforce_audit_log_retention(
                                max_rows=(
                                    settings.audit_log_max_rows
                                    if settings.audit_log_max_rows > 0
                                    else None
                                ),
                                max_days=(
                                    settings.audit_log_max_days
                                    if settings.audit_log_max_days > 0
                                    else None
                                ),
                            )
                            st.audit_log_retention_last = {
                                "at": time.time(),
                                "result": result,
                            }
                    except Exception:
                        pass
                    await asyncio.sleep(max(30.0, interval_s))

            st.maintenance_tasks.append(
                asyncio.create_task(
                    _audit_log_retention_loop(),
                    name="db_audit_log_retention",
                )
            )

        # DB maintenance: event log retention (global).
        if db is not None and (
            settings.events_history_max_rows > 0 or settings.events_history_max_days > 0
        ):
            interval_s = float(settings.events_history_maintenance_interval_s or 3600)
            lease_key = "wsa:maintenance:event_log_retention"
            lease_ttl_s = max(120.0, interval_s * 2.0)

            async def _event_log_retention_loop() -> None:
                while True:
                    try:
                        acquired = await db.try_acquire_lease(
                            key=str(lease_key),
                            owner_id=str(settings.agent_id),
                            ttl_s=float(lease_ttl_s),
                        )
                        if acquired:
                            result = await db.enforce_event_log_retention(
                                max_rows=(
                                    settings.events_history_max_rows
                                    if settings.events_history_max_rows > 0
                                    else None
                                ),
                                max_days=(
                                    settings.events_history_max_days
                                    if settings.events_history_max_days > 0
                                    else None
                                ),
                            )
                            st.event_log_retention_last = {
                                "at": time.time(),
                                "result": result,
                            }
                    except Exception:
                        pass
                    await asyncio.sleep(max(30.0, interval_s))

            st.maintenance_tasks.append(
                asyncio.create_task(
                    _event_log_retention_loop(),
                    name="db_event_log_retention",
                )
            )

        # DB maintenance: event log spool flush (local).
        if (
            db is not None
            and settings.events_spool_flush_interval_s > 0
            and settings.events_spool_max_mb > 0
        ):
            interval_s = float(settings.events_spool_flush_interval_s or 30)

            async def _event_spool_flush_loop() -> None:
                while True:
                    try:
                        await flush_event_spool(st)
                    except Exception:
                        pass
                    await asyncio.sleep(max(5.0, interval_s))

            st.maintenance_tasks.append(
                asyncio.create_task(
                    _event_spool_flush_loop(),
                    name="event_spool_flush",
                )
            )
            try:
                await flush_event_spool(st)
            except Exception:
                pass

        # DB maintenance: auth sessions + login attempts cleanup.
        if db is not None and settings.auth_session_cleanup_interval_s > 0:
            interval_s = float(settings.auth_session_cleanup_interval_s)
            lease_key = "wsa:maintenance:auth_cleanup"
            lease_ttl_s = max(120.0, interval_s * 2.0)

            async def _auth_cleanup_loop() -> None:
                while True:
                    try:
                        acquired = await db.try_acquire_lease(
                            key=str(lease_key),
                            owner_id=str(settings.agent_id),
                            ttl_s=float(lease_ttl_s),
                        )
                        if acquired:
                            await db.cleanup_auth_sessions(
                                max_age_s=float(settings.auth_session_cleanup_max_age_s)
                            )
                            await db.cleanup_auth_login_attempts(
                                older_than_s=float(settings.auth_login_window_s) * 2.0
                            )
                            await db.cleanup_auth_api_keys(
                                older_than_s=float(
                                    settings.auth_session_cleanup_max_age_s
                                )
                            )
                            await db.cleanup_auth_password_resets(
                                older_than_s=float(settings.auth_login_window_s) * 2.0
                            )
                    except Exception:
                        pass
                    await asyncio.sleep(max(30.0, interval_s))

            st.maintenance_tasks.append(
                asyncio.create_task(_auth_cleanup_loop(), name="db_auth_cleanup")
            )

        # DB maintenance: orchestration run retention (global).
        if db is not None and (
            settings.orchestration_runs_max_rows > 0
            or settings.orchestration_runs_max_days > 0
        ):
            interval_s = float(settings.orchestration_runs_maintenance_interval_s or 3600)
            lease_key = "wsa:maintenance:orchestration_runs_retention"
            lease_ttl_s = max(120.0, interval_s * 2.0)

            async def _orchestration_runs_retention_loop() -> None:
                while True:
                    try:
                        acquired = await db.try_acquire_lease(
                            key=str(lease_key),
                            owner_id=str(settings.agent_id),
                            ttl_s=float(lease_ttl_s),
                        )
                        if acquired:
                            result = await db.enforce_orchestration_runs_retention(
                                max_rows=(
                                    settings.orchestration_runs_max_rows
                                    if settings.orchestration_runs_max_rows > 0
                                    else None
                                ),
                                max_days=(
                                    settings.orchestration_runs_max_days
                                    if settings.orchestration_runs_max_days > 0
                                    else None
                                ),
                            )
                            st.orchestration_runs_retention_last = {
                                "at": time.time(),
                                "result": result,
                            }
                    except Exception:
                        pass
                    await asyncio.sleep(max(30.0, interval_s))

            st.maintenance_tasks.append(
                asyncio.create_task(
                    _orchestration_runs_retention_loop(),
                    name="db_orchestration_runs_retention",
                )
            )

        # DB maintenance: fleet history retention (global).
        if db is not None and (
            settings.agent_history_max_rows > 0 or settings.agent_history_max_days > 0
        ):
            interval_s = float(settings.agent_history_maintenance_interval_s or 3600)
            lease_key = "wsa:maintenance:agent_history_retention"
            lease_ttl_s = max(120.0, interval_s * 2.0)

            async def _agent_history_retention_loop() -> None:
                while True:
                    try:
                        acquired = await db.try_acquire_lease(
                            key=str(lease_key),
                            owner_id=str(settings.agent_id),
                            ttl_s=float(lease_ttl_s),
                        )
                        if acquired:
                            result = await db.enforce_agent_heartbeat_history_retention(
                                max_rows=(
                                    settings.agent_history_max_rows
                                    if settings.agent_history_max_rows > 0
                                    else None
                                ),
                                max_days=(
                                    settings.agent_history_max_days
                                    if settings.agent_history_max_days > 0
                                    else None
                                ),
                            )
                            st.agent_history_retention_last = {
                                "at": time.time(),
                                "result": result,
                            }
                    except Exception:
                        pass
                    await asyncio.sleep(max(30.0, interval_s))

            st.maintenance_tasks.append(
                asyncio.create_task(
                    _agent_history_retention_loop(),
                    name="db_agent_history_retention",
                )
            )

        # Optional DB reconcile on startup.
        if db is not None and settings.db_reconcile_on_startup:
            try:
                from services.reconcile_service import run_reconcile_with_status

                st.maintenance_tasks.append(
                    asyncio.create_task(
                        run_reconcile_with_status(
                            st,
                            mode="startup",
                            packs=True,
                            sequences=True,
                            audio=bool(settings.db_reconcile_include_audio),
                            show_configs=True,
                            fseq_exports=True,
                            fpp_scripts=True,
                            scan_limit=int(settings.db_reconcile_scan_limit),
                            precompute_previews=bool(
                                settings.precompute_previews_on_reconcile
                            ),
                            precompute_waveforms=bool(
                                settings.precompute_waveforms_on_reconcile
                            ),
                        ),
                        name="db_reconcile_data_dir",
                    )
                )
            except Exception:
                pass

        # Periodic DB reconcile (optional; guarded by a DB lease).
        if db is not None and settings.db_reconcile_interval_s > 0:
            interval_s = float(settings.db_reconcile_interval_s)
            lease_key = "wsa:maintenance:reconcile_data_dir"
            lease_ttl_s = max(120.0, interval_s * 2.0)

            async def _reconcile_loop() -> None:
                from services.reconcile_service import run_reconcile_with_status

                while True:
                    try:
                        acquired = await db.try_acquire_lease(
                            key=str(lease_key),
                            owner_id=str(settings.agent_id),
                            ttl_s=float(lease_ttl_s),
                        )
                        if acquired:
                            await run_reconcile_with_status(
                                st,
                                mode="scheduled",
                                packs=True,
                                sequences=True,
                                audio=bool(settings.db_reconcile_include_audio),
                                show_configs=True,
                                fseq_exports=True,
                                fpp_scripts=True,
                                scan_limit=int(settings.db_reconcile_scan_limit),
                                precompute_previews=bool(
                                    settings.precompute_previews_on_reconcile
                                ),
                                precompute_waveforms=bool(
                                    settings.precompute_waveforms_on_reconcile
                                ),
                            )
                    except Exception:
                        pass
                    await asyncio.sleep(max(60.0, interval_s))

            st.maintenance_tasks.append(
                asyncio.create_task(
                    _reconcile_loop(),
                    name="db_reconcile_data_dir_interval",
                )
            )

        if app is not None:
            app.state.wsa = st  # type: ignore[attr-defined]
        else:
            _DEFAULT_STATE = st


async def shutdown(app: FastAPI | None = None) -> None:
    global _DEFAULT_STATE

    st: AppState | None = None
    if app is not None:
        st = getattr(app.state, "wsa", None)
    else:
        st = _DEFAULT_STATE

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

        # Stop background services.
        try:
            if getattr(st, "orchestrator", None) is not None:
                await st.orchestrator.stop()
        except Exception:
            pass
        try:
            if getattr(st, "fleet_orchestrator", None) is not None:
                await st.fleet_orchestrator.stop()
        except Exception:
            pass
        try:
            if st.ddp is not None:
                await st.ddp.stop()
        except Exception:
            pass
        try:
            if getattr(st, "blocking", None) is not None:
                await st.blocking.shutdown()
        except Exception:
            pass
        try:
            if getattr(st, "ddp_blocking", None) is not None:
                await st.ddp_blocking.shutdown()
        except Exception:
            pass
        try:
            if getattr(st, "cpu_pool", None) is not None:
                await st.cpu_pool.shutdown()
        except Exception:
            pass
        try:
            if st.sequences is not None:
                await st.sequences.stop()
        except Exception:
            pass
        try:
            if st.fleet_sequences is not None:
                await st.fleet_sequences.stop()
        except Exception:
            pass
        try:
            if getattr(st, "mqtt", None) is not None:
                await st.mqtt.stop()
        except Exception:
            pass
        try:
            if getattr(st, "director", None) is not None:
                close = getattr(st.director, "close", None)
                if callable(close):
                    res = close()
                    if inspect.isawaitable(res):
                        await res
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
    else:
        _DEFAULT_STATE = None
