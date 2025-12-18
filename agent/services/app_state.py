from __future__ import annotations

import os
import shutil
import math
import re
import asyncio
import queue
import datetime
import threading
import time
import uuid
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Dict, List, Optional

import httpx
from fastapi import Depends, FastAPI, Header, HTTPException, Request, Response
from fastapi.responses import (
    FileResponse,
    JSONResponse,
    StreamingResponse,
)
from pydantic import BaseModel, Field

from config.constants import APP_VERSION, SERVICE_NAME

from models.requests import (
    A2AInvokeRequest,
    ApplyRandomLookRequest,
    ApplyStateRequest,
    AudioAnalyzeRequest,
    AuthLoginRequest,
    CommandRequest,
    DDPStartRequest,
    FPPExportFleetSequenceScriptRequest,
    FPPExportFleetStopAllScriptRequest,
    FPPProxyRequest,
    FPPStartPlaylistRequest,
    FPPTriggerEventRequest,
    FPPUploadFileRequest,
    FSEQExportRequest,
    FleetApplyRandomLookRequest,
    FleetInvokeRequest,
    FleetSequenceStartRequest,
    FleetStopAllRequest,
    GenerateLooksRequest,
    GenerateSequenceRequest,
    GoCrazyRequest,
    PlaySequenceRequest,
    ShowConfigLoadRequest,
    XlightsImportNetworksRequest,
    XlightsImportProjectRequest,
    XlightsImportSequenceRequest,
)

from auth import (
    AuthError,
    jwt_decode_hs256,
    jwt_encode_hs256,
    totp_verify,
    verify_password,
)
from audio_analyzer import AudioAnalyzeError, analyze_beats
from config import Settings, load_settings
from ddp_control import prepare_ddp_params
from orientation import infer_orientation, OrientationInfo
from ddp_sender import DDPConfig
from ddp_streamer import DDPStreamer
from geometry import TreeGeometry
from look_service import LookService
from pack_io import ensure_dir, read_json, read_jsonl, write_json
from preset_importer import PresetImporter
from rate_limiter import Cooldown
from sequence_service import SequenceService
from fleet_sequence_service import FleetSequenceService
from wled_client import WLEDClient, WLEDError
from wled_mapper import WLEDMapper
from fpp_client import FPPClient, FPPError
from fpp_export import render_http_post_script, write_script
from fseq import FSEQError, write_fseq_v1_file
from jobs import JobManager, JobCanceled, jobs_snapshot_payload, sse_format_event
from show_config import ShowConfig, load_show_config, write_show_config
from xlights_import import (
    import_xlights_models_file,
    import_xlights_networks_file,
    show_config_from_xlights_networks,
    show_config_from_xlights_project,
)
from xlights_sequence_import import (
    XlightsSequenceImportError,
    import_xlights_xsq_timing_file,
)

try:
    from openai_agent import SimpleDirectorAgent
except Exception:
    SimpleDirectorAgent = None  # type: ignore


# ----------------------------
# Runtime state (initialized in startup())
# ----------------------------
SETTINGS: Settings | None = None

DB_ENGINE = None
ASYNC_DB = None
JOB_STORE = None
KV_STORE = None

JOBS: JobManager | None = None
APP_STARTED_AT: float = 0.0
_RUNTIME_STATE_PATH: str = ""
_KV_RUNTIME_STATE_KEY = "runtime_state"

WLED: WLEDClient | None = None
MAPPER: WLEDMapper | None = None
SEGMENT_IDS: List[int] = []

LOOKS: LookService | None = None
COOLDOWN: Cooldown | None = None
IMPORTER: PresetImporter | None = None
GEOM: TreeGeometry | None = None
DDP_CFG: DDPConfig | None = None
DDP: DDPStreamer | None = None
SEQUENCES: SequenceService | None = None

FPP: FPPClient | None = None
FLEET_SEQUENCES: FleetSequenceService | None = None

DIRECTOR = None

_STATE_LOCK = threading.Lock()
_STATE_INITIALIZED = False

_PEER_HTTP: httpx.Client | None = None
_PEER_HTTP_LOCK = threading.Lock()


def _require_settings() -> Settings:
    settings = SETTINGS
    if settings is None:
        raise HTTPException(status_code=503, detail="Service not initialized")
    return settings


def startup(app: FastAPI | None = None) -> None:
    """
    Initialize all global singletons. This is called from FastAPI lifespan so the
    module can be imported without requiring runtime env vars (e.g. WLED_TREE_URL).
    """
    global SETTINGS
    global DB_ENGINE, ASYNC_DB, JOB_STORE, KV_STORE
    global JOBS, APP_STARTED_AT, _RUNTIME_STATE_PATH
    global WLED, MAPPER, SEGMENT_IDS
    global PEERS
    global LOOKS, COOLDOWN, IMPORTER, GEOM, DDP_CFG, DDP, SEQUENCES
    global FPP, FLEET_SEQUENCES
    global DIRECTOR
    global _STATE_INITIALIZED

    with _STATE_LOCK:
        if _STATE_INITIALIZED:
            return

        settings = load_settings()
        SETTINGS = settings
        ensure_dir(settings.data_dir)

        # Database (optional). Best-effort: never block startup if DB is unavailable.
        DB_ENGINE = None
        ASYNC_DB = None
        JOB_STORE = None
        KV_STORE = None
        if settings.database_url:
            try:
                # Prefer async DB drivers (aiomysql/aiosqlite) when available; fall back to
                # sync (pymysql/sqlite3) so DB issues never prevent startup.
                from sql_store_async import AsyncSQLDatabase

                ASYNC_DB = AsyncSQLDatabase(
                    database_url=settings.database_url,
                    agent_id=settings.agent_id,
                )
                JOB_STORE = ASYNC_DB.job_store
                KV_STORE = ASYNC_DB.kv_store
            except Exception:
                ASYNC_DB = None
                try:
                    from sql_store import (
                        SQLJobStore,
                        SQLKVStore,
                        create_db_engine,
                        init_db,
                    )

                    DB_ENGINE = create_db_engine(settings.database_url)
                    init_db(DB_ENGINE)
                    JOB_STORE = SQLJobStore(
                        engine=DB_ENGINE, agent_id=settings.agent_id
                    )
                    KV_STORE = SQLKVStore(engine=DB_ENGINE, agent_id=settings.agent_id)
                except Exception:
                    DB_ENGINE = None
                    JOB_STORE = None
                    KV_STORE = None

        JOBS = JobManager(
            persist_path=os.path.join(settings.data_dir, "jobs", "jobs.json"),
            store=JOB_STORE,
        )
        APP_STARTED_AT = time.time()
        _RUNTIME_STATE_PATH = os.path.join(
            settings.data_dir, "state", "runtime_state.json"
        )

        if settings.controller_kind != "wled":
            raise RuntimeError(
                "This app controls WLED devices. For ESPixelStick pixel controllers, run pixel_main:app (CONTROLLER_KIND=pixel)."
            )

        WLED = WLEDClient(
            settings.wled_tree_url, timeout_s=settings.wled_http_timeout_s
        )
        MAPPER = WLEDMapper(WLED)

        # Segment IDs (WLED 0.15+ returns segment list in /json/state). If not configured,
        # we attempt to auto-detect from WLED and fall back to [0] if offline.
        seg_ids: List[int] = list(settings.wled_segment_ids)
        if not seg_ids:
            try:
                seg_ids = WLED.get_segment_ids(refresh=True)
            except Exception:
                seg_ids = []
        if not seg_ids:
            seg_ids = [0]
        SEGMENT_IDS = seg_ids

        PEERS = _parse_a2a_peers(list(settings.a2a_peers))

        LOOKS = LookService(
            wled=WLED,
            mapper=MAPPER,
            data_dir=settings.data_dir,
            max_bri=settings.wled_max_bri,
            segment_ids=SEGMENT_IDS,
            replicate_to_all_segments=settings.wled_replicate_to_all_segments,
        )
        COOLDOWN = Cooldown(settings.wled_command_cooldown_ms)
        IMPORTER = PresetImporter(
            wled=WLED,
            mapper=MAPPER,
            cooldown=COOLDOWN,
            max_bri=settings.wled_max_bri,
            segment_ids=SEGMENT_IDS,
            replicate_to_all_segments=settings.wled_replicate_to_all_segments,
        )
        GEOM = TreeGeometry(
            runs=settings.tree_runs,
            pixels_per_run=settings.tree_pixels_per_run,
            segment_len=settings.tree_segment_len,
            segments_per_run=settings.tree_segments_per_run,
        )
        DDP_CFG = DDPConfig(
            host=settings.ddp_host,
            port=settings.ddp_port,
            destination_id=settings.ddp_destination_id,
            max_pixels_per_packet=settings.ddp_max_pixels_per_packet,
        )
        DDP = DDPStreamer(
            wled=WLED,
            geometry=GEOM,
            ddp_cfg=DDP_CFG,
            fps_default=settings.ddp_fps_default,
            fps_max=settings.ddp_fps_max,
            segment_ids=SEGMENT_IDS,
        )
        SEQUENCES = SequenceService(
            wled=WLED, looks=LOOKS, ddp=DDP, data_dir=settings.data_dir
        )

        FPP = None
        if settings.fpp_base_url:
            FPP = FPPClient(
                base_url=settings.fpp_base_url,
                timeout_s=settings.fpp_http_timeout_s,
                headers={k: v for (k, v) in settings.fpp_headers},
            )

        # Fleet sequence coordinator (optional; safe when peers is empty).
        try:
            FLEET_SEQUENCES = FleetSequenceService(
                data_dir=settings.data_dir,
                peers=PEERS,
                local_invoke=_fleet_local_invoke,
                peer_invoke=_fleet_peer_invoke,
                peer_supported_actions=_peer_supported_actions,
                default_timeout_s=float(settings.a2a_http_timeout_s),
            )
        except Exception:
            FLEET_SEQUENCES = None

        # Optional OpenAI director
        DIRECTOR = None
        try:
            _init_director()
        except Exception:
            DIRECTOR = None

        # Optional scheduler autostart
        try:
            _scheduler_init()
            _scheduler_startup()
        except Exception:
            pass

        if app is not None:
            try:
                app.state.wsa_settings = settings  # type: ignore[attr-defined]
            except Exception:
                pass

        _STATE_INITIALIZED = True


def shutdown() -> None:
    global _STATE_INITIALIZED
    try:
        _scheduler_shutdown()
    except Exception:
        pass
    try:
        if DDP is not None:
            DDP.stop()
    except Exception:
        pass
    try:
        if SEQUENCES is not None:
            SEQUENCES.stop()
    except Exception:
        pass
    try:
        if FLEET_SEQUENCES is not None:
            FLEET_SEQUENCES.stop()
    except Exception:
        pass
    try:
        _db_shutdown()
    except Exception:
        pass
    try:
        _peer_http_shutdown()
    except Exception:
        pass
    with _STATE_LOCK:
        _STATE_INITIALIZED = False


def _persist_runtime_state(event: str, extra: Optional[Dict[str, Any]] = None) -> None:
    """
    Best-effort runtime state snapshot under DATA_DIR so the UI can show "what was running"
    even after a restart.
    """
    try:
        if not _RUNTIME_STATE_PATH:
            return
        out: Dict[str, Any] = {
            "ok": True,
            "updated_at": time.time(),
            "event": str(event),
            "extra": dict(extra or {}),
        }
        try:
            out["ddp"] = DDP.status().__dict__
        except Exception:
            out["ddp"] = None
        try:
            out["sequence"] = SEQUENCES.status().__dict__
        except Exception:
            out["sequence"] = None
        try:
            if FLEET_SEQUENCES is not None:
                out["fleet_sequence"] = FLEET_SEQUENCES.status().__dict__
        except Exception:
            pass
        try:
            if KV_STORE is not None:
                KV_STORE.set_json(_KV_RUNTIME_STATE_KEY, out)
        except Exception:
            pass
        write_json(_RUNTIME_STATE_PATH, out)
    except Exception:
        return


def _db_shutdown() -> None:
    try:
        if ASYNC_DB is not None:
            ASYNC_DB.shutdown()
    except Exception:
        pass


def _peer_http_client() -> httpx.Client:
    global _PEER_HTTP
    c = _PEER_HTTP
    if c is not None:
        return c
    with _PEER_HTTP_LOCK:
        c2 = _PEER_HTTP
        if c2 is not None:
            return c2
        _PEER_HTTP = httpx.Client(
            follow_redirects=True,
            headers={"User-Agent": f"{SERVICE_NAME}/{APP_VERSION}"},
            limits=httpx.Limits(max_connections=32, max_keepalive_connections=16),
        )
        return _PEER_HTTP


def _peer_http_shutdown() -> None:
    global _PEER_HTTP
    with _PEER_HTTP_LOCK:
        c = _PEER_HTTP
        _PEER_HTTP = None
    if c is None:
        return
    try:
        c.close()
    except Exception:
        return


async def _auth_middleware(request: Request, call_next):  # type: ignore[no-untyped-def]
    settings = SETTINGS
    if settings is None or (not settings.auth_enabled):
        return await call_next(request)

    path = request.url.path or ""
    if (
        path == "/"
        or path.startswith("/ui")
        or path.startswith("/v1/health")
        or path.startswith("/v1/auth/config")
        or path.startswith("/v1/auth/login")
        or path.startswith("/v1/auth/logout")
    ):
        return await call_next(request)

    # Allow preflight requests to proceed.
    if request.method.upper() == "OPTIONS":
        return await call_next(request)

    # Allow either the configured A2A key (if set) or a valid JWT.
    key = settings.a2a_api_key
    if key:
        cand = (request.headers.get("x-a2a-key") or "").strip()
        if cand == key:
            return await call_next(request)
        auth = request.headers.get("authorization") or ""
        parts = auth.strip().split(None, 1)
        if len(parts) == 2 and parts[0].lower() == "bearer" and parts[1].strip() == key:
            return await call_next(request)

    tok = _jwt_from_request(request)
    if tok:
        try:
            jwt_decode_hs256(
                tok,
                secret=str(settings.auth_jwt_secret or ""),
                issuer=str(settings.auth_jwt_issuer or ""),
            )
            return await call_next(request)
        except AuthError:
            pass

    return JSONResponse(status_code=401, content={"ok": False, "error": "unauthorized"})


@dataclass(frozen=True)
class A2APeer:
    name: str
    base_url: str


def _parse_a2a_peers(entries: List[str]) -> Dict[str, A2APeer]:
    peers: Dict[str, A2APeer] = {}
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


PEERS: Dict[str, A2APeer] = {}


def _require_a2a_auth(
    request: Request,
    x_a2a_key: str | None = Header(default=None, alias="X-A2A-Key"),
    authorization: str | None = Header(default=None, alias="Authorization"),
) -> None:
    settings = _require_settings()
    key = settings.a2a_api_key
    candidate = x_a2a_key
    if (not candidate) and authorization:
        parts = str(authorization).strip().split(None, 1)
        if len(parts) == 2 and parts[0].lower() == "bearer":
            candidate = parts[1].strip()

    # If JWT auth is enabled, allow either a valid A2A key (if set) or a valid JWT.
    if settings.auth_enabled:
        if key and candidate == key:
            return
        tok = _jwt_from_request(request)
        if not tok:
            raise HTTPException(status_code=401, detail="Missing token")
        try:
            jwt_decode_hs256(
                tok,
                secret=str(settings.auth_jwt_secret or ""),
                issuer=str(settings.auth_jwt_issuer or ""),
            )
            return
        except AuthError as e:
            raise HTTPException(status_code=401, detail=str(e))

    # Legacy mode: only enforce A2A key if configured.
    if not key:
        return
    if candidate != key:
        raise HTTPException(status_code=401, detail="Missing or invalid A2A key")


def _get_orientation(refresh: bool = False) -> OrientationInfo | None:
    """Best-effort: derive a street-facing orientation mapping for 4-segment quarter trees."""
    settings = SETTINGS
    if settings is None or WLED is None:
        return None
    ordered = list(SEGMENT_IDS)
    try:
        from segment_layout import fetch_segment_layout

        layout = fetch_segment_layout(WLED, segment_ids=SEGMENT_IDS, refresh=refresh)
        if layout and layout.segments:
            ordered = layout.ordered_ids()
    except Exception:
        ordered = list(SEGMENT_IDS)

    if not ordered:
        return None

    try:
        return infer_orientation(
            ordered_segment_ids=[int(x) for x in ordered],
            right_segment_id=int(settings.quad_right_segment_id),
            order_direction_from_street=str(settings.quad_order_from_street),
        )
    except Exception:
        return None


def _init_director() -> None:
    global DIRECTOR
    settings = SETTINGS
    if settings is None or (not settings.openai_api_key) or SimpleDirectorAgent is None:
        DIRECTOR = None
        return

    if LOOKS is None or COOLDOWN is None or DDP is None:
        DIRECTOR = None
        return
    looks = LOOKS
    cooldown = COOLDOWN
    ddp = DDP

    def _tool_apply_random_look(kwargs: Dict[str, Any]) -> Any:
        # If peers are configured, keep devices visually consistent by picking a look_spec locally
        # and broadcasting that exact spec to all peers.
        if PEERS:
            pack, row = looks.choose_random(
                theme=kwargs.get("theme"), seed=kwargs.get("seed")
            )
            bri = kwargs.get("brightness")
            bri_i: Optional[int] = None
            if bri is not None:
                bri_i = min(settings.wled_max_bri, max(1, int(bri)))

            out: Dict[str, Any] = {
                "picked": {
                    "pack_file": pack,
                    "id": row.get("id"),
                    "name": row.get("name"),
                    "theme": row.get("theme"),
                }
            }
            # apply locally
            try:
                cooldown.wait()
                out["self"] = {
                    "ok": True,
                    "result": looks.apply_look(row, brightness_override=bri_i),
                }
            except Exception as e:
                out["self"] = {"ok": False, "error": str(e)}

            # apply to peers
            timeout_s = float(settings.a2a_http_timeout_s)
            eligible: List[A2APeer] = []
            for peer in PEERS.values():
                actions = _peer_supported_actions(peer, timeout_s=timeout_s)
                if "apply_look_spec" in actions:
                    eligible.append(peer)
                else:
                    out[peer.name] = {
                        "ok": False,
                        "skipped": True,
                        "reason": "Peer does not support apply_look_spec",
                    }

            if eligible:
                payload = {
                    "action": "apply_look_spec",
                    "params": {"look_spec": row, "brightness_override": bri_i},
                }
                with ThreadPoolExecutor(max_workers=min(8, len(eligible))) as ex:
                    futs = {
                        ex.submit(
                            _peer_post,
                            peer,
                            "/v1/a2a/invoke",
                            payload,
                            timeout_s=timeout_s,
                        ): peer
                        for peer in eligible
                    }
                    for fut in as_completed(futs):
                        peer = futs[fut]
                        try:
                            out[peer.name] = fut.result()
                        except Exception as e:
                            out[peer.name] = {"ok": False, "error": str(e)}
            return out

        return looks.apply_random(
            theme=kwargs.get("theme"),
            brightness=kwargs.get("brightness"),
            seed=kwargs.get("seed"),
        )

    def _tool_start_ddp(kwargs: Dict[str, Any]) -> Any:
        # Allow the model to specify direction/start_pos as top-level kwargs (more reliable)
        params = dict(kwargs.get("params") or {})
        if kwargs.get("direction") and "direction" not in params:
            params["direction"] = kwargs.get("direction")
        if kwargs.get("start_pos") and "start_pos" not in params:
            params["start_pos"] = kwargs.get("start_pos")

        ori = _get_orientation(refresh=False)
        params = prepare_ddp_params(
            pattern=str(kwargs.get("pattern")),
            params=params,
            orientation=ori,
            default_start_pos=str(settings.quad_default_start_pos),
        )

        return ddp.start(
            pattern=str(kwargs.get("pattern")),
            params=params,
            duration_s=float(kwargs.get("duration_s", 30.0)),
            brightness=min(settings.wled_max_bri, int(kwargs.get("brightness", 128))),
            fps=float(kwargs.get("fps", settings.ddp_fps_default)),
        ).__dict__

    def _tool_stop_ddp(kwargs: Dict[str, Any]) -> Any:
        return ddp.stop().__dict__

    def _tool_stop_all(kwargs: Dict[str, Any]) -> Any:
        # Stop locally, and if peers exist stop them too.
        out: Dict[str, Any] = {}
        try:
            out["self"] = {"ok": True, "result": _a2a_action_stop_all({})}
        except Exception as e:
            out["self"] = {"ok": False, "error": str(e)}

        if PEERS:
            payload = {"action": "stop_all", "params": {}}
            timeout_s = float(settings.a2a_http_timeout_s)
            with ThreadPoolExecutor(max_workers=min(8, len(PEERS))) as ex:
                futs = {
                    ex.submit(
                        _peer_post, peer, "/v1/a2a/invoke", payload, timeout_s=timeout_s
                    ): peer
                    for peer in PEERS.values()
                }
                for fut in as_completed(futs):
                    peer = futs[fut]
                    try:
                        out[peer.name] = fut.result()
                    except Exception as e:
                        out[peer.name] = {"ok": False, "error": str(e)}
        return out

    def _tool_generate_pack(kwargs: Dict[str, Any]) -> Any:
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
        if FLEET_SEQUENCES is None:
            return {"ok": False, "error": "Fleet sequences are not available."}
        file = str(kwargs.get("file") or kwargs.get("sequence_file") or "").strip()
        if not file:
            return {"ok": False, "error": "Missing 'file'."}
        targets = kwargs.get("targets")
        if targets is not None and not isinstance(targets, list):
            targets = None
        include_self = bool(kwargs.get("include_self", True))
        loop = bool(kwargs.get("loop", False))
        timeout_s = kwargs.get("timeout_s")
        try:
            st = FLEET_SEQUENCES.start(
                file=file,
                loop=loop,
                targets=[str(x) for x in (targets or [])] if targets else None,
                include_self=include_self,
                timeout_s=float(timeout_s) if timeout_s is not None else None,
            )
            return {"ok": True, "status": st.__dict__}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def _tool_fleet_stop_sequence(_: Dict[str, Any]) -> Any:
        if FLEET_SEQUENCES is None:
            return {"ok": False, "error": "Fleet sequences are not available."}
        try:
            st = FLEET_SEQUENCES.stop()
            return {"ok": True, "status": st.__dict__}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def _tool_fpp_start_playlist(kwargs: Dict[str, Any]) -> Any:
        if FPP is None:
            return {"ok": False, "error": "FPP is not configured; set FPP_BASE_URL."}
        name = str(kwargs.get("name") or "").strip()
        if not name:
            return {"ok": False, "error": "Missing 'name'."}
        repeat = bool(kwargs.get("repeat", False))
        try:
            return {
                "ok": True,
                "fpp": FPP.start_playlist(name, repeat=repeat).as_dict(),
            }
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def _tool_fpp_stop_playlist(_: Dict[str, Any]) -> Any:
        if FPP is None:
            return {"ok": False, "error": "FPP is not configured; set FPP_BASE_URL."}
        try:
            return {"ok": True, "fpp": FPP.stop_playlist().as_dict()}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def _tool_fpp_trigger_event(kwargs: Dict[str, Any]) -> Any:
        if FPP is None:
            return {"ok": False, "error": "FPP is not configured; set FPP_BASE_URL."}
        try:
            eid = int(kwargs.get("event_id"))
        except Exception:
            return {"ok": False, "error": "event_id must be an integer > 0"}
        try:
            return {"ok": True, "fpp": FPP.trigger_event(eid).as_dict()}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    # Add orientation context so "clockwise" and "front" mean what you expect from the street.
    ori = _get_orientation(refresh=False)
    ori_txt = ""
    if ori is not None and ori.kind == "quarters":
        pos = ori.positions
        ori_txt = (
            "\nStreet orientation (from the street, facing the house): "
            f"right segment={pos.get('right')}, back={pos.get('back')}, left={pos.get('left')}, front={pos.get('front')}. "
            f"Increasing segment order is {ori.order_direction_from_street}. "
            "When the user requests clockwise/counterclockwise motion, set direction='cw' or direction='ccw' in start_ddp_pattern. "
            "For quadrant chases, prefer starting at start_pos='front' unless the user specifies otherwise.\n"
        )

    system_prompt = (
        "You are a show director for a WLED Christmas mega tree. The tree is split into 4 segments (quadrants). "
        "Use tools to apply looks or start DDP patterns. "
        "Use stop_all to stop everything. "
        "You can also start/stop fleet sequences across multiple controllers, and optionally control Falcon Player (FPP) if configured. "
        "Be concise. Prefer apply_random_look for general requests, and start_ddp_pattern for realtime animation requests. "
        "For quadrant motion, use DDP patterns like 'quad_chase', 'opposite_pulse', 'quad_twinkle', 'quad_comets', and 'quad_spiral'. "
        + ori_txt
    )

    DIRECTOR = SimpleDirectorAgent(
        api_key=settings.openai_api_key,
        model=settings.openai_model,
        tools={
            "apply_random_look": _tool_apply_random_look,
            "start_ddp_pattern": _tool_start_ddp,
            "stop_ddp": _tool_stop_ddp,
            "stop_all": _tool_stop_all,
            "generate_looks_pack": _tool_generate_pack,
            "fleet_start_sequence": _tool_fleet_start_sequence,
            "fleet_stop_sequence": _tool_fleet_stop_sequence,
            "fpp_start_playlist": _tool_fpp_start_playlist,
            "fpp_stop_playlist": _tool_fpp_stop_playlist,
            "fpp_trigger_event": _tool_fpp_trigger_event,
        },
        system_prompt=system_prompt,
    )


# ----------------------------
# Routes
# ----------------------------


def root() -> Response:
    return JSONResponse(
        status_code=200,
        content={"ok": True, "service": SERVICE_NAME, "version": APP_VERSION},
    )


def health() -> Dict[str, Any]:
    return {"ok": True, "service": SERVICE_NAME, "version": APP_VERSION}


def _jwt_from_request(request: Request) -> str | None:
    settings = SETTINGS
    if settings is None:
        return None
    auth = request.headers.get("authorization") or ""
    parts = auth.strip().split(None, 1)
    if len(parts) == 2 and parts[0].lower() == "bearer":
        tok = parts[1].strip()
        # Only treat Bearer values that look like a JWT as JWTs; this avoids
        # conflicting with A2A keys or other non-JWT bearer tokens.
        if tok and tok.count(".") == 2:
            return tok
    if settings.auth_cookie_name:
        tok = request.cookies.get(settings.auth_cookie_name)
        if tok:
            return str(tok).strip()
    return None


def _require_jwt_auth(request: Request) -> Dict[str, Any]:
    settings = _require_settings()
    if not settings.auth_enabled:
        raise HTTPException(
            status_code=400, detail="AUTH_ENABLED is false; JWT auth is not configured."
        )
    tok = _jwt_from_request(request)
    if not tok:
        raise HTTPException(status_code=401, detail="Missing token")
    try:
        claims = jwt_decode_hs256(
            tok,
            secret=str(settings.auth_jwt_secret or ""),
            issuer=str(settings.auth_jwt_issuer or ""),
        )
        return {
            "subject": claims.subject,
            "expires_at": claims.expires_at,
            "issued_at": claims.issued_at,
            "claims": claims.raw,
        }
    except AuthError as e:
        raise HTTPException(status_code=401, detail=str(e))


def auth_config() -> Dict[str, Any]:
    settings = _require_settings()
    return {
        "ok": True,
        "version": APP_VERSION,
        "ui_enabled": bool(settings.ui_enabled),
        "auth_enabled": bool(settings.auth_enabled),
        "totp_enabled": bool(settings.auth_totp_enabled),
        "openai_enabled": bool(settings.openai_api_key),
        "fpp_enabled": bool(settings.fpp_base_url),
        "peers_configured": len(PEERS),
    }


def auth_login(req: AuthLoginRequest, response: Response) -> Dict[str, Any]:
    settings = _require_settings()
    if not settings.auth_enabled:
        raise HTTPException(
            status_code=400, detail="AUTH_ENABLED is false; login is disabled."
        )
    user = (req.username or "").strip()
    if user != (settings.auth_username or "").strip():
        raise HTTPException(status_code=401, detail="Invalid username or password")
    if not verify_password(req.password, str(settings.auth_password or "")):
        raise HTTPException(status_code=401, detail="Invalid username or password")
    if settings.auth_totp_enabled:
        if not totp_verify(
            secret_b32=str(settings.auth_totp_secret or ""), code=str(req.totp or "")
        ):
            raise HTTPException(status_code=401, detail="Invalid TOTP code")

    token = jwt_encode_hs256(
        {"sub": user, "role": "admin"},
        secret=str(settings.auth_jwt_secret or ""),
        ttl_s=int(settings.auth_jwt_ttl_s),
        issuer=str(settings.auth_jwt_issuer or ""),
    )

    response.set_cookie(
        key=str(settings.auth_cookie_name),
        value=token,
        httponly=True,
        secure=bool(settings.auth_cookie_secure),
        samesite="lax",
        max_age=int(settings.auth_jwt_ttl_s),
        path="/",
    )
    return {
        "ok": True,
        "user": {"username": user},
        "token": token,
        "expires_in": int(settings.auth_jwt_ttl_s),
    }


def auth_logout(response: Response) -> Dict[str, Any]:
    settings = _require_settings()
    if settings.auth_cookie_name:
        response.delete_cookie(key=str(settings.auth_cookie_name), path="/")
    return {"ok": True}


def auth_me(request: Request) -> Dict[str, Any]:
    info = _require_jwt_auth(request)
    return {
        "ok": True,
        "user": {"username": info["subject"]},
        "expires_at": info["expires_at"],
    }


def wled_info() -> Dict[str, Any]:
    try:
        info = WLED.get_info()
        return {
            "ok": True,
            "info": info,
            "segment_ids": SEGMENT_IDS,
            "replicate_to_all_segments": SETTINGS.wled_replicate_to_all_segments,
        }
    except WLEDError as e:
        raise HTTPException(status_code=502, detail=str(e))


def wled_state() -> Dict[str, Any]:
    try:
        st = WLED.get_state()
        return {"ok": True, "state": st}
    except WLEDError as e:
        raise HTTPException(status_code=502, detail=str(e))


def wled_segments() -> Dict[str, Any]:
    """Return the current segment list from WLED (useful when you have 2+ segments)."""
    try:
        segs = WLED.get_segments(refresh=True)
        return {"ok": True, "segment_ids": SEGMENT_IDS, "segments": segs}
    except WLEDError as e:
        raise HTTPException(status_code=502, detail=str(e))


def segments_layout() -> Dict[str, Any]:
    """Return inferred segment bounds + whether this looks like a 4-quadrant (quarters) layout."""
    try:
        from segment_layout import fetch_segment_layout

        layout = fetch_segment_layout(WLED, segment_ids=SEGMENT_IDS, refresh=True)
        return {
            "ok": True,
            "layout": {
                "kind": layout.kind,
                "led_count": layout.led_count,
                "segments": [
                    {"id": s.id, "start": s.start, "stop": s.stop, "len": s.length}
                    for s in layout.segments
                ],
                "ordered_ids": layout.ordered_ids(),
            },
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def segments_orientation() -> Dict[str, Any]:
    """Return a best-effort street-facing quadrant mapping (front/right/back/left).

    This is mainly useful for 4-segment trees where each segment is a quarter (your 4 outputs).
    """
    ori = _get_orientation(refresh=True)
    return {
        "ok": True,
        "configured": {
            "quad_right_segment_id": SETTINGS.quad_right_segment_id,
            "quad_order_from_street": SETTINGS.quad_order_from_street,
            "quad_default_start_pos": SETTINGS.quad_default_start_pos,
        },
        "orientation": (
            None
            if ori is None
            else {
                "kind": ori.kind,
                "ordered_segment_ids": ori.ordered_segment_ids,
                "order_direction_from_street": ori.order_direction_from_street,
                "right_segment_id": ori.right_segment_id,
                "positions": ori.positions,
                "notes": ori.notes,
            }
        ),
    }


def wled_apply_state(req: ApplyStateRequest) -> Dict[str, Any]:
    try:
        # brightness safety
        if "bri" in req.state:
            req.state["bri"] = min(SETTINGS.wled_max_bri, max(1, int(req.state["bri"])))
        COOLDOWN.wait()
        out = WLED.apply_state(req.state, verbose=False)
        return {"ok": True, "result": out}
    except WLEDError as e:
        raise HTTPException(status_code=502, detail=str(e))


def ddp_patterns() -> Dict[str, Any]:
    from patterns import PatternFactory

    try:
        info = WLED.device_info()
        from segment_layout import fetch_segment_layout

        layout = None
        try:
            layout = fetch_segment_layout(WLED, segment_ids=SEGMENT_IDS, refresh=False)
        except Exception:
            layout = None
        factory = PatternFactory(
            led_count=info.led_count, geometry=GEOM, segment_layout=layout
        )
        return {
            "ok": True,
            "patterns": factory.available(),
            "geometry_enabled": GEOM.enabled_for(info.led_count),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def ddp_status() -> Dict[str, Any]:
    return {"ok": True, "status": DDP.status().__dict__}


def ddp_start(req: DDPStartRequest) -> Dict[str, Any]:
    try:
        # Merge top-level friendly controls into params for convenience
        params = dict(req.params or {})
        if req.direction and "direction" not in params:
            params["direction"] = req.direction
        if req.start_pos and "start_pos" not in params:
            params["start_pos"] = req.start_pos

        ori = _get_orientation(refresh=False)
        params = prepare_ddp_params(
            pattern=req.pattern,
            params=params,
            orientation=ori,
            default_start_pos=str(SETTINGS.quad_default_start_pos),
        )

        st = DDP.start(
            pattern=req.pattern,
            params=params,
            duration_s=req.duration_s,
            brightness=min(SETTINGS.wled_max_bri, req.brightness),
            fps=req.fps,
        )
        _persist_runtime_state("ddp_start", {"pattern": req.pattern})
        return {"ok": True, "status": st.__dict__}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


def ddp_stop() -> Dict[str, Any]:
    st = DDP.stop()
    _persist_runtime_state("ddp_stop")
    return {"ok": True, "status": st.__dict__}


def looks_generate(req: GenerateLooksRequest) -> Dict[str, Any]:
    try:
        summary = LOOKS.generate_pack(
            total_looks=req.total_looks,
            themes=req.themes,
            brightness=min(SETTINGS.wled_max_bri, req.brightness),
            seed=req.seed,
            write_files=req.write_files,
            include_multi_segment=req.include_multi_segment,
        )
        return {"ok": True, "summary": summary.__dict__}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


def looks_packs() -> Dict[str, Any]:
    return {"ok": True, "packs": LOOKS.list_packs(), "latest": LOOKS.latest_pack()}


def looks_apply_random(req: ApplyRandomLookRequest) -> Dict[str, Any]:
    try:
        COOLDOWN.wait()
        out = LOOKS.apply_random(
            theme=req.theme,
            pack_file=req.pack_file,
            brightness=req.brightness,
            seed=req.seed,
        )
        return {"ok": True, "result": out}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


def presets_import(req: ImportPresetsRequest) -> Dict[str, Any]:
    try:
        pack_path = os.path.join(SETTINGS.data_dir, "looks", req.pack_file)
        res = IMPORTER.import_from_pack(
            pack_path=pack_path,
            start_id=req.start_id,
            limit=req.limit,
            name_prefix=req.name_prefix,
            include_brightness=req.include_brightness,
            save_bounds=req.save_bounds,
        )
        return {"ok": True, "result": res.__dict__}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


def sequences_list() -> Dict[str, Any]:
    return {"ok": True, "files": SEQUENCES.list_sequences()}


def sequences_generate(req: GenerateSequenceRequest) -> Dict[str, Any]:
    try:
        pack = req.pack_file or LOOKS.latest_pack()
        if not pack:
            raise RuntimeError("No looks pack found; generate looks first.")
        pack_path = os.path.join(SETTINGS.data_dir, "looks", pack)
        looks = read_jsonl(pack_path)
        # keep a reasonable subset if file is huge
        if len(looks) > 2000:
            looks = looks[:2000]
        ddp_pats = ddp_patterns()["patterns"]  # reuse

        beats_s: Optional[List[float]] = None
        if req.beats_file:
            beats_path = _resolve_data_path(req.beats_file)
            beats_obj = read_json(str(beats_path))
            if not isinstance(beats_obj, dict):
                raise RuntimeError(
                    "beats_file must contain a JSON object with a beats list (beats_s or beats_ms)"
                )
            raw_beats = beats_obj.get("beats_s")
            if raw_beats is None:
                raw_beats = beats_obj.get("beats_ms")
                if raw_beats is not None:
                    try:
                        beats_s = [float(x) / 1000.0 for x in list(raw_beats)]
                    except Exception:
                        beats_s = None
            else:
                try:
                    beats_s = [float(x) for x in list(raw_beats)]
                except Exception:
                    beats_s = None
            if not beats_s or len(beats_s) < 2:
                raise RuntimeError(
                    "beats_file did not contain a usable beats list (need >= 2 marks)"
                )

        fname = SEQUENCES.generate(
            name=req.name,
            looks=looks,
            duration_s=req.duration_s,
            step_s=req.step_s,
            include_ddp=req.include_ddp,
            renderable_only=bool(req.renderable_only),
            beats_s=beats_s,
            beats_per_step=int(req.beats_per_step),
            beat_offset_s=float(req.beat_offset_s),
            ddp_patterns=ddp_pats,
            seed=req.seed,
        )
        return {"ok": True, "file": fname}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


def sequences_status() -> Dict[str, Any]:
    return {"ok": True, "status": SEQUENCES.status().__dict__}


def sequences_play(req: PlaySequenceRequest) -> Dict[str, Any]:
    try:
        st = SEQUENCES.play(file=req.file, loop=req.loop)
        _persist_runtime_state(
            "sequences_play", {"file": req.file, "loop": bool(req.loop)}
        )
        return {"ok": True, "status": st.__dict__}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


def sequences_stop() -> Dict[str, Any]:
    st = SEQUENCES.stop()
    _persist_runtime_state("sequences_stop")
    return {"ok": True, "status": st.__dict__}


def fseq_export(
    req: FSEQExportRequest, _: None = Depends(_require_a2a_auth)
) -> Dict[str, Any]:
    """
    Export a renderable (procedural-pattern) sequence JSON file to an uncompressed .fseq (v1).

    Note: steps of type "look" (WLED effect states) are not offline-renderable and are rejected.
    """
    try:
        seq_root = _resolve_data_path("sequences").resolve()
        seq_path = (seq_root / (req.sequence_file or "")).resolve()
        if seq_root not in seq_path.parents:
            raise HTTPException(
                status_code=400,
                detail="sequence_file must be within DATA_DIR/sequences",
            )
        seq = read_json(str(seq_path))
        steps: List[Dict[str, Any]] = list((seq or {}).get("steps", []))
        if not steps:
            raise HTTPException(status_code=400, detail="Sequence has no steps")

        led_count = (
            int(req.led_count)
            if req.led_count is not None
            else int(WLED.device_info().led_count)
        )
        if led_count <= 0:
            raise HTTPException(status_code=400, detail="led_count must be > 0")
        payload_len = led_count * 3

        channel_start = int(req.channel_start)
        if channel_start <= 0:
            raise HTTPException(status_code=400, detail="channel_start must be >= 1")

        channels_total = (
            int(req.channels_total)
            if req.channels_total is not None
            else (channel_start - 1 + payload_len)
        )
        if channels_total < (channel_start - 1 + payload_len):
            raise HTTPException(
                status_code=400,
                detail="channels_total is too small for channel_start + led_count*3",
            )

        step_ms = int(req.step_ms)
        default_bri = min(SETTINGS.wled_max_bri, max(1, int(req.default_brightness)))

        per_step_frames: List[int] = []
        total_frames = 0
        for step in steps:
            dur_s = float(step.get("duration_s", 0.0))
            if dur_s <= 0:
                dur_s = 0.1
            n = max(1, int(math.ceil((dur_s * 1000.0) / max(1, step_ms))))
            per_step_frames.append(n)
            total_frames += n

        # Best-effort include segment layout so quadrant-aware patterns render correctly.
        layout = None
        try:
            from segment_layout import fetch_segment_layout

            layout = fetch_segment_layout(WLED, segment_ids=SEGMENT_IDS, refresh=False)
        except Exception:
            layout = None

        from patterns import PatternFactory

        factory = PatternFactory(
            led_count=led_count, geometry=GEOM, segment_layout=layout
        )

        out_path = _resolve_data_path(req.out_file)

        frame_idx = 0

        def _frames():
            nonlocal frame_idx
            off = channel_start - 1
            for step, nframes in zip(steps, per_step_frames):
                typ = str(step.get("type") or "").strip().lower()
                if typ != "ddp":
                    raise RuntimeError(
                        f"Non-renderable step type '{typ}' (only 'ddp' is supported for fseq export)."
                    )
                pat_name = str(step.get("pattern") or "").strip()
                if not pat_name:
                    raise RuntimeError("DDP step missing 'pattern'")
                params = step.get("params") or {}
                if not isinstance(params, dict):
                    params = {}
                bri = step.get("brightness")
                bri_i = (
                    default_bri
                    if bri is None
                    else min(SETTINGS.wled_max_bri, max(1, int(bri)))
                )

                pat = factory.create(pat_name, params=params)
                for i in range(int(nframes)):
                    t = (i * step_ms) / 1000.0
                    rgb = pat.frame(t=t, frame_idx=frame_idx, brightness=bri_i)
                    if len(rgb) != payload_len:
                        rgb = (rgb[:payload_len]).ljust(payload_len, b"\x00")
                    frame = bytearray(channels_total)
                    end = min(channels_total, off + payload_len)
                    frame[off:end] = rgb[: (end - off)]
                    frame_idx += 1
                    yield bytes(frame)

        res = write_fseq_v1_file(
            out_path=str(out_path),
            channel_count=channels_total,
            num_frames=total_frames,
            step_ms=step_ms,
            frame_generator=_frames(),
        )
        return {
            "ok": True,
            "source_sequence": seq_path.name,
            "render": {
                "led_count": led_count,
                "channel_start": channel_start,
                "channels_total": channels_total,
                "step_ms": step_ms,
            },
            "fseq": res.__dict__,
        }
    except HTTPException:
        raise
    except (FSEQError, ValueError) as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def audio_analyze(
    req: AudioAnalyzeRequest, _: None = Depends(_require_a2a_auth)
) -> Dict[str, Any]:
    """
    Analyze an audio file and write a beats/BPM timeline JSON under DATA_DIR.

    This is intentionally lightweight and dependency-free (WAV directly; other formats require ffmpeg).
    """
    try:
        audio_path = _resolve_data_path(req.audio_file)
        out_path = _resolve_data_path(req.out_file)
        analysis = analyze_beats(
            audio_path=str(audio_path),
            min_bpm=int(req.min_bpm),
            max_bpm=int(req.max_bpm),
            hop_ms=int(req.hop_ms),
            window_ms=int(req.window_ms),
            peak_threshold=float(req.peak_threshold),
            min_interval_s=float(req.min_interval_s),
            prefer_ffmpeg=bool(req.prefer_ffmpeg),
        )

        out = analysis.as_dict()
        if analysis.bpm > 0:
            out["bpm_timeline"] = [
                {
                    "start_s": 0.0,
                    "end_s": float(analysis.duration_s),
                    "bpm": float(analysis.bpm),
                }
            ]
        else:
            out["bpm_timeline"] = []

        write_json(str(out_path), out)

        base = Path(SETTINGS.data_dir).resolve()
        rel_out = (
            str(out_path.resolve().relative_to(base))
            if base in out_path.resolve().parents
            else str(out_path)
        )
        return {"ok": True, "analysis": out, "out_file": rel_out}
    except HTTPException:
        raise
    except AudioAnalyzeError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def go_crazy(req: GoCrazyRequest) -> Dict[str, Any]:
    try:
        # 1) generate looks pack
        summary = LOOKS.generate_pack(
            total_looks=req.total_looks,
            themes=req.themes,
            brightness=min(SETTINGS.wled_max_bri, req.brightness),
            seed=req.seed,
            write_files=req.write_files,
            include_multi_segment=True,
        )

        results: Dict[str, Any] = {"looks_pack": summary.__dict__}

        # 2) generate sequences
        seq_files: List[str] = []
        if req.sequences > 0:
            pack_path = os.path.join(SETTINGS.data_dir, "looks", summary.file)
            looks = read_jsonl(pack_path)
            if len(looks) > 2000:
                looks = looks[:2000]
            ddp_pats = ddp_patterns()["patterns"]
            for i in range(req.sequences):
                fname = SEQUENCES.generate(
                    name=f"{i+1:02d}_Mix",
                    looks=looks,
                    duration_s=req.sequence_duration_s,
                    step_s=req.step_s,
                    include_ddp=req.include_ddp,
                    renderable_only=False,
                    ddp_patterns=ddp_pats,
                    seed=req.seed + i,
                )
                seq_files.append(fname)
        results["sequences"] = seq_files

        # 3) optional preset import
        if req.import_presets:
            pack_path = os.path.join(SETTINGS.data_dir, "looks", summary.file)
            res = IMPORTER.import_from_pack(
                pack_path=pack_path,
                start_id=req.import_start_id,
                limit=req.import_limit,
                name_prefix="AI",
                include_brightness=True,
                save_bounds=True,
            )
            results["preset_import"] = res.__dict__

        return {"ok": True, "result": results}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


def _local_command(text: str) -> Dict[str, Any]:
    """
    Minimal local command parser used when OpenAI is not configured.

    This is intentionally small and forgiving: it supports a handful of high-value commands that map to
    existing local/fleet actions (status/stop/apply look/start pattern/start sequence/FPP basics).
    """
    command_text = (text or "").strip()
    if not command_text:
        return {"ok": False, "error": "Empty command"}
    command_lower = command_text.lower().strip()

    def _parse_int_arg(keys: List[str], *, min_v: int, max_v: int) -> Optional[int]:
        for k in keys:
            m = re.search(
                rf"(?:{re.escape(k)})\\s*(?:=|:)?\\s*(\\d{{1,3}})", command_lower
            )
            if not m:
                continue
            try:
                v = int(m.group(1))
            except Exception:
                continue
            if min_v <= v <= max_v:
                return v
        return None

    def _parse_float_arg(
        keys: List[str], *, min_v: float, max_v: float
    ) -> Optional[float]:
        for k in keys:
            m = re.search(
                rf"(?:{re.escape(k)})\\s*(?:=|:)?\\s*(\\d+(?:\\.\\d+)?)", command_lower
            )
            if not m:
                continue
            try:
                v = float(m.group(1))
            except Exception:
                continue
            if min_v <= v <= max_v:
                return v
        return None

    if command_lower in {"help", "/help", "?"} or command_lower.startswith("help "):
        return {
            "ok": True,
            "mode": "local",
            "response": (
                "Supported commands: status; stop all; apply look [theme] [brightness=1..255]; "
                "start pattern <name> [duration_s=..] [brightness=..] [fps=..] [cw/ccw] [front/right/back/left]; "
                "start sequence <file> [loop]; stop sequence; fpp status; start playlist <name>; stop playlist; trigger event <id>."
            ),
        }

    if "status" in command_lower and "fpp" not in command_lower:
        return {
            "ok": True,
            "mode": "local",
            "action": "status",
            "result": _a2a_action_status({}),
        }

    if "stop" in command_lower and "all" in command_lower:
        results: Dict[str, Any] = {}
        try:
            results["self"] = {"ok": True, "result": _a2a_action_stop_all({})}
        except Exception as e:
            results["self"] = {"ok": False, "error": str(e)}

        if PEERS:
            payload = {"action": "stop_all", "params": {}}
            timeout_s = float(SETTINGS.a2a_http_timeout_s)
            with ThreadPoolExecutor(max_workers=min(8, len(PEERS))) as ex:
                futs = {
                    ex.submit(
                        _peer_post, peer, "/v1/a2a/invoke", payload, timeout_s=timeout_s
                    ): peer
                    for peer in PEERS.values()
                }
                for fut in as_completed(futs):
                    peer = futs[fut]
                    try:
                        results[peer.name] = fut.result()
                    except Exception as e:
                        results[peer.name] = {"ok": False, "error": str(e)}

        return {"ok": True, "mode": "local", "action": "stop_all", "result": results}

    if ("stop" in command_lower) and (
        "sequence" in command_lower or "seq" in command_lower
    ):
        if FLEET_SEQUENCES is not None:
            st = FLEET_SEQUENCES.stop()
            return {
                "ok": True,
                "mode": "local",
                "action": "fleet_stop_sequence",
                "status": st.__dict__,
            }
        st = SEQUENCES.stop()
        return {
            "ok": True,
            "mode": "local",
            "action": "stop_sequence",
            "status": st.__dict__,
        }

    sequence_match = re.search(
        r"(?:start|play)\\s+(?:fleet\\s+)?(?:sequence|seq)\\s+([^\\s]+)",
        command_text,
        flags=re.IGNORECASE,
    )
    if sequence_match:
        file = sequence_match.group(1).strip()
        loop = (" loop" in command_lower) or (" repeat" in command_lower)
        if FLEET_SEQUENCES is not None:
            st = FLEET_SEQUENCES.start(
                file=file, loop=loop, targets=None, include_self=True, timeout_s=None
            )
            return {
                "ok": True,
                "mode": "local",
                "action": "fleet_start_sequence",
                "status": st.__dict__,
            }
        st = SEQUENCES.play(file=file, loop=loop)
        return {
            "ok": True,
            "mode": "local",
            "action": "start_sequence",
            "status": st.__dict__,
        }

    if "apply" in command_lower and (
        "look" in command_lower or "theme" in command_lower
    ):
        theme: Optional[str] = None
        theme_map = {
            "candy cane": "candy_cane",
            "candy_cane": "candy_cane",
            "classic": "classic",
            "icy": "icy",
            "warm white": "warm_white",
            "warm_white": "warm_white",
            "rainbow": "rainbow",
            "halloween": "halloween",
        }
        for k, v in theme_map.items():
            if k in command_lower:
                theme = v
                break
        brightness = _parse_int_arg(["brightness", "bri"], min_v=1, max_v=255)

        if PEERS:
            pack, row = LOOKS.choose_random(theme=theme)
            bri_i: Optional[int] = None
            if brightness is not None:
                bri_i = min(SETTINGS.wled_max_bri, max(1, int(brightness)))

            out: Dict[str, Any] = {
                "picked": {
                    "pack_file": pack,
                    "id": row.get("id"),
                    "name": row.get("name"),
                    "theme": row.get("theme"),
                }
            }
            try:
                COOLDOWN.wait()
                out["self"] = {
                    "ok": True,
                    "result": LOOKS.apply_look(row, brightness_override=bri_i),
                }
            except Exception as e:
                out["self"] = {"ok": False, "error": str(e)}

            timeout_s = float(SETTINGS.a2a_http_timeout_s)
            eligible: List[A2APeer] = []
            for peer in PEERS.values():
                actions = _peer_supported_actions(peer, timeout_s=timeout_s)
                if "apply_look_spec" in actions:
                    eligible.append(peer)
                else:
                    out[peer.name] = {
                        "ok": False,
                        "skipped": True,
                        "reason": "Peer does not support apply_look_spec",
                    }

            if eligible:
                payload = {
                    "action": "apply_look_spec",
                    "params": {"look_spec": row, "brightness_override": bri_i},
                }
                with ThreadPoolExecutor(max_workers=min(8, len(eligible))) as ex:
                    futs = {
                        ex.submit(
                            _peer_post,
                            peer,
                            "/v1/a2a/invoke",
                            payload,
                            timeout_s=timeout_s,
                        ): peer
                        for peer in eligible
                    }
                    for fut in as_completed(futs):
                        peer = futs[fut]
                        try:
                            out[peer.name] = fut.result()
                        except Exception as e:
                            out[peer.name] = {"ok": False, "error": str(e)}

            return {
                "ok": True,
                "mode": "local",
                "action": "apply_random_look",
                "result": out,
            }

        res = LOOKS.apply_random(theme=theme, brightness=brightness)
        return {
            "ok": True,
            "mode": "local",
            "action": "apply_random_look",
            "result": res,
        }

    pattern_match = re.search(
        r"(?:start|run)\\s+(?:realtime\\s+)?(?:ddp\\s+)?pattern\\s+([a-z0-9_]+)",
        command_lower,
    )
    if pattern_match:
        pattern_name = pattern_match.group(1).strip()
        duration_s = (
            _parse_float_arg(["duration_s", "duration", "for"], min_v=0.1, max_v=600.0)
            or 30.0
        )
        brightness = _parse_int_arg(["brightness", "bri"], min_v=1, max_v=255) or 128
        fps = _parse_float_arg(["fps"], min_v=1.0, max_v=60.0)

        direction = None
        if "ccw" in command_lower or "counterclockwise" in command_lower:
            direction = "ccw"
        if "cw" in command_lower or "clockwise" in command_lower:
            direction = "cw"

        start_pos = None
        for pos in ("front", "right", "back", "left"):
            if re.search(rf"\\b{pos}\\b", command_lower):
                start_pos = pos
                break

        params: Dict[str, Any] = {
            "pattern": pattern_name,
            "duration_s": duration_s,
            "brightness": brightness,
        }
        if fps is not None:
            params["fps"] = fps
        if direction is not None:
            params["direction"] = direction
        if start_pos is not None:
            params["start_pos"] = start_pos

        out: Dict[str, Any] = {}
        try:
            out["self"] = {"ok": True, "result": _a2a_action_start_ddp(params)}
        except Exception as e:
            out["self"] = {"ok": False, "error": str(e)}

        if PEERS:
            payload = {"action": "start_ddp_pattern", "params": params}
            timeout_s = float(SETTINGS.a2a_http_timeout_s)
            with ThreadPoolExecutor(max_workers=min(8, len(PEERS))) as ex:
                futs = {
                    ex.submit(
                        _peer_post, peer, "/v1/a2a/invoke", payload, timeout_s=timeout_s
                    ): peer
                    for peer in PEERS.values()
                }
                for fut in as_completed(futs):
                    peer = futs[fut]
                    try:
                        out[peer.name] = fut.result()
                    except Exception as e:
                        out[peer.name] = {"ok": False, "error": str(e)}

        return {
            "ok": True,
            "mode": "local",
            "action": "start_ddp_pattern",
            "result": out,
        }

    if "stop" in command_lower and (
        "pattern" in command_lower or "ddp" in command_lower
    ):
        out: Dict[str, Any] = {}
        try:
            out["self"] = {"ok": True, "result": _a2a_action_stop_ddp({})}
        except Exception as e:
            out["self"] = {"ok": False, "error": str(e)}

        if PEERS:
            payload = {"action": "stop_ddp", "params": {}}
            timeout_s = float(SETTINGS.a2a_http_timeout_s)
            with ThreadPoolExecutor(max_workers=min(8, len(PEERS))) as ex:
                futs = {
                    ex.submit(
                        _peer_post, peer, "/v1/a2a/invoke", payload, timeout_s=timeout_s
                    ): peer
                    for peer in PEERS.values()
                }
                for fut in as_completed(futs):
                    peer = futs[fut]
                    try:
                        out[peer.name] = fut.result()
                    except Exception as e:
                        out[peer.name] = {"ok": False, "error": str(e)}

        return {"ok": True, "mode": "local", "action": "stop_ddp", "result": out}

    if "fpp" in command_lower and "status" in command_lower:
        if FPP is None:
            return {"ok": False, "error": "FPP is not configured; set FPP_BASE_URL."}
        return {
            "ok": True,
            "mode": "local",
            "action": "fpp_status",
            "result": FPP.status().as_dict(),
        }

    playlist_match = re.search(
        r"start\\s+playlist\\s+(.+)$", command_text, flags=re.IGNORECASE
    )
    if playlist_match:
        if FPP is None:
            return {"ok": False, "error": "FPP is not configured; set FPP_BASE_URL."}
        name = playlist_match.group(1).strip()
        if not name:
            return {"ok": False, "error": "Playlist name is required."}
        res = FPP.start_playlist(name, repeat=("repeat" in command_lower)).as_dict()
        return {
            "ok": True,
            "mode": "local",
            "action": "fpp_start_playlist",
            "result": res,
        }

    if "stop" in command_lower and "playlist" in command_lower:
        if FPP is None:
            return {"ok": False, "error": "FPP is not configured; set FPP_BASE_URL."}
        return {
            "ok": True,
            "mode": "local",
            "action": "fpp_stop_playlist",
            "result": FPP.stop_playlist().as_dict(),
        }

    event_match = re.search(r"(?:trigger\\s+event|event)\\s+(\\d+)", command_lower)
    if event_match:
        if FPP is None:
            return {"ok": False, "error": "FPP is not configured; set FPP_BASE_URL."}
        eid = int(event_match.group(1))
        return {
            "ok": True,
            "mode": "local",
            "action": "fpp_trigger_event",
            "result": FPP.trigger_event(eid).as_dict(),
        }

    return {"ok": False, "error": "Unrecognized command (try 'help')"}


def command(req: CommandRequest) -> Dict[str, Any]:
    if DIRECTOR is not None:
        try:
            return DIRECTOR.run(req.text)
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    try:
        return _local_command(req.text)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ----------------------------
# Falcon Player (FPP) integration (optional)
# ----------------------------


def _require_fpp() -> FPPClient:
    if FPP is None:
        raise HTTPException(
            status_code=400, detail="FPP integration not configured; set FPP_BASE_URL."
        )
    return FPP


def _resolve_data_path(rel_path: str) -> Path:
    base = Path(_require_settings().data_dir).resolve()
    p = (base / (rel_path or "")).resolve()
    if p == base:
        return p
    if base not in p.parents:
        raise HTTPException(status_code=400, detail="Path must be within DATA_DIR.")
    return p


def fpp_status(_: None = Depends(_require_a2a_auth)) -> Dict[str, Any]:
    fpp = _require_fpp()
    try:
        return {"ok": True, "fpp": fpp.status().as_dict()}
    except FPPError as e:
        raise HTTPException(status_code=502, detail=str(e))


def fpp_discover(_: None = Depends(_require_a2a_auth)) -> Dict[str, Any]:
    fpp = _require_fpp()
    try:
        return {"ok": True, "discover": fpp.discover()}
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))


def fpp_playlists(_: None = Depends(_require_a2a_auth)) -> Dict[str, Any]:
    fpp = _require_fpp()
    try:
        return {"ok": True, "fpp": fpp.playlists().as_dict()}
    except FPPError as e:
        raise HTTPException(status_code=502, detail=str(e))


def fpp_start_playlist(
    req: FPPStartPlaylistRequest, _: None = Depends(_require_a2a_auth)
) -> Dict[str, Any]:
    fpp = _require_fpp()
    try:
        return {
            "ok": True,
            "fpp": fpp.start_playlist(req.name, repeat=req.repeat).as_dict(),
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


def fpp_stop_playlist(_: None = Depends(_require_a2a_auth)) -> Dict[str, Any]:
    fpp = _require_fpp()
    try:
        return {"ok": True, "fpp": fpp.stop_playlist().as_dict()}
    except FPPError as e:
        raise HTTPException(status_code=502, detail=str(e))


def fpp_trigger_event(
    req: FPPTriggerEventRequest, _: None = Depends(_require_a2a_auth)
) -> Dict[str, Any]:
    fpp = _require_fpp()
    try:
        return {"ok": True, "fpp": fpp.trigger_event(req.event_id).as_dict()}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


def fpp_proxy(
    req: FPPProxyRequest, _: None = Depends(_require_a2a_auth)
) -> Dict[str, Any]:
    fpp = _require_fpp()
    method = (req.method or "GET").strip().upper()
    if method not in ("GET", "POST", "PUT", "DELETE", "PATCH"):
        raise HTTPException(
            status_code=400, detail="Unsupported method; use GET/POST/PUT/DELETE/PATCH."
        )
    try:
        resp = fpp.request(
            method, req.path, params=dict(req.params or {}), json_body=req.json_body
        )
        return {"ok": True, "fpp": resp.as_dict()}
    except FPPError as e:
        raise HTTPException(status_code=502, detail=str(e))


def fpp_upload_file(
    req: FPPUploadFileRequest, _: None = Depends(_require_a2a_auth)
) -> Dict[str, Any]:
    fpp = _require_fpp()
    try:
        local_path = _resolve_data_path(req.local_file)
        if not local_path.is_file():
            raise HTTPException(status_code=400, detail="local_file does not exist")
        dest = (req.dest_filename or "").strip() or local_path.name
        content = local_path.read_bytes()
        resp = fpp.upload_file(
            dir=req.dir, subdir=req.subdir, filename=dest, content=content
        )
        return {
            "ok": True,
            "local": str(local_path),
            "dest": {"dir": req.dir, "subdir": req.subdir, "filename": dest},
            "fpp": resp.as_dict(),
        }
    except HTTPException:
        raise
    except FPPError as e:
        raise HTTPException(status_code=502, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# ----------------------------
# Show config + xLights import (optional helpers)
# ----------------------------


def show_config_load(
    req: ShowConfigLoadRequest, _: None = Depends(_require_a2a_auth)
) -> Dict[str, Any]:
    try:
        cfg = load_show_config(data_dir=SETTINGS.data_dir, rel_path=req.file)
        return {"ok": True, "config": cfg.as_dict()}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


def xlights_import_networks(
    req: XlightsImportNetworksRequest, _: None = Depends(_require_a2a_auth)
) -> Dict[str, Any]:
    try:
        networks_path = _resolve_data_path(req.networks_file)
        controllers = import_xlights_networks_file(str(networks_path))
        cfg = show_config_from_xlights_networks(
            networks=controllers,
            show_name=req.show_name,
            subnet=req.subnet,
            coordinator_base_url=req.coordinator_base_url,
            fpp_base_url=req.fpp_base_url,
        )
        out_path = write_show_config(
            data_dir=SETTINGS.data_dir, rel_path=req.out_file, config=cfg
        )
        return {
            "ok": True,
            "controllers": [
                {
                    "name": c.name,
                    "host": c.host,
                    "protocol": c.protocol,
                    "universe_start": c.universe_start,
                    "pixel_count": c.pixel_count,
                }
                for c in controllers
            ],
            "show_config_file": out_path,
            "show_config": cfg.as_dict(),
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


def xlights_import_project(
    req: XlightsImportProjectRequest, _: None = Depends(_require_a2a_auth)
) -> Dict[str, Any]:
    try:
        proj = _resolve_data_path(req.project_dir)
        if not proj.is_dir():
            raise HTTPException(
                status_code=400, detail="project_dir must be a directory under DATA_DIR"
            )

        proj_root = proj.resolve()

        def _pick(name: Optional[str], defaults: List[str]) -> Path:
            candidates = [name] if name else []
            candidates.extend([d for d in defaults if d not in candidates])
            for n in candidates:
                if not n:
                    continue
                p = (proj_root / n).resolve()
                if proj_root not in p.parents:
                    continue
                if p.is_file():
                    return p
            raise HTTPException(
                status_code=400,
                detail=f"Missing required xLights file (tried: {', '.join(candidates)})",
            )

        networks_path = _pick(req.networks_file, ["xlights_networks.xml"])
        models_path = _pick(
            req.models_file,
            ["xlights_rgbeffects.xml", "xlights_models.xml", "xlights_layout.xml"],
        )

        controllers = import_xlights_networks_file(str(networks_path))
        models = import_xlights_models_file(str(models_path))

        cfg = show_config_from_xlights_project(
            networks=controllers,
            models=models,
            show_name=req.show_name,
            subnet=req.subnet,
            coordinator_base_url=req.coordinator_base_url,
            fpp_base_url=req.fpp_base_url,
            include_controllers=bool(req.include_controllers),
            include_models=bool(req.include_models),
        )
        out_path = write_show_config(
            data_dir=SETTINGS.data_dir, rel_path=req.out_file, config=cfg
        )
        return {
            "ok": True,
            "project_dir": str(proj_root),
            "networks_file": networks_path.name,
            "models_file": models_path.name,
            "controllers": [
                {
                    "name": c.name,
                    "host": c.host,
                    "protocol": c.protocol,
                    "universe_start": c.universe_start,
                    "pixel_count": c.pixel_count,
                }
                for c in controllers
            ],
            "models": [
                {
                    "name": m.name,
                    "start_channel": m.start_channel,
                    "channel_count": m.channel_count,
                    "pixel_count": m.pixel_count,
                }
                for m in models
            ],
            "show_config_file": out_path,
            "show_config": cfg.as_dict(),
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


def xlights_import_sequence(
    req: XlightsImportSequenceRequest, _: None = Depends(_require_a2a_auth)
) -> Dict[str, Any]:
    """
    Import a beat/timing grid from an xLights `.xsq` file.

    This does not attempt to reproduce xLights effects; it only extracts timing marks which can be used to
    generate beat-aligned sequences in this service.
    """
    try:
        xsq_path = _resolve_data_path(req.xsq_file)
        out_path = _resolve_data_path(req.out_file)

        analysis = import_xlights_xsq_timing_file(
            xsq_path=str(xsq_path), timing_track=req.timing_track
        )
        write_json(str(out_path), analysis)

        base = Path(SETTINGS.data_dir).resolve()
        rel_out = (
            str(out_path.resolve().relative_to(base))
            if base in out_path.resolve().parents
            else str(out_path)
        )
        return {"ok": True, "analysis": analysis, "out_file": rel_out}
    except HTTPException:
        raise
    except XlightsSequenceImportError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ----------------------------
# Export FPP helper scripts (FPP -> Agent triggers)
# ----------------------------


def export_fleet_sequence_start_script(
    req: FPPExportFleetSequenceScriptRequest, _: None = Depends(_require_a2a_auth)
) -> Dict[str, Any]:
    coord = (req.coordinator_base_url or "").strip()
    if (not coord) and req.show_config_file:
        cfg = load_show_config(
            data_dir=SETTINGS.data_dir, rel_path=req.show_config_file
        )
        coord = (cfg.coordinator.base_url or "").strip()
    if not coord:
        raise HTTPException(
            status_code=400,
            detail="Provide coordinator_base_url or show_config_file with coordinator.base_url.",
        )

    payload: Dict[str, Any] = {
        "file": req.sequence_file,
        "loop": bool(req.loop),
        "targets": req.targets,
        "include_self": bool(req.include_self),
    }
    script = render_http_post_script(
        coordinator_base_url=coord,
        path="/v1/fleet/sequences/start",
        payload=payload,
        a2a_api_key=SETTINGS.a2a_api_key if req.include_a2a_key else None,
    )

    out_dir = str(_resolve_data_path("fpp/scripts"))
    res = write_script(out_dir=out_dir, filename=req.out_filename, script_text=script)
    return {
        "ok": True,
        "script": {
            "file": res.filename,
            "path": res.rel_path,
            "bytes": res.bytes_written,
        },
    }


def export_fleet_stop_all_script(
    req: FPPExportFleetStopAllScriptRequest, _: None = Depends(_require_a2a_auth)
) -> Dict[str, Any]:
    coord = (req.coordinator_base_url or "").strip()
    if (not coord) and req.show_config_file:
        cfg = load_show_config(
            data_dir=SETTINGS.data_dir, rel_path=req.show_config_file
        )
        coord = (cfg.coordinator.base_url or "").strip()
    if not coord:
        raise HTTPException(
            status_code=400,
            detail="Provide coordinator_base_url or show_config_file with coordinator.base_url.",
        )

    payload: Dict[str, Any] = {
        "targets": req.targets,
        "include_self": bool(req.include_self),
    }
    script = render_http_post_script(
        coordinator_base_url=coord,
        path="/v1/fleet/stop_all",
        payload=payload,
        a2a_api_key=SETTINGS.a2a_api_key if req.include_a2a_key else None,
    )

    out_dir = str(_resolve_data_path("fpp/scripts"))
    res = write_script(out_dir=out_dir, filename=req.out_filename, script_text=script)
    return {
        "ok": True,
        "script": {
            "file": res.filename,
            "path": res.rel_path,
            "bytes": res.bytes_written,
        },
    }


# ----------------------------
# A2A (Agent-to-Agent) protocol
# ----------------------------


def _a2a_action_pick_random_look_spec(kwargs: Dict[str, Any]) -> Dict[str, Any]:
    pack, row = LOOKS.choose_random(
        theme=kwargs.get("theme"),
        pack_file=kwargs.get("pack_file"),
        seed=kwargs.get("seed"),
    )
    return {"pack_file": pack, "look_spec": row}


def _a2a_action_apply_look_spec(kwargs: Dict[str, Any]) -> Dict[str, Any]:
    look_spec = (
        kwargs.get("look_spec") or kwargs.get("look") or kwargs.get("state") or {}
    )
    if not isinstance(look_spec, dict):
        raise ValueError("look_spec must be an object")
    bri = kwargs.get("brightness")
    if bri is None:
        bri = kwargs.get("brightness_override")
    bri_i: Optional[int] = None
    if bri is not None:
        bri_i = min(SETTINGS.wled_max_bri, max(1, int(bri)))
    COOLDOWN.wait()
    return LOOKS.apply_look(look_spec, brightness_override=bri_i)


def _a2a_action_apply_state(kwargs: Dict[str, Any]) -> Dict[str, Any]:
    state = kwargs.get("state") or {}
    if not isinstance(state, dict):
        raise ValueError("state must be an object")
    if "bri" in state:
        state["bri"] = min(SETTINGS.wled_max_bri, max(1, int(state["bri"])))
    COOLDOWN.wait()
    out = WLED.apply_state(state, verbose=False)
    return {"result": out}


def _a2a_action_start_ddp(kwargs: Dict[str, Any]) -> Dict[str, Any]:
    params = dict(kwargs.get("params") or {})
    if kwargs.get("direction") and "direction" not in params:
        params["direction"] = kwargs.get("direction")
    if kwargs.get("start_pos") and "start_pos" not in params:
        params["start_pos"] = kwargs.get("start_pos")

    ori = _get_orientation(refresh=False)
    params = prepare_ddp_params(
        pattern=str(kwargs.get("pattern")),
        params=params,
        orientation=ori,
        default_start_pos=str(SETTINGS.quad_default_start_pos),
    )

    st = DDP.start(
        pattern=str(kwargs.get("pattern")),
        params=params,
        duration_s=float(kwargs.get("duration_s", 30.0)),
        brightness=min(SETTINGS.wled_max_bri, int(kwargs.get("brightness", 128))),
        fps=float(kwargs.get("fps", SETTINGS.ddp_fps_default)),
    )
    return {"status": st.__dict__}


def _a2a_action_stop_ddp(_: Dict[str, Any]) -> Dict[str, Any]:
    return {"status": DDP.stop().__dict__}


def _a2a_action_stop_all(_: Dict[str, Any]) -> Dict[str, Any]:
    # sequences.stop() best-effort stops DDP too.
    st = SEQUENCES.stop()
    fleet_st = None
    try:
        if FLEET_SEQUENCES is not None:
            fleet_st = FLEET_SEQUENCES.stop().__dict__
    except Exception:
        fleet_st = None
    return {
        "sequence": st.__dict__,
        "fleet_sequence": fleet_st,
        "ddp": DDP.status().__dict__,
    }


def _a2a_action_status(_: Dict[str, Any]) -> Dict[str, Any]:
    fleet_st = None
    try:
        if FLEET_SEQUENCES is not None:
            fleet_st = FLEET_SEQUENCES.status().__dict__
    except Exception:
        fleet_st = None
    return {
        "sequence": SEQUENCES.status().__dict__,
        "fleet_sequence": fleet_st,
        "ddp": DDP.status().__dict__,
    }


_A2A_ACTIONS: Dict[str, Any] = {
    "pick_random_look_spec": _a2a_action_pick_random_look_spec,
    "apply_look_spec": _a2a_action_apply_look_spec,
    "apply_state": _a2a_action_apply_state,
    "start_ddp_pattern": _a2a_action_start_ddp,
    "stop_ddp": _a2a_action_stop_ddp,
    "stop_all": _a2a_action_stop_all,
    "status": _a2a_action_status,
}

_A2A_CAPABILITIES: List[Dict[str, Any]] = [
    {
        "action": "pick_random_look_spec",
        "description": "Choose a look spec from a local pack without applying it.",
        "params": {
            "theme": "optional string",
            "pack_file": "optional string",
            "seed": "optional int",
        },
    },
    {
        "action": "apply_look_spec",
        "description": "Apply a look spec (effect/palette by name) to this WLED device.",
        "params": {"look_spec": "object", "brightness_override": "optional int"},
    },
    {
        "action": "apply_state",
        "description": "Apply a raw WLED /json/state payload (brightness capped).",
        "params": {"state": "object"},
    },
    {
        "action": "start_ddp_pattern",
        "description": "Start a realtime DDP pattern for a duration.",
        "params": {
            "pattern": "string",
            "params": "object",
            "duration_s": "optional number",
            "brightness": "optional int",
            "fps": "optional number",
            "direction": "optional 'cw'|'ccw'",
            "start_pos": "optional 'front'|'right'|'back'|'left'",
        },
    },
    {"action": "stop_ddp", "description": "Stop any running DDP stream.", "params": {}},
    {"action": "stop_all", "description": "Stop sequences and DDP.", "params": {}},
    {"action": "status", "description": "Get sequence + DDP status.", "params": {}},
]


def a2a_card(_: None = Depends(_require_a2a_auth)) -> Dict[str, Any]:
    return {
        "ok": True,
        "agent": {
            "id": SETTINGS.agent_id,
            "name": SETTINGS.agent_name,
            "role": SETTINGS.agent_role,
            "version": APP_VERSION,
            "endpoints": {"card": "/v1/a2a/card", "invoke": "/v1/a2a/invoke"},
            "wled": {
                "url": SETTINGS.wled_tree_url,
                "segment_ids": SEGMENT_IDS,
                "replicate_to_all_segments": SETTINGS.wled_replicate_to_all_segments,
            },
            "capabilities": _A2A_CAPABILITIES,
        },
    }


def a2a_invoke(
    req: A2AInvokeRequest, _: None = Depends(_require_a2a_auth)
) -> Dict[str, Any]:
    action = (req.action or "").strip()
    fn = _A2A_ACTIONS.get(action)
    if fn is None:
        return {
            "ok": False,
            "request_id": req.request_id,
            "error": f"Unknown action '{action}'",
        }
    try:
        res = fn(dict(req.params or {}))
        return {
            "ok": True,
            "request_id": req.request_id,
            "action": action,
            "result": res,
        }
    except Exception as e:
        return {
            "ok": False,
            "request_id": req.request_id,
            "action": action,
            "error": str(e),
        }


# ----------------------------
# Fleet orchestration (optional)
# ----------------------------


def _peer_headers() -> Dict[str, str]:
    settings = SETTINGS
    if settings is None or (not settings.a2a_api_key):
        return {}
    return {"X-A2A-Key": settings.a2a_api_key}


def _peer_post(
    peer: A2APeer, path: str, payload: Dict[str, Any], *, timeout_s: float
) -> Dict[str, Any]:
    url = peer.base_url.rstrip("/") + path
    client = _peer_http_client()
    try:
        resp = client.post(
            url,
            json=payload,
            headers=_peer_headers(),
            timeout=float(timeout_s),
        )
    except Exception as e:
        return {"ok": False, "error": str(e)}
    try:
        body = resp.json()
    except Exception:
        body = {"ok": False, "error": resp.text[:300]}
    if (
        resp.status_code >= 400
        and isinstance(body, dict)
        and body.get("ok") is not False
    ):
        body = {"ok": False, "error": f"HTTP {resp.status_code}: {resp.text[:300]}"}
    return (
        body
        if isinstance(body, dict)
        else {"ok": False, "error": "Non-object response"}
    )


def _peer_get(peer: A2APeer, path: str, *, timeout_s: float) -> Dict[str, Any]:
    url = peer.base_url.rstrip("/") + path
    client = _peer_http_client()
    try:
        resp = client.get(
            url,
            headers=_peer_headers(),
            timeout=float(timeout_s),
        )
    except Exception as e:
        return {"ok": False, "error": str(e)}
    try:
        body = resp.json()
    except Exception:
        body = {"ok": False, "error": resp.text[:300]}
    if (
        resp.status_code >= 400
        and isinstance(body, dict)
        and body.get("ok") is not False
    ):
        body = {"ok": False, "error": f"HTTP {resp.status_code}: {resp.text[:300]}"}
    return (
        body
        if isinstance(body, dict)
        else {"ok": False, "error": "Non-object response"}
    )


def _peer_supported_actions(peer: A2APeer, *, timeout_s: float) -> set[str]:
    card = _peer_get(peer, "/v1/a2a/card", timeout_s=timeout_s)
    if not isinstance(card, dict) or card.get("ok") is not True:
        return set()
    agent = card.get("agent") or {}
    caps = agent.get("capabilities") or []
    actions: set[str] = set()
    if isinstance(caps, list):
        for c in caps:
            if isinstance(c, dict) and "action" in c:
                actions.add(str(c.get("action")))
            elif isinstance(c, str):
                actions.add(c)
    return actions


def _select_peers(targets: Optional[List[str]]) -> List[A2APeer]:
    if not PEERS:
        return []
    if not targets:
        return list(PEERS.values())
    out: List[A2APeer] = []
    for t in targets:
        if t in PEERS:
            out.append(PEERS[t])
    return out


def _fleet_local_invoke(action: str, params: Dict[str, Any]) -> Any:
    fn = _A2A_ACTIONS.get(action)
    if fn is None:
        raise RuntimeError(f"Unknown local action '{action}'")
    return fn(dict(params or {}))


def _fleet_peer_invoke(
    peer: A2APeer, action: str, params: Dict[str, Any], timeout_s: float
) -> Dict[str, Any]:
    payload = {"action": action, "params": dict(params or {})}
    return _peer_post(peer, "/v1/a2a/invoke", payload, timeout_s=float(timeout_s))


# Run sequences across the whole fleet (coordinator use-case; safe when PEERS is empty too).


def fleet_peers(_: None = Depends(_require_a2a_auth)) -> Dict[str, Any]:
    return {
        "ok": True,
        "self": {
            "id": SETTINGS.agent_id,
            "name": SETTINGS.agent_name,
            "role": SETTINGS.agent_role,
        },
        "peers": [{"name": p.name, "base_url": p.base_url} for p in PEERS.values()],
    }


def fleet_invoke(
    req: FleetInvokeRequest, _: None = Depends(_require_a2a_auth)
) -> Dict[str, Any]:
    action = (req.action or "").strip()
    timeout_s = (
        float(req.timeout_s)
        if req.timeout_s is not None
        else float(SETTINGS.a2a_http_timeout_s)
    )
    peers = _select_peers(req.targets)

    results: Dict[str, Any] = {}
    if req.include_self:
        local = _A2A_ACTIONS.get(action)
        if local is None:
            results["self"] = {"ok": False, "error": f"Unknown action '{action}'"}
        else:
            try:
                results["self"] = {"ok": True, "result": local(dict(req.params or {}))}
            except Exception as e:
                results["self"] = {"ok": False, "error": str(e)}

    if peers:
        payload = {"action": action, "params": dict(req.params or {})}
        with ThreadPoolExecutor(max_workers=min(8, len(peers))) as ex:
            futs = {
                ex.submit(
                    _peer_post, peer, "/v1/a2a/invoke", payload, timeout_s=timeout_s
                ): peer
                for peer in peers
            }
            for fut in as_completed(futs):
                peer = futs[fut]
                try:
                    results[peer.name] = fut.result()
                except Exception as e:
                    results[peer.name] = {"ok": False, "error": str(e)}

    return {"ok": True, "action": action, "results": results}


def fleet_apply_random_look(
    req: FleetApplyRandomLookRequest, _: None = Depends(_require_a2a_auth)
) -> Dict[str, Any]:
    # Pick on this agent, then broadcast the same look_spec to peers so devices match.
    pack, row = LOOKS.choose_random(
        theme=req.theme, pack_file=req.pack_file, seed=req.seed
    )
    bri = (
        min(SETTINGS.wled_max_bri, req.brightness)
        if req.brightness is not None
        else None
    )

    results: Dict[str, Any] = {
        "pack_file": pack,
        "picked": {
            "id": row.get("id"),
            "name": row.get("name"),
            "theme": row.get("theme"),
        },
    }

    peers = _select_peers(req.targets)
    timeout_s = float(SETTINGS.a2a_http_timeout_s)

    if req.include_self:
        try:
            COOLDOWN.wait()
            results["self"] = {
                "ok": True,
                "result": LOOKS.apply_look(row, brightness_override=bri),
            }
        except Exception as e:
            results["self"] = {"ok": False, "error": str(e)}

    if peers:
        eligible: List[A2APeer] = []
        for peer in peers:
            actions = _peer_supported_actions(peer, timeout_s=timeout_s)
            if "apply_look_spec" in actions:
                eligible.append(peer)
            else:
                results[peer.name] = {
                    "ok": False,
                    "skipped": True,
                    "reason": "Peer does not support apply_look_spec",
                }

        if eligible:
            payload = {
                "action": "apply_look_spec",
                "params": {"look_spec": row, "brightness_override": bri},
            }
            with ThreadPoolExecutor(max_workers=min(8, len(eligible))) as ex:
                futs = {
                    ex.submit(
                        _peer_post, peer, "/v1/a2a/invoke", payload, timeout_s=timeout_s
                    ): peer
                    for peer in eligible
                }
                for fut in as_completed(futs):
                    peer = futs[fut]
                    try:
                        results[peer.name] = fut.result()
                    except Exception as e:
                        results[peer.name] = {"ok": False, "error": str(e)}

    return {"ok": True, "result": results}


def fleet_stop_all(
    req: FleetStopAllRequest, _: None = Depends(_require_a2a_auth)
) -> Dict[str, Any]:
    timeout_s = (
        float(req.timeout_s)
        if req.timeout_s is not None
        else float(SETTINGS.a2a_http_timeout_s)
    )
    peers = _select_peers(req.targets)
    results: Dict[str, Any] = {}

    if req.include_self:
        try:
            results["self"] = {"ok": True, "result": _a2a_action_stop_all({})}
        except Exception as e:
            results["self"] = {"ok": False, "error": str(e)}

    if peers:
        payload = {"action": "stop_all", "params": {}}
        with ThreadPoolExecutor(max_workers=min(8, len(peers))) as ex:
            futs = {
                ex.submit(
                    _peer_post, peer, "/v1/a2a/invoke", payload, timeout_s=timeout_s
                ): peer
                for peer in peers
            }
            for fut in as_completed(futs):
                peer = futs[fut]
                try:
                    results[peer.name] = fut.result()
                except Exception as e:
                    results[peer.name] = {"ok": False, "error": str(e)}

    _persist_runtime_state("fleet_stop_all", {"targets": req.targets})
    return {"ok": True, "action": "stop_all", "results": results}


def fleet_sequences_status(_: None = Depends(_require_a2a_auth)) -> Dict[str, Any]:
    if FLEET_SEQUENCES is None:
        raise HTTPException(
            status_code=500, detail="Fleet sequence service not initialized."
        )
    return {"ok": True, "status": FLEET_SEQUENCES.status().__dict__}


def fleet_sequences_start(
    req: FleetSequenceStartRequest, _: None = Depends(_require_a2a_auth)
) -> Dict[str, Any]:
    if FLEET_SEQUENCES is None:
        raise HTTPException(
            status_code=500, detail="Fleet sequence service not initialized."
        )
    try:
        st = FLEET_SEQUENCES.start(
            file=req.file,
            loop=req.loop,
            targets=req.targets,
            include_self=req.include_self,
            timeout_s=req.timeout_s,
        )
        _persist_runtime_state(
            "fleet_sequences_start",
            {"file": req.file, "loop": bool(req.loop), "targets": req.targets},
        )
        return {"ok": True, "status": st.__dict__}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


def fleet_sequences_stop(_: None = Depends(_require_a2a_auth)) -> Dict[str, Any]:
    if FLEET_SEQUENCES is None:
        raise HTTPException(
            status_code=500, detail="Fleet sequence service not initialized."
        )
    st = FLEET_SEQUENCES.stop()
    _persist_runtime_state("fleet_sequences_stop")
    return {"ok": True, "status": st.__dict__}


# ----------------------------
# Data files helpers (UI convenience)
# ----------------------------


def files_list(
    dir: str = "",
    glob: str = "*",
    recursive: bool = False,
    limit: int = 500,
    _: None = Depends(_require_a2a_auth),
) -> Dict[str, Any]:
    try:
        base = _resolve_data_path(dir)
        if not base.exists():
            return {"ok": True, "files": []}
        if not base.is_dir():
            raise HTTPException(
                status_code=400, detail="dir must be a directory under DATA_DIR"
            )

        pattern = (glob or "*").strip() or "*"
        it = base.rglob(pattern) if bool(recursive) else base.glob(pattern)

        root = Path(SETTINGS.data_dir).resolve()
        out: List[str] = []
        for p in it:
            try:
                rp = p.resolve()
            except Exception:
                continue
            if not rp.is_file():
                continue
            try:
                rel = str(rp.relative_to(root))
            except Exception:
                continue
            out.append(rel)
            if len(out) >= max(1, int(limit)):
                break

        out.sort()
        return {"ok": True, "files": out}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def files_download(path: str, _: None = Depends(_require_a2a_auth)) -> FileResponse:
    p = _resolve_data_path(path)
    if not p.is_file():
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(path=str(p), filename=p.name)


async def files_upload(
    path: str,
    request: Request,
    overwrite: bool = False,
    _: None = Depends(_require_a2a_auth),
) -> Dict[str, Any]:
    """
    Upload a file to DATA_DIR.

    This endpoint accepts raw bytes (e.g. Content-Type: application/octet-stream) to avoid
    requiring multipart parsing dependencies.
    """
    root = Path(_require_settings().data_dir).resolve()
    dest = _resolve_data_path(path)
    if dest == root or dest.is_dir():
        raise HTTPException(
            status_code=400, detail="path must be a file under DATA_DIR"
        )

    try:
        dest.parent.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create parent dir: {e}")

    if dest.exists() and not overwrite:
        raise HTTPException(
            status_code=409, detail="File already exists (set overwrite=true)"
        )

    tmp = dest.with_name(f".{dest.name}.uploading-{uuid.uuid4().hex}")
    total = 0
    try:
        with open(tmp, "wb") as f:
            async for chunk in request.stream():
                if not chunk:
                    continue
                total += len(chunk)
                f.write(chunk)
        os.replace(tmp, dest)
    finally:
        try:
            if tmp.exists():
                tmp.unlink()
        except Exception:
            pass

    try:
        rel = str(dest.resolve().relative_to(root))
    except Exception:
        rel = str(dest)
    return {"ok": True, "path": rel, "bytes": total}


def files_delete(path: str, _: None = Depends(_require_a2a_auth)) -> Dict[str, Any]:
    p = _resolve_data_path(path)
    if not p.exists():
        return {"ok": True, "deleted": False}
    if not p.is_file():
        raise HTTPException(
            status_code=400, detail="path must be a file under DATA_DIR"
        )
    try:
        p.unlink()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete file: {e}")
    return {"ok": True, "deleted": True}


# ----------------------------
# Content packs (zip ingestion)
# ----------------------------


def _zipinfo_is_symlink(info: zipfile.ZipInfo) -> bool:
    # Only works for Unix-style zips; safe default is "not a symlink".
    mode = (int(getattr(info, "external_attr", 0)) >> 16) & 0o170000
    return mode == 0o120000


async def packs_ingest(
    request: Request,
    dest_dir: str,
    overwrite: bool = False,
    _: None = Depends(_require_a2a_auth),
) -> Dict[str, Any]:
    """
    Upload a zip file and unpack it into a dedicated folder under DATA_DIR.

    - The zip is streamed to disk first (no full buffering).
    - Extraction is staged, then atomically renamed into place for rollback safety.
    """
    dest_rel = str(dest_dir or "").strip().strip("/")
    if not dest_rel:
        raise HTTPException(status_code=400, detail="dest_dir is required")

    root = Path(SETTINGS.data_dir).resolve()
    final_dir = _resolve_data_path(dest_rel)
    if final_dir == root:
        raise HTTPException(
            status_code=400, detail="dest_dir must not be DATA_DIR root"
        )

    parent = final_dir.parent
    try:
        parent.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create parent dir: {e}")

    if final_dir.exists():
        if not bool(overwrite):
            raise HTTPException(
                status_code=409,
                detail="Destination already exists (set overwrite=true)",
            )
        if not final_dir.is_dir():
            raise HTTPException(status_code=400, detail="dest_dir must be a directory")

    ingest_id = uuid.uuid4().hex
    staging_dir = parent / f".ingest-{final_dir.name}-{ingest_id}"
    tmp_zip = parent / f".ingest-{final_dir.name}-{ingest_id}.zip"

    total_bytes = 0
    try:
        with open(tmp_zip, "wb") as f:
            async for chunk in request.stream():
                if not chunk:
                    continue
                total_bytes += len(chunk)
                f.write(chunk)

        try:
            staging_dir.mkdir(parents=True, exist_ok=False)
        except Exception as e:
            raise HTTPException(
                status_code=500, detail=f"Failed to create staging dir: {e}"
            )

        max_files = max(1, int(os.environ.get("PACK_MAX_FILES", "4000") or "4000"))
        max_unpacked_mb = float(os.environ.get("PACK_MAX_UNPACKED_MB", "500") or "500")
        max_unpacked_bytes = int(max(1.0, max_unpacked_mb) * 1024 * 1024)

        extracted: list[str] = []
        unpacked_bytes = 0

        with zipfile.ZipFile(tmp_zip) as zf:
            infos = zf.infolist()
            if len(infos) > max_files:
                raise HTTPException(
                    status_code=400, detail="Zip contains too many entries"
                )

            file_infos: list[tuple[zipfile.ZipInfo, PurePosixPath]] = []
            for info in infos:
                name = str(getattr(info, "filename", "") or "")
                if not name:
                    continue
                # Normalize Windows zip paths.
                name = name.replace("\\", "/")
                if name.endswith("/"):
                    continue
                if getattr(info, "is_dir", None) and info.is_dir():
                    continue
                if _zipinfo_is_symlink(info):
                    raise HTTPException(
                        status_code=400, detail="Zip contains a symlink entry"
                    )

                p = PurePosixPath(name)
                if p.is_absolute() or ".." in p.parts:
                    raise HTTPException(
                        status_code=400, detail="Zip contains unsafe paths"
                    )
                if not p.parts:
                    continue
                # Reject Windows drive-letter like "C:".
                if ":" in p.parts[0]:
                    raise HTTPException(
                        status_code=400, detail="Zip contains unsafe paths"
                    )

                unpacked_bytes += int(getattr(info, "file_size", 0) or 0)
                if unpacked_bytes > max_unpacked_bytes:
                    raise HTTPException(status_code=400, detail="Zip unpacks too large")
                file_infos.append((info, p))

            for info, rel_posix in file_infos:
                out_path = staging_dir.joinpath(*rel_posix.parts)
                try:
                    rp = out_path.resolve()
                except Exception:
                    raise HTTPException(
                        status_code=400, detail="Zip contains invalid paths"
                    )
                if staging_dir.resolve() not in rp.parents:
                    raise HTTPException(
                        status_code=400, detail="Zip contains unsafe paths"
                    )

                out_path.parent.mkdir(parents=True, exist_ok=True)
                with zf.open(info, "r") as src, open(out_path, "wb") as dst:
                    shutil.copyfileobj(src, dst, length=1024 * 1024)
                extracted.append(rel_posix.as_posix())

        manifest = {
            "ok": True,
            "service": SERVICE_NAME,
            "version": APP_VERSION,
            "ingest_id": ingest_id,
            "dest_dir": dest_rel,
            "uploaded_bytes": total_bytes,
            "unpacked_bytes": unpacked_bytes,
            "files": sorted(extracted),
            "created_at": time.time(),
        }
        write_json(str(staging_dir / "manifest.json"), manifest)

        if final_dir.exists() and bool(overwrite):
            shutil.rmtree(final_dir)
        os.replace(str(staging_dir), str(final_dir))

        return {
            "ok": True,
            "dest_dir": dest_rel,
            "uploaded_bytes": total_bytes,
            "unpacked_bytes": unpacked_bytes,
            "files": sorted(extracted),
            "manifest": f"{dest_rel.rstrip('/')}/manifest.json",
        }
    finally:
        try:
            if tmp_zip.exists():
                tmp_zip.unlink()
        except Exception:
            pass
        try:
            if staging_dir.exists():
                shutil.rmtree(staging_dir)
        except Exception:
            pass


def runtime_state(_: None = Depends(_require_a2a_auth)) -> Dict[str, Any]:
    try:
        try:
            if KV_STORE is not None:
                row = KV_STORE.get_json(_KV_RUNTIME_STATE_KEY)
                if row:
                    return row
        except Exception:
            pass
        p = Path(_RUNTIME_STATE_PATH)
        if not p.is_file():
            return {"ok": True, "exists": False}
        return read_json(str(p))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ----------------------------
# Scheduler (show automation)
# ----------------------------

_SCHEDULER_CONFIG_PATH = ""
_KV_SCHEDULER_CONFIG_KEY = "scheduler_config"


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


def _hhmm_to_minutes(value: str) -> int:
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
        self, config_path: str, *, kv_store: Any = None, kv_key: str = ""
    ) -> None:
        self._config_path = str(config_path)
        self._kv_store = kv_store
        self._kv_key = str(kv_key) if kv_key else ""
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
            start_min = _hhmm_to_minutes(str(cfg.get("start_hhmm")))
            end_min = _hhmm_to_minutes(str(cfg.get("end_hhmm")))
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
            # tick interval
            self._stop.wait(timeout=1.0)

    def _tick(self, cfg: SchedulerConfig) -> None:
        if not cfg.enabled:
            with self._lock:
                self._window_active = False
            return

        start_min = _hhmm_to_minutes(cfg.start_hhmm)
        end_min = _hhmm_to_minutes(cfg.end_hhmm)
        now_min = _now_minutes_local()
        active_now = _in_window(now_min, start_min, end_min)

        with self._lock:
            prev_active = self._window_active
            self._window_active = active_now

        if active_now and not prev_active:
            # entering window: act immediately
            self._execute_action(cfg, reason="enter_window")
            return

        if (not active_now) and prev_active:
            # leaving window: optional stop_all
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
            self._ensure_sequence(cfg)

    def _stop_all(self, cfg: SchedulerConfig, *, reason: str) -> None:
        try:
            if cfg.scope == "fleet":
                fleet_stop_all(
                    FleetStopAllRequest(
                        targets=cfg.targets, include_self=cfg.include_self
                    ),
                    None,
                )
            else:
                _a2a_action_stop_all({})
                _persist_runtime_state("scheduler_stop_all", {"reason": reason})
            with self._lock:
                self._last_error = None
        except Exception as e:
            with self._lock:
                self._last_error = str(e)

    def _execute_action(self, cfg: SchedulerConfig, *, reason: str) -> None:
        if cfg.mode == "looks":
            self._apply_random_look(cfg, reason=reason)
            return
        if cfg.mode == "sequence":
            self._ensure_sequence(cfg)
            return

    def _apply_random_look(self, cfg: SchedulerConfig, *, reason: str) -> None:
        try:
            bri = (
                min(SETTINGS.wled_max_bri, cfg.brightness)
                if cfg.brightness is not None
                else None
            )
            if cfg.scope == "fleet":
                fleet_apply_random_look(
                    FleetApplyRandomLookRequest(
                        theme=cfg.theme,
                        brightness=bri,
                        targets=cfg.targets,
                        include_self=cfg.include_self,
                    ),
                    None,
                )
            else:
                COOLDOWN.wait()
                LOOKS.apply_random(theme=cfg.theme, brightness=bri)
                _persist_runtime_state(
                    "scheduler_apply_random_look", {"reason": reason}
                )

            with self._lock:
                self._last_action_at = time.time()
                self._last_action = f"apply_random_look({cfg.scope})"
                self._last_error = None
        except Exception as e:
            with self._lock:
                self._last_error = str(e)

    def _ensure_sequence(self, cfg: SchedulerConfig) -> None:
        file = (cfg.sequence_file or "").strip()
        if not file:
            with self._lock:
                self._last_error = "sequence_file is required for mode=sequence"
            return

        try:
            if cfg.scope == "fleet":
                if FLEET_SEQUENCES is None:
                    raise RuntimeError("Fleet sequence service not initialized")
                st = FLEET_SEQUENCES.status()
                if (
                    (not st.running)
                    or (st.file != file)
                    or (bool(st.loop) != bool(cfg.sequence_loop))
                ):
                    FLEET_SEQUENCES.start(
                        file=file,
                        loop=bool(cfg.sequence_loop),
                        targets=cfg.targets,
                        include_self=cfg.include_self,
                    )
                    _persist_runtime_state(
                        "scheduler_start_fleet_sequence", {"file": file}
                    )
            else:
                st = SEQUENCES.status()
                if (
                    (not st.running)
                    or (st.file != file)
                    or (bool(st.loop) != bool(cfg.sequence_loop))
                ):
                    SEQUENCES.play(file=file, loop=bool(cfg.sequence_loop))
                    _persist_runtime_state("scheduler_play_sequence", {"file": file})

            with self._lock:
                self._last_action_at = time.time()
                self._last_action = f"ensure_sequence({cfg.scope})"
                self._last_error = None
        except Exception as e:
            with self._lock:
                self._last_error = str(e)


SCHEDULER: SchedulerService | None = None


def _scheduler_init() -> None:
    global _SCHEDULER_CONFIG_PATH, SCHEDULER
    settings = SETTINGS
    if settings is None:
        return
    _SCHEDULER_CONFIG_PATH = os.path.join(settings.data_dir, "show", "scheduler.json")
    SCHEDULER = SchedulerService(
        _SCHEDULER_CONFIG_PATH, kv_store=KV_STORE, kv_key=_KV_SCHEDULER_CONFIG_KEY
    )


def _scheduler_startup() -> None:
    if SCHEDULER is None:
        return
    cfg = SCHEDULER.get_config()
    if cfg.autostart and cfg.enabled:
        SCHEDULER.start()


def _scheduler_shutdown() -> None:
    try:
        if SCHEDULER is not None:
            SCHEDULER.stop()
    except Exception:
        pass


def scheduler_status(_: None = Depends(_require_a2a_auth)) -> Dict[str, Any]:
    if SCHEDULER is None:
        raise HTTPException(status_code=503, detail="Scheduler not initialized")
    return SCHEDULER.status()


def scheduler_get_config(_: None = Depends(_require_a2a_auth)) -> Dict[str, Any]:
    if SCHEDULER is None:
        raise HTTPException(status_code=503, detail="Scheduler not initialized")
    return {"ok": True, "config": SCHEDULER.get_config().model_dump()}


def scheduler_set_config(
    cfg: SchedulerConfig, _: None = Depends(_require_a2a_auth)
) -> Dict[str, Any]:
    if SCHEDULER is None:
        raise HTTPException(status_code=503, detail="Scheduler not initialized")
    # Validate times early to return a friendly 400.
    try:
        _hhmm_to_minutes(cfg.start_hhmm)
        _hhmm_to_minutes(cfg.end_hhmm)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    SCHEDULER.set_config(cfg, persist=True)
    return {"ok": True, "config": cfg.model_dump()}


def scheduler_start(_: None = Depends(_require_a2a_auth)) -> Dict[str, Any]:
    if SCHEDULER is None:
        raise HTTPException(status_code=503, detail="Scheduler not initialized")
    SCHEDULER.start()
    return SCHEDULER.status()


def scheduler_stop(_: None = Depends(_require_a2a_auth)) -> Dict[str, Any]:
    if SCHEDULER is None:
        raise HTTPException(status_code=503, detail="Scheduler not initialized")
    SCHEDULER.stop()
    return SCHEDULER.status()


def scheduler_run_once(_: None = Depends(_require_a2a_auth)) -> Dict[str, Any]:
    if SCHEDULER is None:
        raise HTTPException(status_code=503, detail="Scheduler not initialized")
    SCHEDULER.run_once()
    return SCHEDULER.status()


def metrics(_: None = Depends(_require_a2a_auth)) -> Dict[str, Any]:
    """
    Lightweight JSON metrics for LAN monitoring.
    """
    fleet_st = None
    try:
        if FLEET_SEQUENCES is not None:
            fleet_st = FLEET_SEQUENCES.status().__dict__
    except Exception:
        fleet_st = None

    return {
        "ok": True,
        "version": APP_VERSION,
        "uptime_s": max(0.0, time.time() - APP_STARTED_AT),
        "peers_configured": len(PEERS),
        "jobs": {"count": len(JOBS.list_jobs(limit=10_000))},
        "scheduler": SCHEDULER.status(),
        "ddp": DDP.status().__dict__,
        "sequence": SEQUENCES.status().__dict__,
        "fleet_sequence": fleet_st,
    }


# ----------------------------
# Jobs + progress (SSE)
# ----------------------------


def jobs_list(limit: int = 50, _: None = Depends(_require_a2a_auth)) -> Dict[str, Any]:
    return {"ok": True, "jobs": [j.as_dict() for j in JOBS.list_jobs(limit=limit)]}


def jobs_get(job_id: str, _: None = Depends(_require_a2a_auth)) -> Dict[str, Any]:
    j = JOBS.get(job_id)
    if not j:
        raise HTTPException(status_code=404, detail="Job not found")
    return {"ok": True, "job": j.as_dict()}


def jobs_cancel(job_id: str, _: None = Depends(_require_a2a_auth)) -> Dict[str, Any]:
    j = JOBS.cancel(job_id)
    if not j:
        raise HTTPException(status_code=404, detail="Job not found")
    return {"ok": True, "job": j.as_dict()}


async def jobs_stream(
    request: Request, _: None = Depends(_require_a2a_auth)
) -> StreamingResponse:
    q = JOBS.subscribe()

    async def gen():
        try:
            snap = jobs_snapshot_payload(JOBS.list_jobs(limit=100))
            yield sse_format_event(event="snapshot", data=snap)

            while True:
                if await request.is_disconnected():
                    break
                try:
                    msg = await asyncio.to_thread(q.get, True, 1.0)
                    yield sse_format_event(event="message", data=msg)
                except queue.Empty:
                    yield sse_format_event(event="ping", data="{}")
        finally:
            JOBS.unsubscribe(q)

    headers = {
        "Cache-Control": "no-cache",
        "X-Accel-Buffering": "no",
    }
    return StreamingResponse(gen(), media_type="text/event-stream", headers=headers)


def jobs_looks_generate(
    req: GenerateLooksRequest, _: None = Depends(_require_a2a_auth)
) -> Dict[str, Any]:
    params = req.model_dump()

    def _run(ctx):
        ctx.set_progress(
            current=0,
            total=float(params.get("total_looks") or 0),
            message="Generating looks…",
        )
        summary = LOOKS.generate_pack(
            total_looks=int(params["total_looks"]),
            themes=list(params["themes"]),
            brightness=min(SETTINGS.wled_max_bri, int(params["brightness"])),
            seed=int(params["seed"]),
            write_files=bool(params.get("write_files", True)),
            include_multi_segment=bool(params.get("include_multi_segment", True)),
            progress_cb=lambda cur, total, msg: (
                ctx.check_cancelled(),
                ctx.set_progress(current=cur, total=total, message=msg),
            ),
            cancel_cb=lambda: JOBS.is_cancel_requested(ctx.job_id),
        )
        ctx.set_progress(
            current=float(summary.total), total=float(summary.total), message="Done."
        )
        return {"summary": summary.__dict__}

    job = JOBS.create(kind="looks_generate", runner=_run)
    return {"ok": True, "job": job.as_dict()}


def jobs_audio_analyze(
    req: AudioAnalyzeRequest, _: None = Depends(_require_a2a_auth)
) -> Dict[str, Any]:
    params = req.model_dump()

    def _run(ctx):
        audio_path = _resolve_data_path(str(params["audio_file"]))
        out_path = _resolve_data_path(str(params["out_file"]))
        ctx.set_progress(message="Analyzing audio…")
        analysis = analyze_beats(
            audio_path=str(audio_path),
            min_bpm=int(params["min_bpm"]),
            max_bpm=int(params["max_bpm"]),
            hop_ms=int(params["hop_ms"]),
            window_ms=int(params["window_ms"]),
            peak_threshold=float(params["peak_threshold"]),
            min_interval_s=float(params["min_interval_s"]),
            prefer_ffmpeg=bool(params["prefer_ffmpeg"]),
            progress_cb=lambda cur, total, msg: (
                ctx.check_cancelled(),
                ctx.set_progress(current=cur, total=total, message=msg),
            ),
            cancel_cb=lambda: JOBS.is_cancel_requested(ctx.job_id),
        )

        out = analysis.as_dict()
        if analysis.bpm > 0:
            out["bpm_timeline"] = [
                {
                    "start_s": 0.0,
                    "end_s": float(analysis.duration_s),
                    "bpm": float(analysis.bpm),
                }
            ]
        else:
            out["bpm_timeline"] = []

        write_json(str(out_path), out)

        base = Path(SETTINGS.data_dir).resolve()
        rel_out = (
            str(out_path.resolve().relative_to(base))
            if base in out_path.resolve().parents
            else str(out_path)
        )
        ctx.set_progress(message="Done.")
        return {"analysis": out, "out_file": rel_out}

    job = JOBS.create(kind="audio_analyze", runner=_run)
    return {"ok": True, "job": job.as_dict()}


def jobs_xlights_import_project(
    req: XlightsImportProjectRequest, _: None = Depends(_require_a2a_auth)
) -> Dict[str, Any]:
    params = req.model_dump()

    def _run(ctx):
        ctx.set_progress(current=0, total=3, message="Reading project…")
        proj = _resolve_data_path(str(params["project_dir"]))
        if not proj.is_dir():
            raise HTTPException(
                status_code=400, detail="project_dir must be a directory under DATA_DIR"
            )

        proj_root = proj.resolve()

        def _pick(name: Optional[str], defaults: List[str]) -> Path:
            candidates = [name] if name else []
            candidates.extend([d for d in defaults if d not in candidates])
            for n in candidates:
                if not n:
                    continue
                p = (proj_root / n).resolve()
                if proj_root not in p.parents:
                    continue
                if p.is_file():
                    return p
            raise HTTPException(
                status_code=400,
                detail=f"Missing required xLights file (tried: {', '.join(candidates)})",
            )

        networks_path = _pick(params.get("networks_file"), ["xlights_networks.xml"])
        models_path = _pick(
            params.get("models_file"),
            ["xlights_rgbeffects.xml", "xlights_models.xml", "xlights_layout.xml"],
        )
        ctx.set_progress(current=1, total=3, message="Parsing networks + models…")

        controllers = import_xlights_networks_file(str(networks_path))
        models = import_xlights_models_file(str(models_path))

        ctx.set_progress(current=2, total=3, message="Writing show config…")
        cfg = show_config_from_xlights_project(
            networks=controllers,
            models=models,
            show_name=str(params.get("show_name") or "xlights-project"),
            subnet=params.get("subnet"),
            coordinator_base_url=params.get("coordinator_base_url"),
            fpp_base_url=params.get("fpp_base_url"),
            include_controllers=bool(params.get("include_controllers", True)),
            include_models=bool(params.get("include_models", True)),
        )
        out_path = write_show_config(
            data_dir=SETTINGS.data_dir, rel_path=str(params["out_file"]), config=cfg
        )
        ctx.set_progress(current=3, total=3, message="Done.")
        return {"show_config_file": out_path, "show_config": cfg.as_dict()}

    job = JOBS.create(kind="xlights_import_project", runner=_run)
    return {"ok": True, "job": job.as_dict()}


def jobs_xlights_import_networks(
    req: XlightsImportNetworksRequest, _: None = Depends(_require_a2a_auth)
) -> Dict[str, Any]:
    params = req.model_dump()

    def _run(ctx):
        ctx.set_progress(current=0, total=2, message="Parsing networks…")
        networks_path = _resolve_data_path(str(params["networks_file"]))
        controllers = import_xlights_networks_file(str(networks_path))
        cfg = show_config_from_xlights_networks(
            networks=controllers,
            show_name=str(params.get("show_name") or "xlights-import"),
            subnet=params.get("subnet"),
            coordinator_base_url=params.get("coordinator_base_url"),
            fpp_base_url=params.get("fpp_base_url"),
        )
        ctx.set_progress(current=1, total=2, message="Writing show config…")
        out_path = write_show_config(
            data_dir=SETTINGS.data_dir, rel_path=str(params["out_file"]), config=cfg
        )
        ctx.set_progress(current=2, total=2, message="Done.")
        return {
            "controllers": [
                {
                    "name": c.name,
                    "host": c.host,
                    "protocol": c.protocol,
                    "universe_start": c.universe_start,
                    "pixel_count": c.pixel_count,
                }
                for c in controllers
            ],
            "show_config_file": out_path,
            "show_config": cfg.as_dict(),
        }

    job = JOBS.create(kind="xlights_import_networks", runner=_run)
    return {"ok": True, "job": job.as_dict()}


def jobs_xlights_import_sequence(
    req: XlightsImportSequenceRequest, _: None = Depends(_require_a2a_auth)
) -> Dict[str, Any]:
    params = req.model_dump()

    def _run(ctx):
        ctx.set_progress(current=0, total=2, message="Parsing .xsq timing grid…")
        xsq_path = _resolve_data_path(str(params["xsq_file"]))
        out_path = _resolve_data_path(str(params["out_file"]))
        analysis = import_xlights_xsq_timing_file(
            xsq_path=str(xsq_path), timing_track=params.get("timing_track")
        )
        ctx.set_progress(current=1, total=2, message="Writing beats JSON…")
        write_json(str(out_path), analysis)
        base = Path(SETTINGS.data_dir).resolve()
        rel_out = (
            str(out_path.resolve().relative_to(base))
            if base in out_path.resolve().parents
            else str(out_path)
        )
        ctx.set_progress(current=2, total=2, message="Done.")
        return {"analysis": analysis, "out_file": rel_out}

    job = JOBS.create(kind="xlights_import_sequence", runner=_run)
    return {"ok": True, "job": job.as_dict()}


def jobs_sequences_generate(
    req: GenerateSequenceRequest, _: None = Depends(_require_a2a_auth)
) -> Dict[str, Any]:
    params = req.model_dump()

    def _run(ctx):
        ctx.set_progress(current=0, total=3, message="Loading looks…")
        pack = params.get("pack_file") or LOOKS.latest_pack()
        if not pack:
            raise RuntimeError("No looks pack found; generate looks first.")
        pack_path = os.path.join(SETTINGS.data_dir, "looks", str(pack))
        looks = read_jsonl(pack_path)
        if len(looks) > 2000:
            looks = looks[:2000]

        ctx.set_progress(current=1, total=3, message="Preparing patterns…")
        ddp_pats = ddp_patterns()["patterns"]

        beats_s: Optional[List[float]] = None
        if params.get("beats_file"):
            ctx.set_progress(current=1.5, total=3, message="Loading beat grid…")
            beats_path = _resolve_data_path(str(params["beats_file"]))
            beats_obj = read_json(str(beats_path))
            if not isinstance(beats_obj, dict):
                raise RuntimeError(
                    "beats_file must contain a JSON object with a beats list (beats_s or beats_ms)"
                )
            raw_beats = beats_obj.get("beats_s")
            if raw_beats is None:
                raw_beats = beats_obj.get("beats_ms")
                if raw_beats is not None:
                    try:
                        beats_s = [float(x) / 1000.0 for x in list(raw_beats)]
                    except Exception:
                        beats_s = None
            else:
                try:
                    beats_s = [float(x) for x in list(raw_beats)]
                except Exception:
                    beats_s = None
            if not beats_s or len(beats_s) < 2:
                raise RuntimeError(
                    "beats_file did not contain a usable beats list (need >= 2 marks)"
                )

        ctx.set_progress(current=2, total=3, message="Generating sequence…")
        fname = SEQUENCES.generate(
            name=str(params["name"]),
            looks=looks,
            duration_s=int(params["duration_s"]),
            step_s=int(params["step_s"]),
            include_ddp=bool(params["include_ddp"]),
            renderable_only=bool(params.get("renderable_only", False)),
            beats_s=beats_s,
            beats_per_step=int(params["beats_per_step"]),
            beat_offset_s=float(params["beat_offset_s"]),
            ddp_patterns=ddp_pats,
            seed=int(params["seed"]),
        )
        ctx.set_progress(current=3, total=3, message="Done.")
        return {"file": fname}

    job = JOBS.create(kind="sequences_generate", runner=_run)
    return {"ok": True, "job": job.as_dict()}


def jobs_fseq_export(
    req: FSEQExportRequest, _: None = Depends(_require_a2a_auth)
) -> Dict[str, Any]:
    params = req.model_dump()

    def _run(ctx):
        seq_root = _resolve_data_path("sequences").resolve()
        seq_path = (seq_root / (params.get("sequence_file") or "")).resolve()
        if seq_root not in seq_path.parents:
            raise HTTPException(
                status_code=400,
                detail="sequence_file must be within DATA_DIR/sequences",
            )
        seq = read_json(str(seq_path))
        steps: List[Dict[str, Any]] = list((seq or {}).get("steps", []))
        if not steps:
            raise HTTPException(status_code=400, detail="Sequence has no steps")

        led_count = (
            int(params["led_count"])
            if params.get("led_count") is not None
            else int(WLED.device_info().led_count)
        )
        if led_count <= 0:
            raise HTTPException(status_code=400, detail="led_count must be > 0")
        payload_len = led_count * 3

        channel_start = int(params["channel_start"])
        if channel_start <= 0:
            raise HTTPException(status_code=400, detail="channel_start must be >= 1")

        channels_total = (
            int(params["channels_total"])
            if params.get("channels_total") is not None
            else (channel_start - 1 + payload_len)
        )
        if channels_total < (channel_start - 1 + payload_len):
            raise HTTPException(
                status_code=400,
                detail="channels_total is too small for channel_start + led_count*3",
            )

        step_ms = int(params["step_ms"])
        default_bri = min(
            SETTINGS.wled_max_bri, max(1, int(params["default_brightness"]))
        )

        per_step_frames: List[int] = []
        total_frames = 0
        for step in steps:
            dur_s = float(step.get("duration_s", 0.0))
            if dur_s <= 0:
                dur_s = 0.1
            n = max(1, int(math.ceil((dur_s * 1000.0) / max(1, step_ms))))
            per_step_frames.append(n)
            total_frames += n

        ctx.set_progress(
            current=0, total=float(total_frames), message="Rendering frames…"
        )

        layout = None
        try:
            from segment_layout import fetch_segment_layout

            layout = fetch_segment_layout(WLED, segment_ids=SEGMENT_IDS, refresh=False)
        except Exception:
            layout = None

        from patterns import PatternFactory

        factory = PatternFactory(
            led_count=led_count, geometry=GEOM, segment_layout=layout
        )

        out_path = _resolve_data_path(str(params["out_file"]))

        frame_idx = 0
        last_report = 0

        def _frames():
            nonlocal frame_idx, last_report
            off = channel_start - 1
            for step, nframes in zip(steps, per_step_frames):
                ctx.check_cancelled()
                typ = str(step.get("type") or "").strip().lower()
                if typ != "ddp":
                    raise RuntimeError(
                        f"Non-renderable step type '{typ}' (only 'ddp' is supported for fseq export)."
                    )
                pat_name = str(step.get("pattern") or "").strip()
                if not pat_name:
                    raise RuntimeError("DDP step missing 'pattern'")
                params2 = step.get("params") or {}
                if not isinstance(params2, dict):
                    params2 = {}
                bri = step.get("brightness")
                bri_i = (
                    default_bri
                    if bri is None
                    else min(SETTINGS.wled_max_bri, max(1, int(bri)))
                )

                pat = factory.create(pat_name, params=params2)
                for i in range(int(nframes)):
                    ctx.check_cancelled()
                    t = (i * step_ms) / 1000.0
                    rgb = pat.frame(t=t, frame_idx=frame_idx, brightness=bri_i)
                    if len(rgb) != payload_len:
                        rgb = (rgb[:payload_len]).ljust(payload_len, b"\x00")
                    frame = bytearray(channels_total)
                    end = min(channels_total, off + payload_len)
                    frame[off:end] = rgb[: (end - off)]
                    frame_idx += 1
                    # Update progress occasionally to keep overhead low.
                    if frame_idx - last_report >= 250:
                        last_report = frame_idx
                        ctx.set_progress(
                            current=float(frame_idx),
                            total=float(total_frames),
                            message="Rendering frames…",
                        )
                    yield bytes(frame)

        res = write_fseq_v1_file(
            out_path=str(out_path),
            channel_count=channels_total,
            num_frames=total_frames,
            step_ms=step_ms,
            frame_generator=_frames(),
        )
        ctx.set_progress(
            current=float(total_frames), total=float(total_frames), message="Done."
        )
        return {
            "source_sequence": seq_path.name,
            "render": {
                "led_count": led_count,
                "channel_start": channel_start,
                "channels_total": channels_total,
                "step_ms": step_ms,
            },
            "fseq": res.__dict__,
            "out_file": str(
                Path(out_path).resolve().relative_to(Path(SETTINGS.data_dir).resolve())
            ),
        }

    job = JOBS.create(kind="fseq_export", runner=_run)
    return {"ok": True, "job": job.as_dict()}
