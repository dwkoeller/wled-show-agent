from __future__ import annotations

import os
import math
import re
import asyncio
import queue
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from fastapi import Depends, FastAPI, Header, HTTPException, Request, Response
from fastapi.responses import (
    FileResponse,
    JSONResponse,
    RedirectResponse,
    StreamingResponse,
)
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

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


app = FastAPI(title="WLED Show Agent v3", version="3.4.0")


# ----------------------------
# Dependency singletons
# ----------------------------
SETTINGS: Settings = load_settings()
ensure_dir(SETTINGS.data_dir)
JOBS = JobManager()


@app.middleware("http")
async def _auth_middleware(request: Request, call_next):  # type: ignore[no-untyped-def]
    if not SETTINGS.auth_enabled:
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
    key = SETTINGS.a2a_api_key
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
                secret=str(SETTINGS.auth_jwt_secret or ""),
                issuer=str(SETTINGS.auth_jwt_issuer or ""),
            )
            return await call_next(request)
        except AuthError:
            pass

    return JSONResponse(status_code=401, content={"ok": False, "error": "unauthorized"})


if SETTINGS.controller_kind != "wled":
    raise RuntimeError(
        "This app controls WLED devices. For ESPixelStick pixel controllers, run pixel_main:app (CONTROLLER_KIND=pixel)."
    )

WLED = WLEDClient(SETTINGS.wled_tree_url, timeout_s=SETTINGS.wled_http_timeout_s)
MAPPER = WLEDMapper(WLED)

# Segment IDs (WLED 0.15+ returns segment list in /json/state). If not configured,
# we attempt to auto-detect from WLED and fall back to [0] if offline.
SEGMENT_IDS: List[int] = list(SETTINGS.wled_segment_ids)
if not SEGMENT_IDS:
    try:
        SEGMENT_IDS = WLED.get_segment_ids(refresh=True)
    except Exception:
        SEGMENT_IDS = []
if not SEGMENT_IDS:
    SEGMENT_IDS = [0]


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


PEERS: Dict[str, A2APeer] = _parse_a2a_peers(list(SETTINGS.a2a_peers))


def _require_a2a_auth(
    request: Request,
    x_a2a_key: str | None = Header(default=None, alias="X-A2A-Key"),
    authorization: str | None = Header(default=None, alias="Authorization"),
) -> None:
    key = SETTINGS.a2a_api_key
    candidate = x_a2a_key
    if (not candidate) and authorization:
        parts = str(authorization).strip().split(None, 1)
        if len(parts) == 2 and parts[0].lower() == "bearer":
            candidate = parts[1].strip()

    # If JWT auth is enabled, allow either a valid A2A key (if set) or a valid JWT.
    if SETTINGS.auth_enabled:
        if key and candidate == key:
            return
        tok = _jwt_from_request(request)
        if not tok:
            raise HTTPException(status_code=401, detail="Missing token")
        try:
            jwt_decode_hs256(
                tok,
                secret=str(SETTINGS.auth_jwt_secret or ""),
                issuer=str(SETTINGS.auth_jwt_issuer or ""),
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
            right_segment_id=int(SETTINGS.quad_right_segment_id),
            order_direction_from_street=str(SETTINGS.quad_order_from_street),
        )
    except Exception:
        return None


LOOKS = LookService(
    wled=WLED,
    mapper=MAPPER,
    data_dir=SETTINGS.data_dir,
    max_bri=SETTINGS.wled_max_bri,
    segment_ids=SEGMENT_IDS,
    replicate_to_all_segments=SETTINGS.wled_replicate_to_all_segments,
)

COOLDOWN = Cooldown(SETTINGS.wled_command_cooldown_ms)
IMPORTER = PresetImporter(
    wled=WLED,
    mapper=MAPPER,
    cooldown=COOLDOWN,
    max_bri=SETTINGS.wled_max_bri,
    segment_ids=SEGMENT_IDS,
    replicate_to_all_segments=SETTINGS.wled_replicate_to_all_segments,
)

GEOM = TreeGeometry(
    runs=SETTINGS.tree_runs,
    pixels_per_run=SETTINGS.tree_pixels_per_run,
    segment_len=SETTINGS.tree_segment_len,
    segments_per_run=SETTINGS.tree_segments_per_run,
)

DDP_CFG = DDPConfig(
    host=SETTINGS.ddp_host,
    port=SETTINGS.ddp_port,
    destination_id=SETTINGS.ddp_destination_id,
    max_pixels_per_packet=SETTINGS.ddp_max_pixels_per_packet,
)

DDP = DDPStreamer(
    wled=WLED,
    geometry=GEOM,
    ddp_cfg=DDP_CFG,
    fps_default=SETTINGS.ddp_fps_default,
    fps_max=SETTINGS.ddp_fps_max,
    segment_ids=SEGMENT_IDS,
)

SEQUENCES = SequenceService(wled=WLED, looks=LOOKS, ddp=DDP, data_dir=SETTINGS.data_dir)

FPP: FPPClient | None = None
if SETTINGS.fpp_base_url:
    FPP = FPPClient(
        base_url=SETTINGS.fpp_base_url,
        timeout_s=SETTINGS.fpp_http_timeout_s,
        headers={k: v for (k, v) in SETTINGS.fpp_headers},
    )

FLEET_SEQUENCES: FleetSequenceService | None = None


DIRECTOR = None
if SETTINGS.openai_api_key and SimpleDirectorAgent is not None:

    def _tool_apply_random_look(kwargs: Dict[str, Any]) -> Any:
        # If peers are configured, keep devices visually consistent by picking a look_spec locally
        # and broadcasting that exact spec to all peers.
        if PEERS:
            pack, row = LOOKS.choose_random(
                theme=kwargs.get("theme"), seed=kwargs.get("seed")
            )
            bri = kwargs.get("brightness")
            bri_i: Optional[int] = None
            if bri is not None:
                bri_i = min(SETTINGS.wled_max_bri, max(1, int(bri)))

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
                COOLDOWN.wait()
                out["self"] = {
                    "ok": True,
                    "result": LOOKS.apply_look(row, brightness_override=bri_i),
                }
            except Exception as e:
                out["self"] = {"ok": False, "error": str(e)}

            # apply to peers
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
            return out

        return LOOKS.apply_random(
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
            default_start_pos=str(SETTINGS.quad_default_start_pos),
        )

        return DDP.start(
            pattern=str(kwargs.get("pattern")),
            params=params,
            duration_s=float(kwargs.get("duration_s", 30.0)),
            brightness=min(SETTINGS.wled_max_bri, int(kwargs.get("brightness", 128))),
            fps=float(kwargs.get("fps", SETTINGS.ddp_fps_default)),
        ).__dict__

    def _tool_stop_ddp(kwargs: Dict[str, Any]) -> Any:
        return DDP.stop().__dict__

    def _tool_stop_all(kwargs: Dict[str, Any]) -> Any:
        # Stop locally, and if peers exist stop them too.
        out: Dict[str, Any] = {}
        try:
            out["self"] = {"ok": True, "result": _a2a_action_stop_all({})}
        except Exception as e:
            out["self"] = {"ok": False, "error": str(e)}

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
        bri = int(kwargs.get("brightness", SETTINGS.wled_max_bri))
        seed = int(kwargs.get("seed", 1337))
        return LOOKS.generate_pack(
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
        api_key=SETTINGS.openai_api_key,
        model=SETTINGS.openai_model,
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
# Models
# ----------------------------


class ApplyStateRequest(BaseModel):
    state: Dict[str, Any] = Field(
        ..., description="Partial or full WLED /json/state payload"
    )


class GenerateLooksRequest(BaseModel):
    total_looks: int = Field(800, ge=50, le=5000)
    themes: List[str] = Field(
        default_factory=lambda: [
            "classic",
            "candy_cane",
            "icy",
            "warm_white",
            "rainbow",
        ]
    )
    brightness: int = Field(180, ge=1, le=255)
    seed: int = Field(1337)
    write_files: bool = True
    include_multi_segment: bool = True


class ApplyRandomLookRequest(BaseModel):
    theme: Optional[str] = None
    pack_file: Optional[str] = None
    brightness: Optional[int] = Field(default=None, ge=1, le=255)
    seed: Optional[int] = None


class ImportPresetsRequest(BaseModel):
    pack_file: str
    start_id: int = Field(120, ge=1, le=250)
    limit: int = Field(50, ge=1, le=250)
    name_prefix: str = "AI"
    include_brightness: bool = True
    save_bounds: bool = True


class DDPStartRequest(BaseModel):
    pattern: str
    params: Dict[str, Any] = Field(default_factory=dict)
    duration_s: float = Field(30.0, ge=0.1, le=600.0)
    brightness: int = Field(128, ge=1, le=255)
    fps: Optional[float] = Field(default=None, ge=1.0, le=60.0)
    direction: Optional[str] = Field(
        default=None, description="Rotation direction from street: cw or ccw"
    )
    start_pos: Optional[str] = Field(
        default=None, description="Start position from street: front/right/back/left"
    )


class GoCrazyRequest(BaseModel):
    total_looks: int = Field(1500, ge=50, le=8000)
    themes: List[str] = Field(
        default_factory=lambda: [
            "classic",
            "candy_cane",
            "icy",
            "warm_white",
            "rainbow",
            "halloween",
        ]
    )
    write_files: bool = True
    import_presets: bool = False
    import_start_id: int = Field(120, ge=1, le=250)
    import_limit: int = Field(50, ge=1, le=250)
    sequences: int = Field(10, ge=0, le=100)
    sequence_duration_s: int = Field(240, ge=10, le=3600)
    step_s: int = Field(8, ge=1, le=60)
    include_ddp: bool = True
    brightness: int = Field(180, ge=1, le=255)
    seed: int = Field(1337)


class GenerateSequenceRequest(BaseModel):
    name: str = "Mix"
    pack_file: Optional[str] = None
    duration_s: int = Field(240, ge=10, le=3600)
    step_s: int = Field(8, ge=1, le=60)
    include_ddp: bool = True
    renderable_only: bool = Field(
        default=False,
        description="If true, generate a sequence that only uses renderable procedural patterns (no WLED look steps).",
    )
    beats_file: Optional[str] = Field(
        default=None,
        description="Optional: beats timeline JSON under DATA_DIR (from /v1/audio/analyze or /v1/xlights/import_sequence). "
        "If set, step durations are derived from the beat grid instead of fixed step_s.",
    )
    beats_per_step: int = Field(
        4,
        ge=1,
        le=64,
        description="When beats_file is set, how many beats each step spans.",
    )
    beat_offset_s: float = Field(
        0.0,
        ge=-60.0,
        le=60.0,
        description="Optional offset applied to beat timestamps.",
    )
    seed: int = Field(1337)


class PlaySequenceRequest(BaseModel):
    file: str
    loop: bool = False


class CommandRequest(BaseModel):
    text: str


class AuthLoginRequest(BaseModel):
    username: str
    password: str
    totp: Optional[str] = None


class FPPStartPlaylistRequest(BaseModel):
    name: str
    repeat: bool = False


class FPPTriggerEventRequest(BaseModel):
    event_id: int = Field(..., ge=1)


class FPPProxyRequest(BaseModel):
    method: str = Field(
        "GET", description="HTTP method to use against FPP (GET/POST/PUT/DELETE)."
    )
    path: str = Field(
        ..., description="Path on the FPP server (e.g. /api/fppd/status)."
    )
    params: Dict[str, Any] = Field(default_factory=dict)
    json_body: Any = None


class FSEQExportRequest(BaseModel):
    sequence_file: str = Field(
        ..., description="Sequence JSON filename under DATA_DIR/sequences."
    )
    out_file: str = Field(
        "fseq/out.fseq", description="Output path relative to DATA_DIR."
    )
    step_ms: int = Field(
        50,
        ge=10,
        le=255,
        description="Frame interval in milliseconds (FSEQ v1 supports 1..255).",
    )
    channel_start: int = Field(
        1,
        ge=1,
        description="1-based start channel for this rendered prop within the FSEQ.",
    )
    channels_total: Optional[int] = Field(
        default=None,
        ge=1,
        description="Optional total channel count; defaults to channel_start-1 + led_count*3.",
    )
    led_count: Optional[int] = Field(
        default=None,
        ge=1,
        description="Optional LED count to render; defaults to WLED reported led_count.",
    )
    default_brightness: int = Field(128, ge=1, le=255)


class FPPUploadFileRequest(BaseModel):
    local_file: str = Field(
        ...,
        description="Path relative to DATA_DIR to upload to FPP (e.g. fseq/out.fseq).",
    )
    dir: str = Field(
        "sequences",
        description="FPP media dir (e.g. sequences, music, playlists, scripts).",
    )
    subdir: Optional[str] = Field(
        default=None, description="Optional subdirectory under the FPP dir."
    )
    dest_filename: Optional[str] = Field(
        default=None,
        description="Optional filename on FPP; defaults to local_file basename.",
    )


class AudioAnalyzeRequest(BaseModel):
    audio_file: str = Field(
        ..., description="Path relative to DATA_DIR (wav/mp3/ogg if ffmpeg available)."
    )
    out_file: str = Field(
        "audio/beats.json", description="Output path relative to DATA_DIR."
    )
    min_bpm: int = Field(60, ge=20, le=400)
    max_bpm: int = Field(200, ge=20, le=400)
    hop_ms: int = Field(10, ge=5, le=100)
    window_ms: int = Field(50, ge=10, le=500)
    peak_threshold: float = Field(
        1.35, ge=0.1, le=10.0, description="Higher means fewer beats detected."
    )
    min_interval_s: float = Field(0.20, ge=0.05, le=2.0)
    prefer_ffmpeg: bool = True


class ShowConfigLoadRequest(BaseModel):
    file: str = Field(
        ..., description="Path relative to DATA_DIR (e.g. show/show_config.json)"
    )


class XlightsImportNetworksRequest(BaseModel):
    networks_file: str = Field(
        ..., description="Path relative to DATA_DIR (e.g. xlights/xlights_networks.xml)"
    )
    out_file: str = Field(
        "show/show_config_xlights.json", description="Output path relative to DATA_DIR"
    )
    show_name: str = "xlights-import"
    subnet: Optional[str] = "172.16.200.0/24"
    coordinator_base_url: Optional[str] = Field(
        default=None,
        description="Coordinator URL as reachable from FPP (e.g. http://172.16.200.10:8088)",
    )
    fpp_base_url: Optional[str] = Field(
        default=None, description="Optional: FPP base URL (e.g. http://172.16.200.20)"
    )


class XlightsImportProjectRequest(BaseModel):
    project_dir: str = Field(
        ..., description="Directory under DATA_DIR containing xLights project files."
    )
    networks_file: Optional[str] = Field(
        default=None,
        description="Optional override filename under project_dir (default: xlights_networks.xml).",
    )
    models_file: Optional[str] = Field(
        default=None,
        description="Optional override filename under project_dir (default: xlights_rgbeffects.xml).",
    )
    out_file: str = Field(
        "show/show_config_xlights_project.json",
        description="Output path relative to DATA_DIR",
    )
    show_name: str = "xlights-project"
    include_controllers: bool = True
    include_models: bool = True
    subnet: Optional[str] = "172.16.200.0/24"
    coordinator_base_url: Optional[str] = Field(
        default=None, description="Optional: coordinator URL for FPP scripts/etc."
    )
    fpp_base_url: Optional[str] = Field(
        default=None, description="Optional: FPP base URL (e.g. http://172.16.200.20)"
    )


class XlightsImportSequenceRequest(BaseModel):
    xsq_file: str = Field(
        ..., description="Path relative to DATA_DIR to an xLights .xsq file."
    )
    timing_track: Optional[str] = Field(
        default=None,
        description="Optional timing track name to use (defaults to best/longest).",
    )
    out_file: str = Field(
        "audio/beats_xlights.json",
        description="Output path relative to DATA_DIR (beats timeline JSON).",
    )


class FleetSequenceStartRequest(BaseModel):
    file: str
    loop: bool = False
    targets: Optional[List[str]] = None
    include_self: bool = True
    timeout_s: Optional[float] = Field(default=None, ge=0.1, le=30.0)


class FPPExportFleetSequenceScriptRequest(BaseModel):
    sequence_file: str = Field(
        ...,
        description="A generated sequence file in DATA_DIR/sequences (e.g. sequence_Mix_*.json)",
    )
    coordinator_base_url: Optional[str] = Field(
        default=None, description="Coordinator URL as reachable from the FPP host."
    )
    show_config_file: Optional[str] = Field(
        default=None,
        description="Optional: load coordinator URL from a show config file under DATA_DIR.",
    )
    out_filename: str = Field(
        "start_fleet_sequence.sh",
        description="Script filename to write under DATA_DIR/fpp/scripts",
    )
    include_a2a_key: bool = Field(
        default=False, description="If true, embed A2A_API_KEY in the script header."
    )
    loop: bool = False
    targets: Optional[List[str]] = None
    include_self: bool = True


class FPPExportFleetStopAllScriptRequest(BaseModel):
    coordinator_base_url: Optional[str] = Field(
        default=None, description="Coordinator URL as reachable from the FPP host."
    )
    show_config_file: Optional[str] = Field(
        default=None,
        description="Optional: load coordinator URL from a show config file under DATA_DIR.",
    )
    out_filename: str = Field(
        "fleet_stop_all.sh",
        description="Script filename to write under DATA_DIR/fpp/scripts",
    )
    include_a2a_key: bool = Field(
        default=False, description="If true, embed A2A_API_KEY in the script header."
    )
    targets: Optional[List[str]] = None
    include_self: bool = True


class A2AInvokeRequest(BaseModel):
    action: str
    params: Dict[str, Any] = Field(default_factory=dict)
    request_id: Optional[str] = None


class FleetInvokeRequest(BaseModel):
    action: str
    params: Dict[str, Any] = Field(default_factory=dict)
    targets: Optional[List[str]] = None  # peer names; default: all configured peers
    include_self: bool = True
    timeout_s: Optional[float] = Field(default=None, ge=0.1, le=30.0)


class FleetApplyRandomLookRequest(BaseModel):
    theme: Optional[str] = None
    pack_file: Optional[str] = None
    brightness: Optional[int] = Field(default=None, ge=1, le=255)
    seed: Optional[int] = None
    targets: Optional[List[str]] = None
    include_self: bool = True


class FleetStopAllRequest(BaseModel):
    targets: Optional[List[str]] = None
    include_self: bool = True
    timeout_s: Optional[float] = Field(default=None, ge=0.1, le=30.0)


# ----------------------------
# Routes
# ----------------------------


@app.get("/", include_in_schema=False)
def root() -> Response:
    if SETTINGS.ui_enabled and _UI_DIST_DIR.is_dir():
        return RedirectResponse(url="/ui/", status_code=302)
    return JSONResponse(
        status_code=200,
        content={"ok": True, "service": "wled-show-agent", "version": app.version},
    )


class SPAStaticFiles(StaticFiles):
    """Serve a built single-page app with an `index.html` fallback for client-side routes."""

    async def get_response(self, path: str, scope: Any) -> Response:  # type: ignore[override]
        response = await super().get_response(path, scope)
        if (
            response.status_code == 404
            and scope.get("method") in ("GET", "HEAD")
            and "." not in (path or "")
        ):
            return await super().get_response("index.html", scope)
        return response


_UI_DIST_DIR = Path(__file__).resolve().parent / "ui" / "dist"
if SETTINGS.ui_enabled and _UI_DIST_DIR.is_dir():
    app.mount("/ui", SPAStaticFiles(directory=str(_UI_DIST_DIR), html=True), name="ui")
elif SETTINGS.ui_enabled:

    @app.get("/ui", include_in_schema=False)
    def ui_missing() -> Dict[str, Any]:
        raise HTTPException(
            status_code=404,
            detail="UI build not found. Build it with: cd agent/ui && npm install && npm run build",
        )


def _ui_app_html() -> str:
    return """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>WLED Show Agent</title>
  <style>
    :root { color-scheme: light dark; }
    body { font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif; margin: 0; padding: 0; }
    header { position: sticky; top: 0; backdrop-filter: blur(8px); background: rgba(0,0,0,.04); border-bottom: 1px solid rgba(127,127,127,.25); }
    .wrap { max-width: 980px; margin: 0 auto; padding: 14px 14px 80px; }
    h1 { font-size: 18px; margin: 0; }
    .top { display: flex; gap: 10px; align-items: center; justify-content: space-between; padding: 12px 14px; }
    .btn { padding: 10px 12px; border-radius: 10px; border: 1px solid rgba(127,127,127,.35); background: rgba(127,127,127,.08); }
    .btn.primary { background: #16a34a; border-color: rgba(22,163,74,.5); color: #fff; font-weight: 650; }
    .btn.danger { background: #dc2626; border-color: rgba(220,38,38,.6); color: #fff; font-weight: 650; }
    .grid { display: grid; grid-template-columns: 1fr; gap: 12px; margin-top: 14px; }
    @media (min-width: 860px) { .grid { grid-template-columns: 1fr 1fr; } }
    .card { border: 1px solid rgba(127,127,127,.35); border-radius: 14px; padding: 12px; background: rgba(127,127,127,.06); }
    .card h2 { font-size: 14px; margin: 0 0 8px; opacity: .9; }
    .row { display: grid; grid-template-columns: 1fr; gap: 8px; }
    label { font-size: 12px; opacity: .8; }
    input, select, textarea { width: 100%; padding: 10px 10px; border-radius: 10px; border: 1px solid rgba(127,127,127,.45); background: transparent; }
    textarea { min-height: 90px; font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; }
    .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; font-size: 12px; white-space: pre-wrap; }
    .muted { opacity: .75; font-size: 12px; }
    .split { display: grid; grid-template-columns: 1fr; gap: 8px; }
    @media (min-width: 520px) { .split { grid-template-columns: 1fr 1fr; } }
  </style>
</head>
<body>
  <header>
    <div class="top">
      <div>
        <h1>WLED Show Agent</h1>
        <div class="muted" id="who"></div>
      </div>
      <div style="display:flex; gap:8px; align-items:center;">
        <button class="btn" id="refresh">Refresh</button>
        <button class="btn danger" id="stopAll">Stop all</button>
        <button class="btn" id="logout">Logout</button>
      </div>
    </div>
  </header>
  <div class="wrap">
    <div class="grid">
      <div class="card">
        <h2>Status</h2>
        <div class="row">
          <div class="mono" id="status">Loading…</div>
        </div>
      </div>

	      <div class="card">
	        <h2>Quick Look</h2>
	        <div class="row">
	          <div class="split">
	            <div>
	              <label>Theme (optional)</label>
	              <input id="lookTheme" placeholder="classic / candy_cane / icy / ..." />
	            </div>
	            <div>
	              <label>Brightness (optional)</label>
	              <input id="lookBri" type="number" min="1" max="255" placeholder="180" />
	            </div>
	          </div>
	          <div class="split">
	            <div>
	              <label>Scope</label>
	              <select id="lookScope">
	                <option value="local">local</option>
	                <option value="fleet">fleet</option>
	              </select>
	            </div>
	            <div>
	              <label>Targets (optional)</label>
	              <input id="lookTargets" placeholder="roofline1,roofline2" />
	            </div>
	          </div>
	          <button class="btn primary" id="applyLook">Apply random look</button>
	          <div class="mono" id="lookOut"></div>
	        </div>
	      </div>

	      <div class="card">
	        <h2>Realtime Pattern (DDP)</h2>
	        <div class="row">
	          <div class="split">
            <div>
              <label>Pattern</label>
              <select id="pat"></select>
            </div>
            <div>
              <label>Duration (s)</label>
              <input id="dur" type="number" min="0.1" max="600" value="30" />
            </div>
            <div>
              <label>Brightness</label>
              <input id="bri" type="number" min="1" max="255" value="128" />
            </div>
            <div>
              <label>FPS (optional)</label>
              <input id="fps" type="number" min="1" max="60" placeholder="20" />
            </div>
            <div>
              <label>Direction (optional)</label>
              <select id="dir">
                <option value="">(auto)</option>
                <option value="cw">cw</option>
                <option value="ccw">ccw</option>
              </select>
            </div>
	            <div>
	              <label>Start pos (optional)</label>
	              <select id="pos">
	                <option value="">(auto)</option>
	                <option value="front">front</option>
	                <option value="right">right</option>
	                <option value="back">back</option>
	                <option value="left">left</option>
	              </select>
	            </div>
	            <div>
	              <label>Scope</label>
	              <select id="patScope">
	                <option value="local">local</option>
	                <option value="fleet">fleet</option>
	              </select>
	            </div>
	            <div>
	              <label>Targets (optional)</label>
	              <input id="patTargets" placeholder="roofline1,star_esps" />
	            </div>
	          </div>
	          <div style="display:flex; gap:8px;">
	            <button class="btn primary" id="startPat">Start</button>
	            <button class="btn" id="stopPat">Stop</button>
	          </div>
	          <div class="mono" id="patOut"></div>
	        </div>
	      </div>

	      <div class="card">
	        <h2>Fleet Sequence</h2>
	        <div class="row">
	          <div>
	            <label>Sequence file</label>
	            <select id="seqFile"></select>
	          </div>
	          <div class="split">
	            <div>
	              <label>Loop</label>
	              <select id="seqLoop">
	                <option value="false">false</option>
	                <option value="true">true</option>
	              </select>
	            </div>
	            <div>
	              <label>Targets (optional)</label>
	              <input id="seqTargets" placeholder="roofline1,roofline2" />
	            </div>
	          </div>
	          <div style="display:flex; gap:8px;">
	            <button class="btn primary" id="seqStart">Start</button>
	            <button class="btn" id="seqStop">Stop</button>
	          </div>
	          <div class="mono" id="seqOut"></div>
	        </div>
	      </div>

	      <div class="card">
	        <h2>Chat (optional)</h2>
	        <div class="row">
	          <div class="muted">Uses <span class="mono">/v1/command</span> (requires <span class="mono">OPENAI_API_KEY</span>).</div>
          <textarea id="chatIn" placeholder="e.g. 'make it candy cane and a bit brighter'"></textarea>
          <button class="btn primary" id="chatSend">Send</button>
          <div class="mono" id="chatOut"></div>
        </div>
      </div>
    </div>
  </div>

	<script>
	  const $ = (id) => document.getElementById(id);
	  const pretty = (x) => { try { return JSON.stringify(x, null, 2); } catch { return String(x); } };
	  const parseTargets = (s) => {
	    const raw = (s || '').split(',').map((x) => (x || '').trim()).filter(Boolean);
	    return raw.length ? raw : null;
	  };
	  async function api(path, opts) {
	    const o = opts || {};
	    o.credentials = 'include';
	    o.headers = Object.assign({ 'Content-Type': 'application/json' }, o.headers || {});
	    if (o.body && typeof o.body !== 'string') o.body = JSON.stringify(o.body);
    const resp = await fetch(path, o);
    const data = await resp.json().catch(() => ({}));
    if (!resp.ok) {
      const msg = (data && (data.detail || data.error)) ? (data.detail || data.error) : ('HTTP ' + resp.status);
      throw new Error(msg);
    }
    return data;
  }

	  async function loadPatterns() {
	    try {
	      const d = await api('/v1/ddp/patterns', { method: 'GET' });
	      const sel = $('pat');
	      sel.innerHTML = '';
      (d.patterns || []).forEach((p) => {
        const opt = document.createElement('option');
        opt.value = p;
        opt.textContent = p;
        sel.appendChild(opt);
      });
    } catch (e) {
      $('patOut').textContent = 'Failed to load patterns: ' + e.message;
	    }
	  }

	  async function loadSequences() {
	    try {
	      const d = await api('/v1/sequences/list', { method: 'GET' });
	      const sel = $('seqFile');
	      sel.innerHTML = '';
	      (d.files || []).forEach((f) => {
	        const opt = document.createElement('option');
	        opt.value = f;
	        opt.textContent = f;
	        sel.appendChild(opt);
	      });
	    } catch (e) {
	      $('seqOut').textContent = 'Failed to load sequences: ' + e.message;
	    }
	  }

	  async function refresh() {
	    const out = {};
	    try { out.health = await api('/v1/health', { method: 'GET' }); } catch (e) { out.health = { ok:false, error:e.message }; }
	    try { out.wled = await api('/v1/wled/info', { method: 'GET' }); } catch (e) { out.wled = { ok:false, error:e.message }; }
	    try { out.ddp = await api('/v1/ddp/status', { method: 'GET' }); } catch (e) { out.ddp = { ok:false, error:e.message }; }
	    try { out.sequence = await api('/v1/sequences/status', { method: 'GET' }); } catch (e) { out.sequence = { ok:false, error:e.message }; }
    try { out.fleet = await api('/v1/fleet/peers', { method: 'GET' }); } catch (e) { out.fleet = { ok:false, error:e.message }; }
    try {
      out.fleet_status = await api('/v1/fleet/invoke', { method: 'POST', body: { action: 'status', params: {}, include_self: true } });
    } catch (e) {
      out.fleet_status = { ok:false, error:e.message };
    }
    $('status').textContent = pretty(out);
  }

  async function whoami() {
    try {
      const me = await api('/v1/auth/me', { method: 'GET' });
      $('who').textContent = 'Signed in as ' + (me.user && me.user.username ? me.user.username : 'user');
    } catch {
      $('who').textContent = '';
    }
  }

	  $('refresh').addEventListener('click', refresh);
	  $('applyLook').addEventListener('click', async () => {
	    $('lookOut').textContent = '';
	    try {
	      const theme = ($('lookTheme').value || '').trim() || null;
	      const briRaw = ($('lookBri').value || '').trim();
	      const brightness = briRaw ? parseInt(briRaw, 10) : null;
	      const scope = ($('lookScope').value || 'local').trim();
	      const targets = parseTargets(($('lookTargets').value || '').trim());
	      const path = (scope === 'fleet') ? '/v1/fleet/apply_random_look' : '/v1/looks/apply_random';
	      const body = (scope === 'fleet') ? { theme, brightness, targets, include_self: true } : { theme, brightness };
	      const res = await api(path, { method: 'POST', body });
	      $('lookOut').textContent = pretty(res);
	      refresh();
	    } catch (e) {
	      $('lookOut').textContent = 'Error: ' + e.message;
	    }
	  });

	  $('startPat').addEventListener('click', async () => {
	    $('patOut').textContent = '';
	    try {
	      const body = {
	        pattern: $('pat').value,
	        duration_s: parseFloat(($('dur').value || '30')),
	        brightness: parseInt(($('bri').value || '128'), 10),
	      };
	      const fpsRaw = ($('fps').value || '').trim();
	      if (fpsRaw) body.fps = parseFloat(fpsRaw);
	      const dir = $('dir').value || '';
	      if (dir) body.direction = dir;
	      const pos = $('pos').value || '';
	      if (pos) body.start_pos = pos;
	      const scope = ($('patScope').value || 'local').trim();
	      const targets = parseTargets(($('patTargets').value || '').trim());
	      const path = (scope === 'fleet') ? '/v1/fleet/invoke' : '/v1/ddp/start';
	      const payload = (scope === 'fleet') ? { action: 'start_ddp_pattern', params: body, targets, include_self: true } : body;
	      const res = await api(path, { method: 'POST', body: payload });
	      $('patOut').textContent = pretty(res);
	      refresh();
	    } catch (e) {
	      $('patOut').textContent = 'Error: ' + e.message;
	    }
	  });

	  $('stopPat').addEventListener('click', async () => {
	    $('patOut').textContent = '';
	    try {
	      const scope = ($('patScope').value || 'local').trim();
	      const targets = parseTargets(($('patTargets').value || '').trim());
	      const path = (scope === 'fleet') ? '/v1/fleet/invoke' : '/v1/ddp/stop';
	      const payload = (scope === 'fleet') ? { action: 'stop_ddp', params: {}, targets, include_self: true } : {};
	      const res = await api(path, { method: 'POST', body: payload });
	      $('patOut').textContent = pretty(res);
	      refresh();
	    } catch (e) {
	      $('patOut').textContent = 'Error: ' + e.message;
	    }
	  });

	  $('seqStart').addEventListener('click', async () => {
	    $('seqOut').textContent = '';
	    try {
	      const file = $('seqFile').value || '';
	      if (!file) throw new Error('Pick a sequence file');
	      const loop = ($('seqLoop').value || 'false') === 'true';
	      const targets = parseTargets(($('seqTargets').value || '').trim());
	      const res = await api('/v1/fleet/sequences/start', { method: 'POST', body: { file, loop, targets, include_self: true } });
	      $('seqOut').textContent = pretty(res);
	      refresh();
	    } catch (e) {
	      $('seqOut').textContent = 'Error: ' + e.message;
	    }
	  });

	  $('seqStop').addEventListener('click', async () => {
	    $('seqOut').textContent = '';
	    try {
	      const res = await api('/v1/fleet/sequences/stop', { method: 'POST', body: {} });
	      $('seqOut').textContent = pretty(res);
	      refresh();
	    } catch (e) {
	      $('seqOut').textContent = 'Error: ' + e.message;
	    }
	  });

	  $('stopAll').addEventListener('click', async () => {
	    $('status').textContent = 'Stopping…';
	    try {
	      await api('/v1/fleet/stop_all', { method: 'POST', body: {} });
	    } catch (e) {
      $('status').textContent = 'Stop error: ' + e.message;
    } finally {
      refresh();
    }
  });

  $('logout').addEventListener('click', async () => {
    try { await api('/v1/auth/logout', { method: 'POST', body: {} }); } catch {}
    window.location = '/ui';
  });

  $('chatSend').addEventListener('click', async () => {
    $('chatOut').textContent = '';
    try {
      const text = ($('chatIn').value || '').trim();
      if (!text) return;
      const res = await api('/v1/command', { method: 'POST', body: { text } });
      $('chatOut').textContent = pretty(res);
      refresh();
    } catch (e) {
      $('chatOut').textContent = 'Error: ' + e.message;
	    }
	  });

	  loadPatterns().then(loadSequences).then(refresh).then(whoami);
	</script>
	</body>
	</html>
	"""


@app.get("/v1/health")
def health() -> Dict[str, Any]:
    return {"ok": True, "service": "wled-show-agent", "version": app.version}


def _jwt_from_request(request: Request) -> str | None:
    auth = request.headers.get("authorization") or ""
    parts = auth.strip().split(None, 1)
    if len(parts) == 2 and parts[0].lower() == "bearer":
        tok = parts[1].strip()
        # Only treat Bearer values that look like a JWT as JWTs; this avoids
        # conflicting with A2A keys or other non-JWT bearer tokens.
        if tok and tok.count(".") == 2:
            return tok
    if SETTINGS.auth_cookie_name:
        tok = request.cookies.get(SETTINGS.auth_cookie_name)
        if tok:
            return str(tok).strip()
    return None


def _require_jwt_auth(request: Request) -> Dict[str, Any]:
    if not SETTINGS.auth_enabled:
        raise HTTPException(
            status_code=400, detail="AUTH_ENABLED is false; JWT auth is not configured."
        )
    tok = _jwt_from_request(request)
    if not tok:
        raise HTTPException(status_code=401, detail="Missing token")
    try:
        claims = jwt_decode_hs256(
            tok,
            secret=str(SETTINGS.auth_jwt_secret or ""),
            issuer=str(SETTINGS.auth_jwt_issuer or ""),
        )
        return {
            "subject": claims.subject,
            "expires_at": claims.expires_at,
            "issued_at": claims.issued_at,
            "claims": claims.raw,
        }
    except AuthError as e:
        raise HTTPException(status_code=401, detail=str(e))


@app.get("/v1/auth/config")
def auth_config() -> Dict[str, Any]:
    return {
        "ok": True,
        "version": app.version,
        "ui_enabled": bool(SETTINGS.ui_enabled),
        "auth_enabled": bool(SETTINGS.auth_enabled),
        "totp_enabled": bool(SETTINGS.auth_totp_enabled),
        "openai_enabled": bool(SETTINGS.openai_api_key),
        "fpp_enabled": bool(SETTINGS.fpp_base_url),
        "peers_configured": len(PEERS),
    }


@app.post("/v1/auth/login")
def auth_login(req: AuthLoginRequest, response: Response) -> Dict[str, Any]:
    if not SETTINGS.auth_enabled:
        raise HTTPException(
            status_code=400, detail="AUTH_ENABLED is false; login is disabled."
        )
    user = (req.username or "").strip()
    if user != (SETTINGS.auth_username or "").strip():
        raise HTTPException(status_code=401, detail="Invalid username or password")
    if not verify_password(req.password, str(SETTINGS.auth_password or "")):
        raise HTTPException(status_code=401, detail="Invalid username or password")
    if SETTINGS.auth_totp_enabled:
        if not totp_verify(
            secret_b32=str(SETTINGS.auth_totp_secret or ""), code=str(req.totp or "")
        ):
            raise HTTPException(status_code=401, detail="Invalid TOTP code")

    token = jwt_encode_hs256(
        {"sub": user, "role": "admin"},
        secret=str(SETTINGS.auth_jwt_secret or ""),
        ttl_s=int(SETTINGS.auth_jwt_ttl_s),
        issuer=str(SETTINGS.auth_jwt_issuer or ""),
    )

    response.set_cookie(
        key=str(SETTINGS.auth_cookie_name),
        value=token,
        httponly=True,
        secure=bool(SETTINGS.auth_cookie_secure),
        samesite="lax",
        max_age=int(SETTINGS.auth_jwt_ttl_s),
        path="/",
    )
    return {
        "ok": True,
        "user": {"username": user},
        "token": token,
        "expires_in": int(SETTINGS.auth_jwt_ttl_s),
    }


@app.post("/v1/auth/logout")
def auth_logout(response: Response) -> Dict[str, Any]:
    if SETTINGS.auth_cookie_name:
        response.delete_cookie(key=str(SETTINGS.auth_cookie_name), path="/")
    return {"ok": True}


@app.get("/v1/auth/me")
def auth_me(request: Request) -> Dict[str, Any]:
    info = _require_jwt_auth(request)
    return {
        "ok": True,
        "user": {"username": info["subject"]},
        "expires_at": info["expires_at"],
    }


@app.get("/v1/wled/info")
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


@app.get("/v1/wled/state")
def wled_state() -> Dict[str, Any]:
    try:
        st = WLED.get_state()
        return {"ok": True, "state": st}
    except WLEDError as e:
        raise HTTPException(status_code=502, detail=str(e))


@app.get("/v1/wled/segments")
def wled_segments() -> Dict[str, Any]:
    """Return the current segment list from WLED (useful when you have 2+ segments)."""
    try:
        segs = WLED.get_segments(refresh=True)
        return {"ok": True, "segment_ids": SEGMENT_IDS, "segments": segs}
    except WLEDError as e:
        raise HTTPException(status_code=502, detail=str(e))


@app.get("/v1/segments/layout")
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


@app.get("/v1/segments/orientation")
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


@app.post("/v1/wled/state")
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


@app.get("/v1/ddp/patterns")
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


@app.get("/v1/ddp/status")
def ddp_status() -> Dict[str, Any]:
    return {"ok": True, "status": DDP.status().__dict__}


@app.post("/v1/ddp/start")
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
        return {"ok": True, "status": st.__dict__}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/v1/ddp/stop")
def ddp_stop() -> Dict[str, Any]:
    st = DDP.stop()
    return {"ok": True, "status": st.__dict__}


@app.post("/v1/looks/generate")
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


@app.get("/v1/looks/packs")
def looks_packs() -> Dict[str, Any]:
    return {"ok": True, "packs": LOOKS.list_packs(), "latest": LOOKS.latest_pack()}


@app.post("/v1/looks/apply_random")
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


@app.post("/v1/presets/import_from_pack")
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


@app.get("/v1/sequences/list")
def sequences_list() -> Dict[str, Any]:
    return {"ok": True, "files": SEQUENCES.list_sequences()}


@app.post("/v1/sequences/generate")
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


@app.get("/v1/sequences/status")
def sequences_status() -> Dict[str, Any]:
    return {"ok": True, "status": SEQUENCES.status().__dict__}


@app.post("/v1/sequences/play")
def sequences_play(req: PlaySequenceRequest) -> Dict[str, Any]:
    try:
        st = SEQUENCES.play(file=req.file, loop=req.loop)
        return {"ok": True, "status": st.__dict__}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/v1/sequences/stop")
def sequences_stop() -> Dict[str, Any]:
    st = SEQUENCES.stop()
    return {"ok": True, "status": st.__dict__}


@app.post("/v1/fseq/export")
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


@app.post("/v1/audio/analyze")
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


@app.post("/v1/go_crazy")
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


@app.post("/v1/command")
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
    base = Path(SETTINGS.data_dir).resolve()
    p = (base / (rel_path or "")).resolve()
    if p == base:
        return p
    if base not in p.parents:
        raise HTTPException(status_code=400, detail="Path must be within DATA_DIR.")
    return p


@app.get("/v1/fpp/status")
def fpp_status(_: None = Depends(_require_a2a_auth)) -> Dict[str, Any]:
    fpp = _require_fpp()
    try:
        return {"ok": True, "fpp": fpp.status().as_dict()}
    except FPPError as e:
        raise HTTPException(status_code=502, detail=str(e))


@app.get("/v1/fpp/discover")
def fpp_discover(_: None = Depends(_require_a2a_auth)) -> Dict[str, Any]:
    fpp = _require_fpp()
    try:
        return {"ok": True, "discover": fpp.discover()}
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))


@app.get("/v1/fpp/playlists")
def fpp_playlists(_: None = Depends(_require_a2a_auth)) -> Dict[str, Any]:
    fpp = _require_fpp()
    try:
        return {"ok": True, "fpp": fpp.playlists().as_dict()}
    except FPPError as e:
        raise HTTPException(status_code=502, detail=str(e))


@app.post("/v1/fpp/playlist/start")
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


@app.post("/v1/fpp/playlist/stop")
def fpp_stop_playlist(_: None = Depends(_require_a2a_auth)) -> Dict[str, Any]:
    fpp = _require_fpp()
    try:
        return {"ok": True, "fpp": fpp.stop_playlist().as_dict()}
    except FPPError as e:
        raise HTTPException(status_code=502, detail=str(e))


@app.post("/v1/fpp/event/trigger")
def fpp_trigger_event(
    req: FPPTriggerEventRequest, _: None = Depends(_require_a2a_auth)
) -> Dict[str, Any]:
    fpp = _require_fpp()
    try:
        return {"ok": True, "fpp": fpp.trigger_event(req.event_id).as_dict()}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/v1/fpp/request")
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


@app.post("/v1/fpp/upload_file")
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


@app.post("/v1/show/config/load")
def show_config_load(
    req: ShowConfigLoadRequest, _: None = Depends(_require_a2a_auth)
) -> Dict[str, Any]:
    try:
        cfg = load_show_config(data_dir=SETTINGS.data_dir, rel_path=req.file)
        return {"ok": True, "config": cfg.as_dict()}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/v1/xlights/import_networks")
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


@app.post("/v1/xlights/import_project")
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


@app.post("/v1/xlights/import_sequence")
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


@app.post("/v1/fpp/export/fleet_sequence_start_script")
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


@app.post("/v1/fpp/export/fleet_stop_all_script")
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


@app.get("/v1/a2a/card")
def a2a_card(_: None = Depends(_require_a2a_auth)) -> Dict[str, Any]:
    return {
        "ok": True,
        "agent": {
            "id": SETTINGS.agent_id,
            "name": SETTINGS.agent_name,
            "role": SETTINGS.agent_role,
            "version": app.version,
            "endpoints": {"card": "/v1/a2a/card", "invoke": "/v1/a2a/invoke"},
            "wled": {
                "url": SETTINGS.wled_tree_url,
                "segment_ids": SEGMENT_IDS,
                "replicate_to_all_segments": SETTINGS.wled_replicate_to_all_segments,
            },
            "capabilities": _A2A_CAPABILITIES,
        },
    }


@app.post("/v1/a2a/invoke")
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
    if not SETTINGS.a2a_api_key:
        return {}
    return {"X-A2A-Key": SETTINGS.a2a_api_key}


def _peer_post(
    peer: A2APeer, path: str, payload: Dict[str, Any], *, timeout_s: float
) -> Dict[str, Any]:
    url = peer.base_url.rstrip("/") + path
    resp = requests.post(url, json=payload, headers=_peer_headers(), timeout=timeout_s)
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
    resp = requests.get(url, headers=_peer_headers(), timeout=timeout_s)
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
FLEET_SEQUENCES = FleetSequenceService(
    data_dir=SETTINGS.data_dir,
    peers=PEERS,
    local_invoke=_fleet_local_invoke,
    peer_invoke=_fleet_peer_invoke,
    peer_supported_actions=_peer_supported_actions,
    default_timeout_s=float(SETTINGS.a2a_http_timeout_s),
)


@app.get("/v1/fleet/peers")
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


@app.post("/v1/fleet/invoke")
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


@app.post("/v1/fleet/apply_random_look")
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


@app.post("/v1/fleet/stop_all")
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

    return {"ok": True, "action": "stop_all", "results": results}


@app.get("/v1/fleet/sequences/status")
def fleet_sequences_status(_: None = Depends(_require_a2a_auth)) -> Dict[str, Any]:
    if FLEET_SEQUENCES is None:
        raise HTTPException(
            status_code=500, detail="Fleet sequence service not initialized."
        )
    return {"ok": True, "status": FLEET_SEQUENCES.status().__dict__}


@app.post("/v1/fleet/sequences/start")
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
        return {"ok": True, "status": st.__dict__}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/v1/fleet/sequences/stop")
def fleet_sequences_stop(_: None = Depends(_require_a2a_auth)) -> Dict[str, Any]:
    if FLEET_SEQUENCES is None:
        raise HTTPException(
            status_code=500, detail="Fleet sequence service not initialized."
        )
    st = FLEET_SEQUENCES.stop()
    return {"ok": True, "status": st.__dict__}


# ----------------------------
# Data files helpers (UI convenience)
# ----------------------------


@app.get("/v1/files/list")
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


@app.get("/v1/files/download")
def files_download(path: str, _: None = Depends(_require_a2a_auth)) -> FileResponse:
    p = _resolve_data_path(path)
    if not p.is_file():
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(path=str(p), filename=p.name)


# ----------------------------
# Jobs + progress (SSE)
# ----------------------------


@app.get("/v1/jobs")
def jobs_list(limit: int = 50, _: None = Depends(_require_a2a_auth)) -> Dict[str, Any]:
    return {"ok": True, "jobs": [j.as_dict() for j in JOBS.list_jobs(limit=limit)]}


@app.get("/v1/jobs/{job_id}")
def jobs_get(job_id: str, _: None = Depends(_require_a2a_auth)) -> Dict[str, Any]:
    j = JOBS.get(job_id)
    if not j:
        raise HTTPException(status_code=404, detail="Job not found")
    return {"ok": True, "job": j.as_dict()}


@app.post("/v1/jobs/{job_id}/cancel")
def jobs_cancel(job_id: str, _: None = Depends(_require_a2a_auth)) -> Dict[str, Any]:
    j = JOBS.cancel(job_id)
    if not j:
        raise HTTPException(status_code=404, detail="Job not found")
    return {"ok": True, "job": j.as_dict()}


@app.get("/v1/jobs/stream")
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


@app.post("/v1/jobs/looks/generate")
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


@app.post("/v1/jobs/audio/analyze")
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


@app.post("/v1/jobs/xlights/import_project")
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


@app.post("/v1/jobs/xlights/import_networks")
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


@app.post("/v1/jobs/xlights/import_sequence")
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


@app.post("/v1/jobs/sequences/generate")
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


@app.post("/v1/jobs/fseq/export")
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
