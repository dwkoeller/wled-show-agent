from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


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
