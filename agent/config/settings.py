from __future__ import annotations

import os
import json
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse


def _as_int(val: str | None, default: int) -> int:
    try:
        return int(val) if val is not None and str(val).strip() != "" else default
    except Exception:
        return default


def _as_float(val: str | None, default: float) -> float:
    try:
        return float(val) if val is not None and str(val).strip() != "" else default
    except Exception:
        return default


def _as_bool(val: str | None, default: bool) -> bool:
    if val is None:
        return default
    s = str(val).strip().lower()
    if s in ("1", "true", "yes", "y", "on"):
        return True
    if s in ("0", "false", "no", "n", "off"):
        return False
    return default


def _as_str(val: str | None, default: str) -> str:
    if val is None:
        return default
    s = str(val).strip()
    return s if s else default


def _norm_dir(val: str | None, default: str = "ccw") -> str:
    """Normalize a rotation direction string to 'cw' or 'ccw'."""
    s = str(val).strip().lower() if val is not None else ""
    if s in ("cw", "clockwise"):
        return "cw"
    if s in (
        "ccw",
        "counterclockwise",
        "counter-clockwise",
        "anticlockwise",
        "anti-clockwise",
    ):
        return "ccw"
    return default


def _norm_pos(val: str | None, default: str = "front") -> str:
    """Normalize a street-facing position string."""
    s = str(val).strip().lower() if val is not None else ""
    # Common synonyms
    if s in ("front", "street", "street-facing", "south"):
        return "front"
    if s in ("back", "rear", "north"):
        return "back"
    if s in ("left", "west"):
        return "left"
    if s in ("right", "east"):
        return "right"
    return default


def _as_int_list(val: str | None) -> tuple[int, ...]:
    """Parse comma-separated ints, ignoring blanks/bad tokens."""
    if val is None:
        return tuple()
    out: list[int] = []
    for part in str(val).split(","):
        p = part.strip()
        if not p:
            continue
        try:
            out.append(int(p))
        except Exception:
            continue
    # de-dup while preserving order
    seen: set[int] = set()
    uniq: list[int] = []
    for x in out:
        if x in seen:
            continue
        seen.add(x)
        uniq.append(x)
    return tuple(uniq)


def _as_csv(val: str | None) -> tuple[str, ...]:
    """Parse comma-separated strings, ignoring blanks."""
    if val is None:
        return tuple()
    out: list[str] = []
    for part in str(val).split(","):
        p = part.strip()
        if not p:
            continue
        out.append(p)
    return tuple(out)


def _as_json_headers(val: str | None) -> tuple[tuple[str, str], ...]:
    """Parse a JSON object string into (key,value) header pairs."""
    if val is None:
        return tuple()
    raw = str(val).strip()
    if not raw:
        return tuple()
    try:
        obj = json.loads(raw)
    except Exception:
        return tuple()
    if not isinstance(obj, dict):
        return tuple()
    out: list[tuple[str, str]] = []
    for k, v in obj.items():
        if v is None:
            continue
        key = str(k).strip()
        if not key:
            continue
        out.append((key, str(v)))
    return tuple(out)


def _as_json_list(val: str | None) -> list[dict[str, Any]]:
    """Parse a JSON array into a list of dicts."""
    if val is None:
        return []
    raw = str(val).strip()
    if not raw:
        return []
    try:
        obj = json.loads(raw)
    except Exception:
        return []
    if not isinstance(obj, list):
        return []
    out: list[dict[str, Any]] = []
    for item in obj:
        if isinstance(item, dict):
            out.append(item)
    return out


def _host_from_url(url: str) -> str:
    p = urlparse(url)
    return p.hostname or ""


@dataclass(frozen=True)
class Settings:
    # Controller kind
    controller_kind: str  # "wled" (default) or "pixel"

    # WLED
    wled_tree_url: str
    wled_http_timeout_s: float
    wled_max_bri: int
    wled_command_cooldown_ms: int
    wled_segment_ids: tuple[int, ...]
    wled_replicate_to_all_segments: bool

    # Street-facing orientation hints for 4-segment "quadrant" trees.
    # These are used to interpret "clockwise/counterclockwise" and "front/right/back/left"
    # for quadrant-aware DDP patterns and the optional AI director.
    quad_right_segment_id: int
    quad_order_from_street: (
        str  # 'cw' or 'ccw' meaning increasing segment order as seen from the street
    )
    quad_default_start_pos: str  # 'front'|'right'|'back'|'left'

    # Tree geometry (optional)
    tree_runs: int
    tree_pixels_per_run: int
    tree_segment_len: int
    tree_segments_per_run: int

    # DDP
    ddp_host: str
    ddp_port: int
    ddp_destination_id: int
    ddp_max_pixels_per_packet: int
    ddp_fps_default: float
    ddp_fps_max: float
    ddp_drop_late_frames: bool
    ddp_backpressure_max_lag_s: float
    ddp_use_cpu_pool: bool

    # Media previews
    sequence_preview_width: int
    sequence_preview_height: int
    sequence_preview_fps: float
    sequence_preview_max_s: float
    sequence_preview_cache_max_mb: int
    sequence_preview_cache_max_days: float
    waveform_cache_max_mb: int
    waveform_cache_max_days: float
    waveform_points_default: int

    # Pixel streaming (for non-WLED controllers like ESPixelStick)
    pixel_host: str
    pixel_port: int
    pixel_protocol: str  # "e131" or "artnet"
    pixel_universe_start: int
    pixel_channels_per_universe: int
    pixel_count: int
    pixel_priority: int
    pixel_source_name: str

    # Data dir
    data_dir: str

    # Database (required)
    database_url: str
    db_migrate_on_startup: bool
    job_history_max_rows: int
    job_history_max_days: int
    job_history_maintenance_interval_s: int
    metrics_history_interval_s: int
    metrics_history_max_rows: int
    metrics_history_max_days: int
    metrics_history_maintenance_interval_s: int
    scheduler_events_max_rows: int
    scheduler_events_max_days: int
    scheduler_events_maintenance_interval_s: int
    pack_ingests_max_rows: int
    pack_ingests_max_days: int
    pack_ingests_maintenance_interval_s: int
    sequence_meta_max_rows: int
    sequence_meta_max_days: int
    sequence_meta_maintenance_interval_s: int
    audio_analyses_max_rows: int
    audio_analyses_max_days: int
    audio_analyses_maintenance_interval_s: int
    show_configs_max_rows: int
    show_configs_max_days: int
    show_configs_maintenance_interval_s: int
    fseq_exports_max_rows: int
    fseq_exports_max_days: int
    fseq_exports_maintenance_interval_s: int
    fpp_scripts_max_rows: int
    fpp_scripts_max_days: int
    fpp_scripts_maintenance_interval_s: int
    audit_log_max_rows: int
    audit_log_max_days: int
    audit_log_maintenance_interval_s: int
    events_history_max_rows: int
    events_history_max_days: int
    events_history_maintenance_interval_s: int
    events_spool_path: str
    events_spool_max_mb: int
    events_spool_flush_interval_s: int
    orchestration_runs_max_rows: int
    orchestration_runs_max_days: int
    orchestration_runs_maintenance_interval_s: int
    agent_history_max_rows: int
    agent_history_max_days: int
    agent_history_maintenance_interval_s: int
    agent_history_interval_s: int
    db_reconcile_on_startup: bool
    db_reconcile_interval_s: int
    db_reconcile_scan_limit: int
    db_reconcile_include_audio: bool
    precompute_previews_on_ingest: bool
    precompute_waveforms_on_ingest: bool
    precompute_previews_on_reconcile: bool
    precompute_waveforms_on_reconcile: bool

    # Jobs
    job_max_jobs: int
    job_queue_size: int
    job_worker_count: int

    # OpenAI
    openai_api_key: str | None
    openai_model: str
    openai_stt_model: str

    # Agent identity / A2A
    agent_id: str
    agent_name: str
    agent_role: str
    agent_base_url: str | None
    agent_tags: tuple[str, ...]
    scheduler_leader_roles: tuple[str, ...]
    fleet_stale_after_s: float
    fleet_db_discovery_enabled: bool
    fleet_health_cache_ttl_s: float
    a2a_api_key: str | None
    a2a_peers: tuple[str, ...]
    a2a_http_timeout_s: float

    # Outbound retry/backoff policy
    outbound_retry_attempts: int
    outbound_retry_backoff_base_s: float
    outbound_retry_backoff_max_s: float
    outbound_retry_status_codes: tuple[int, ...]

    # Falcon Player (FPP) integration (optional)
    fpp_base_url: str
    fpp_http_timeout_s: float
    fpp_headers: tuple[tuple[str, str], ...]

    # LedFx integration (optional)
    ledfx_base_url: str
    ledfx_http_timeout_s: float
    ledfx_headers: tuple[tuple[str, str], ...]
    ledfx_fleet_cache_ttl_s: float

    # UI + auth (required)
    ui_enabled: bool
    auth_enabled: bool
    auth_username: str
    auth_password: str | None
    auth_jwt_secret: str | None
    auth_jwt_ttl_s: int
    auth_jwt_issuer: str
    auth_cookie_name: str
    auth_cookie_secure: bool
    auth_csrf_enabled: bool
    auth_csrf_cookie_name: str
    auth_csrf_header_name: str
    auth_totp_enabled: bool
    auth_totp_secret: str | None
    auth_totp_issuer: str
    auth_user_role: str
    auth_users: list[dict[str, Any]]
    auth_login_max_attempts: int
    auth_login_window_s: int
    auth_login_lockout_s: int
    auth_session_cleanup_interval_s: int
    auth_session_cleanup_max_age_s: int
    auth_session_touch_interval_s: int

    # Metrics / Prometheus
    metrics_public: bool
    metrics_scrape_token: str | None
    metrics_scrape_header: str

    # API rate limiting
    rate_limit_enabled: bool
    rate_limit_requests_per_minute: int
    rate_limit_burst: int
    rate_limit_scope: str
    rate_limit_exempt_paths: tuple[str, ...]
    rate_limit_bucket_ttl_s: float
    rate_limit_cleanup_interval_s: float
    rate_limit_trust_proxy_headers: bool

    # MQTT bridge (optional)
    mqtt_enabled: bool
    mqtt_url: str
    mqtt_username: str | None
    mqtt_password: str | None
    mqtt_base_topic: str
    mqtt_qos: int
    mqtt_status_interval_s: int
    mqtt_reconnect_interval_s: float
    ha_mqtt_discovery_enabled: bool
    ha_mqtt_discovery_prefix: str
    ha_mqtt_entity_prefix: str

    # Files
    files_upload_allowlist_only: bool

    # Backup / restore
    backup_max_zip_mb: int
    backup_max_unpacked_mb: int
    backup_max_file_mb: int
    backup_max_files: int
    backup_exclude_globs: tuple[str, ...]
    backup_spool_max_mb: int

    # Blocking worker pool
    blocking_max_workers: int
    blocking_max_queue: int
    blocking_queue_timeout_s: float
    ddp_blocking_max_workers: int
    ddp_blocking_max_queue: int
    ddp_blocking_queue_timeout_s: float
    cpu_pool_max_workers: int
    cpu_pool_max_queue: int
    cpu_pool_queue_timeout_s: float

    @property
    def tree_total_pixels(self) -> int:
        if self.tree_runs > 0 and self.tree_pixels_per_run > 0:
            return self.tree_runs * self.tree_pixels_per_run
        return 0


def load_settings() -> Settings:
    controller_kind = (
        _as_str(os.environ.get("CONTROLLER_KIND"), default="wled").strip().lower()
        or "wled"
    )

    wled_tree_url = os.environ.get("WLED_TREE_URL", "").strip()
    if controller_kind == "wled" and not wled_tree_url:
        raise RuntimeError(
            "WLED_TREE_URL is required when CONTROLLER_KIND=wled (e.g. http://10.0.0.50)"
        )

    data_dir = os.environ.get("DATA_DIR", "/data").strip() or "/data"
    database_url = os.environ.get("DATABASE_URL", "").strip()
    if not database_url:
        raise RuntimeError(
            "DATABASE_URL is required (e.g. mysql://wsa:wsa@db:3306/wsa or sqlite:////data/wsa.sqlite)"
        )
    db_migrate_on_startup = _as_bool(
        os.environ.get("DB_MIGRATE_ON_STARTUP"), True
    )
    job_history_max_rows = max(0, _as_int(os.environ.get("JOB_HISTORY_MAX_ROWS"), 2000))
    job_history_max_days = max(0, _as_int(os.environ.get("JOB_HISTORY_MAX_DAYS"), 30))
    job_history_maintenance_interval_s = max(
        60, _as_int(os.environ.get("JOB_HISTORY_MAINTENANCE_INTERVAL_S"), 3600)
    )
    metrics_history_interval_s = max(
        10, _as_int(os.environ.get("METRICS_HISTORY_INTERVAL_S"), 30)
    )
    metrics_history_max_rows = max(
        0, _as_int(os.environ.get("METRICS_HISTORY_MAX_ROWS"), 10000)
    )
    metrics_history_max_days = max(
        0, _as_int(os.environ.get("METRICS_HISTORY_MAX_DAYS"), 14)
    )
    metrics_history_maintenance_interval_s = max(
        60, _as_int(os.environ.get("METRICS_HISTORY_MAINTENANCE_INTERVAL_S"), 3600)
    )
    scheduler_events_max_rows = max(
        0, _as_int(os.environ.get("SCHEDULER_EVENTS_MAX_ROWS"), 5000)
    )
    scheduler_events_max_days = max(
        0, _as_int(os.environ.get("SCHEDULER_EVENTS_MAX_DAYS"), 30)
    )
    scheduler_events_maintenance_interval_s = max(
        60, _as_int(os.environ.get("SCHEDULER_EVENTS_MAINTENANCE_INTERVAL_S"), 3600)
    )
    pack_ingests_max_rows = max(
        0, _as_int(os.environ.get("PACK_INGESTS_MAX_ROWS"), 500)
    )
    pack_ingests_max_days = max(
        0, _as_int(os.environ.get("PACK_INGESTS_MAX_DAYS"), 365)
    )
    pack_ingests_maintenance_interval_s = max(
        60, _as_int(os.environ.get("PACK_INGESTS_MAINTENANCE_INTERVAL_S"), 3600)
    )
    sequence_meta_max_rows = max(
        0, _as_int(os.environ.get("SEQUENCE_META_MAX_ROWS"), 2000)
    )
    sequence_meta_max_days = max(
        0, _as_int(os.environ.get("SEQUENCE_META_MAX_DAYS"), 365)
    )
    sequence_meta_maintenance_interval_s = max(
        60, _as_int(os.environ.get("SEQUENCE_META_MAINTENANCE_INTERVAL_S"), 3600)
    )
    audio_analyses_max_rows = max(
        0, _as_int(os.environ.get("AUDIO_ANALYSES_MAX_ROWS"), 1000)
    )
    audio_analyses_max_days = max(
        0, _as_int(os.environ.get("AUDIO_ANALYSES_MAX_DAYS"), 90)
    )
    audio_analyses_maintenance_interval_s = max(
        60, _as_int(os.environ.get("AUDIO_ANALYSES_MAINTENANCE_INTERVAL_S"), 3600)
    )
    show_configs_max_rows = max(
        0, _as_int(os.environ.get("SHOW_CONFIGS_MAX_ROWS"), 1000)
    )
    show_configs_max_days = max(
        0, _as_int(os.environ.get("SHOW_CONFIGS_MAX_DAYS"), 365)
    )
    show_configs_maintenance_interval_s = max(
        60, _as_int(os.environ.get("SHOW_CONFIGS_MAINTENANCE_INTERVAL_S"), 3600)
    )
    fseq_exports_max_rows = max(
        0, _as_int(os.environ.get("FSEQ_EXPORTS_MAX_ROWS"), 1000)
    )
    fseq_exports_max_days = max(
        0, _as_int(os.environ.get("FSEQ_EXPORTS_MAX_DAYS"), 365)
    )
    fseq_exports_maintenance_interval_s = max(
        60, _as_int(os.environ.get("FSEQ_EXPORTS_MAINTENANCE_INTERVAL_S"), 3600)
    )
    fpp_scripts_max_rows = max(
        0, _as_int(os.environ.get("FPP_SCRIPTS_MAX_ROWS"), 1000)
    )
    fpp_scripts_max_days = max(
        0, _as_int(os.environ.get("FPP_SCRIPTS_MAX_DAYS"), 365)
    )
    fpp_scripts_maintenance_interval_s = max(
        60, _as_int(os.environ.get("FPP_SCRIPTS_MAINTENANCE_INTERVAL_S"), 3600)
    )
    audit_log_max_rows = max(
        0, _as_int(os.environ.get("AUDIT_LOG_MAX_ROWS"), 2000)
    )
    audit_log_max_days = max(
        0, _as_int(os.environ.get("AUDIT_LOG_MAX_DAYS"), 30)
    )
    audit_log_maintenance_interval_s = max(
        60, _as_int(os.environ.get("AUDIT_LOG_MAINTENANCE_INTERVAL_S"), 3600)
    )
    events_history_max_rows = max(
        0, _as_int(os.environ.get("EVENTS_HISTORY_MAX_ROWS"), 5000)
    )
    events_history_max_days = max(
        0, _as_int(os.environ.get("EVENTS_HISTORY_MAX_DAYS"), 7)
    )
    events_history_maintenance_interval_s = max(
        60, _as_int(os.environ.get("EVENTS_HISTORY_MAINTENANCE_INTERVAL_S"), 3600)
    )
    events_spool_path = os.environ.get("EVENTS_SPOOL_PATH", "").strip()
    if not events_spool_path:
        events_spool_path = os.path.join(data_dir, "state", "events_spool.jsonl")
    events_spool_max_mb = max(0, _as_int(os.environ.get("EVENTS_SPOOL_MAX_MB"), 50))
    events_spool_flush_interval_s = max(
        0, _as_int(os.environ.get("EVENTS_SPOOL_FLUSH_INTERVAL_S"), 30)
    )
    orchestration_runs_max_rows = max(
        0, _as_int(os.environ.get("ORCHESTRATION_RUNS_MAX_ROWS"), 1000)
    )
    orchestration_runs_max_days = max(
        0, _as_int(os.environ.get("ORCHESTRATION_RUNS_MAX_DAYS"), 90)
    )
    orchestration_runs_maintenance_interval_s = max(
        60,
        _as_int(os.environ.get("ORCHESTRATION_RUNS_MAINTENANCE_INTERVAL_S"), 3600),
    )
    agent_history_max_rows = max(
        0, _as_int(os.environ.get("AGENT_HISTORY_MAX_ROWS"), 2000)
    )
    agent_history_max_days = max(
        0, _as_int(os.environ.get("AGENT_HISTORY_MAX_DAYS"), 14)
    )
    agent_history_maintenance_interval_s = max(
        60, _as_int(os.environ.get("AGENT_HISTORY_MAINTENANCE_INTERVAL_S"), 3600)
    )
    agent_history_interval_s = max(
        0, _as_int(os.environ.get("AGENT_HISTORY_INTERVAL_S"), 300)
    )
    db_reconcile_on_startup = _as_bool(
        os.environ.get("DB_RECONCILE_ON_STARTUP"), default=True
    )
    db_reconcile_interval_s = max(
        0, _as_int(os.environ.get("DB_RECONCILE_INTERVAL_S"), 0)
    )
    db_reconcile_scan_limit = max(
        100, _as_int(os.environ.get("DB_RECONCILE_SCAN_LIMIT"), 5000)
    )
    db_reconcile_include_audio = _as_bool(
        os.environ.get("DB_RECONCILE_AUDIO"), False
    )
    precompute_previews_on_ingest = _as_bool(
        os.environ.get("PRECOMPUTE_PREVIEWS_ON_INGEST"), True
    )
    precompute_waveforms_on_ingest = _as_bool(
        os.environ.get("PRECOMPUTE_WAVEFORMS_ON_INGEST"), True
    )
    precompute_previews_on_reconcile = _as_bool(
        os.environ.get("PRECOMPUTE_PREVIEWS_ON_RECONCILE"), True
    )
    precompute_waveforms_on_reconcile = _as_bool(
        os.environ.get("PRECOMPUTE_WAVEFORMS_ON_RECONCILE"), True
    )
    job_max_jobs = max(10, _as_int(os.environ.get("JOB_MAX_JOBS"), 200))
    job_queue_size = max(1, _as_int(os.environ.get("JOB_QUEUE_SIZE"), 50))
    job_worker_count = max(1, _as_int(os.environ.get("JOB_WORKER_COUNT"), 2))

    wled_http_timeout_s = _as_float(os.environ.get("WLED_HTTP_TIMEOUT_S"), 2.5)
    wled_max_bri = max(1, min(255, _as_int(os.environ.get("WLED_MAX_BRI"), 180)))
    wled_command_cooldown_ms = max(
        0, _as_int(os.environ.get("WLED_COMMAND_COOLDOWN_MS"), 250)
    )

    # Segments
    # If not provided, the app will auto-detect from /json/state on startup (and fall back to [0] if offline).
    seg_ids = _as_int_list(os.environ.get("WLED_SEGMENT_IDS"))
    if not seg_ids:
        seg_count = max(0, _as_int(os.environ.get("WLED_SEGMENT_COUNT"), 0))
        if seg_count > 0:
            seg_ids = tuple(range(seg_count))
    replicate_to_all = _as_bool(os.environ.get("WLED_REPLICATE_TO_ALL_SEGMENTS"), True)

    quad_right_segment_id = _as_int(os.environ.get("QUAD_RIGHT_SEGMENT_ID"), 0)
    quad_order_from_street = _norm_dir(
        os.environ.get("QUAD_ORDER_FROM_STREET"), default="ccw"
    )
    quad_default_start_pos = _norm_pos(
        os.environ.get("QUAD_DEFAULT_START_POS"), default="front"
    )

    tree_runs = max(0, _as_int(os.environ.get("TREE_RUNS"), 16))
    tree_pixels_per_run = max(0, _as_int(os.environ.get("TREE_PIXELS_PER_RUN"), 196))
    tree_segment_len = max(0, _as_int(os.environ.get("TREE_SEGMENT_LEN"), 49))
    tree_segments_per_run = max(0, _as_int(os.environ.get("TREE_SEGMENTS_PER_RUN"), 4))

    ddp_host = os.environ.get("DDP_HOST", "").strip() or _host_from_url(wled_tree_url)
    ddp_port = _as_int(os.environ.get("DDP_PORT"), 4048)
    ddp_destination_id = max(
        1, min(255, _as_int(os.environ.get("DDP_DESTINATION_ID"), 1))
    )
    ddp_max_pixels_per_packet = max(
        1, _as_int(os.environ.get("DDP_MAX_PIXELS_PER_PACKET"), 480)
    )
    ddp_fps_default = max(1.0, _as_float(os.environ.get("DDP_FPS_DEFAULT"), 20.0))
    ddp_fps_max = max(ddp_fps_default, _as_float(os.environ.get("DDP_FPS_MAX"), 45.0))
    ddp_drop_late_frames = _as_bool(os.environ.get("DDP_DROP_LATE_FRAMES"), True)
    ddp_backpressure_max_lag_s = max(
        0.0, _as_float(os.environ.get("DDP_BACKPRESSURE_MAX_LAG_S"), 0.25)
    )
    ddp_use_cpu_pool = _as_bool(os.environ.get("DDP_USE_CPU_POOL"), False)

    sequence_preview_width = max(
        16, _as_int(os.environ.get("SEQUENCE_PREVIEW_WIDTH"), 120)
    )
    sequence_preview_height = max(
        1, _as_int(os.environ.get("SEQUENCE_PREVIEW_HEIGHT"), 24)
    )
    sequence_preview_fps = max(
        1.0, _as_float(os.environ.get("SEQUENCE_PREVIEW_FPS"), 12.0)
    )
    sequence_preview_max_s = max(
        1.0, _as_float(os.environ.get("SEQUENCE_PREVIEW_MAX_S"), 20.0)
    )
    sequence_preview_cache_max_mb = max(
        0, _as_int(os.environ.get("SEQUENCE_PREVIEW_CACHE_MAX_MB"), 256)
    )
    sequence_preview_cache_max_days = max(
        0.0, _as_float(os.environ.get("SEQUENCE_PREVIEW_CACHE_MAX_DAYS"), 7.0)
    )
    waveform_cache_max_mb = max(
        0, _as_int(os.environ.get("WAVEFORM_CACHE_MAX_MB"), 128)
    )
    waveform_cache_max_days = max(
        0.0, _as_float(os.environ.get("WAVEFORM_CACHE_MAX_DAYS"), 7.0)
    )
    waveform_points_default = max(
        32, _as_int(os.environ.get("WAVEFORM_POINTS_DEFAULT"), 512)
    )

    openai_api_key = os.environ.get("OPENAI_API_KEY", "").strip() or None
    openai_model = os.environ.get("OPENAI_MODEL", "gpt-5-mini").strip() or "gpt-5-mini"
    openai_stt_model = (
        os.environ.get("OPENAI_STT_MODEL", "gpt-4o-mini-transcribe").strip()
        or "gpt-4o-mini-transcribe"
    )

    agent_id = _as_str(os.environ.get("AGENT_ID"), default="wled-agent")
    agent_name = _as_str(os.environ.get("AGENT_NAME"), default=agent_id)
    agent_role = _as_str(os.environ.get("AGENT_ROLE"), default="device")
    agent_base_url = os.environ.get("AGENT_BASE_URL", "").strip() or None
    if not agent_base_url:
        # Default to docker-compose service DNS if we're in a container.
        try:
            if os.path.exists("/.dockerenv") and agent_id:
                agent_base_url = f"http://{agent_id}:8088"
        except Exception:
            agent_base_url = None
    agent_tags = _as_csv(os.environ.get("AGENT_TAGS"))

    mqtt_url = os.environ.get("MQTT_URL", "").strip()
    mqtt_host = os.environ.get("MQTT_HOST", "").strip()
    mqtt_port = max(1, _as_int(os.environ.get("MQTT_PORT"), 1883))

    if not mqtt_url and mqtt_host:
        mqtt_url = f"mqtt://{mqtt_host}:{mqtt_port}"
    mqtt_enabled = _as_bool(os.environ.get("MQTT_ENABLED"), default=bool(mqtt_url))
    mqtt_username = os.environ.get("MQTT_USERNAME", "").strip() or None
    mqtt_password = os.environ.get("MQTT_PASSWORD", "").strip() or None
    mqtt_base_topic = (
        os.environ.get("MQTT_BASE_TOPIC", f"wsa/{agent_id}").strip()
        or f"wsa/{agent_id}"
    )
    mqtt_qos = max(0, min(2, _as_int(os.environ.get("MQTT_QOS"), 0)))
    mqtt_status_interval_s = max(
        0, _as_int(os.environ.get("MQTT_STATUS_INTERVAL_S"), 30)
    )
    mqtt_reconnect_interval_s = max(
        1.0, _as_float(os.environ.get("MQTT_RECONNECT_INTERVAL_S"), 5.0)
    )
    ha_mqtt_discovery_enabled = _as_bool(
        os.environ.get("HA_MQTT_DISCOVERY_ENABLED"), default=False
    )
    ha_mqtt_discovery_prefix = (
        os.environ.get("HA_MQTT_DISCOVERY_PREFIX", "homeassistant").strip()
        or "homeassistant"
    )
    ha_mqtt_entity_prefix = (
        os.environ.get("HA_MQTT_ENTITY_PREFIX", "").strip() or agent_name
    )

    scheduler_leader_roles = _as_csv(os.environ.get("SCHEDULER_LEADER_ROLES"))
    if not scheduler_leader_roles:
        scheduler_leader_roles = ("tree", "device")

    fleet_stale_after_s = max(
        1.0, _as_float(os.environ.get("FLEET_STALE_AFTER_S"), 30.0)
    )
    fleet_db_discovery_enabled = _as_bool(
        os.environ.get("FLEET_DB_DISCOVERY_ENABLED"), default=True
    )
    fleet_health_cache_ttl_s = max(
        0.0, _as_float(os.environ.get("FLEET_HEALTH_CACHE_TTL_S"), 15.0)
    )
    a2a_api_key = os.environ.get("A2A_API_KEY", "").strip() or None
    a2a_peers = _as_csv(os.environ.get("A2A_PEERS"))
    a2a_http_timeout_s = max(0.5, _as_float(os.environ.get("A2A_HTTP_TIMEOUT_S"), 2.5))

    outbound_retry_attempts = max(
        1, _as_int(os.environ.get("OUTBOUND_RETRY_ATTEMPTS"), 2)
    )
    outbound_retry_backoff_base_s = max(
        0.01, _as_float(os.environ.get("OUTBOUND_RETRY_BACKOFF_BASE_S"), 0.15)
    )
    outbound_retry_backoff_max_s = max(
        float(outbound_retry_backoff_base_s),
        _as_float(os.environ.get("OUTBOUND_RETRY_BACKOFF_MAX_S"), 1.0),
    )
    outbound_retry_status_codes = _as_int_list(
        os.environ.get("OUTBOUND_RETRY_STATUS_CODES")
    )
    if not outbound_retry_status_codes:
        outbound_retry_status_codes = (408, 425, 429, 500, 502, 503, 504)

    pixel_protocol = (
        _as_str(os.environ.get("PIXEL_PROTOCOL"), default="e131").strip().lower()
        or "e131"
    )
    pixel_host = _as_str(os.environ.get("PIXEL_HOST"), default="")
    pixel_port_default = 5568 if pixel_protocol == "e131" else 6454
    pixel_port = _as_int(os.environ.get("PIXEL_PORT"), pixel_port_default)
    pixel_universe_start_default = 1 if pixel_protocol == "e131" else 0
    pixel_universe_start = max(
        0, _as_int(os.environ.get("PIXEL_UNIVERSE_START"), pixel_universe_start_default)
    )
    pixel_channels_per_universe = max(
        1, min(512, _as_int(os.environ.get("PIXEL_CHANNELS_PER_UNIVERSE"), 510))
    )
    pixel_count = max(0, _as_int(os.environ.get("PIXEL_COUNT"), 0))
    pixel_priority = max(0, min(200, _as_int(os.environ.get("PIXEL_PRIORITY"), 100)))
    pixel_source_name = _as_str(
        os.environ.get("PIXEL_SOURCE_NAME"), default=agent_name
    )[:64]

    fpp_base_url = _as_str(os.environ.get("FPP_BASE_URL"), default="").rstrip("/")
    fpp_http_timeout_s = max(0.5, _as_float(os.environ.get("FPP_HTTP_TIMEOUT_S"), 2.5))
    fpp_headers = _as_json_headers(os.environ.get("FPP_HEADERS_JSON"))

    ledfx_base_url = _as_str(os.environ.get("LEDFX_BASE_URL"), default="").rstrip("/")
    ledfx_http_timeout_s = max(
        0.5, _as_float(os.environ.get("LEDFX_HTTP_TIMEOUT_S"), 2.5)
    )
    ledfx_headers = _as_json_headers(os.environ.get("LEDFX_HEADERS_JSON"))
    ledfx_fleet_cache_ttl_s = max(
        0.0, _as_float(os.environ.get("LEDFX_FLEET_CACHE_TTL_S"), 15.0)
    )

    ui_enabled = _as_bool(os.environ.get("UI_ENABLED"), True)
    auth_enabled = _as_bool(os.environ.get("AUTH_ENABLED"), True)
    auth_username = _as_str(os.environ.get("AUTH_USERNAME"), default="admin")
    auth_password = os.environ.get("AUTH_PASSWORD", "").strip() or None
    auth_jwt_secret = os.environ.get("AUTH_JWT_SECRET", "").strip() or None
    auth_jwt_ttl_s = max(60, _as_int(os.environ.get("AUTH_JWT_TTL_S"), 12 * 60 * 60))
    auth_jwt_issuer = _as_str(
        os.environ.get("AUTH_JWT_ISSUER"), default="wled-show-agent"
    )
    auth_cookie_name = _as_str(os.environ.get("AUTH_COOKIE_NAME"), default="wsa_token")
    auth_cookie_secure = _as_bool(os.environ.get("AUTH_COOKIE_SECURE"), False)
    auth_csrf_enabled = _as_bool(os.environ.get("AUTH_CSRF_ENABLED"), True)
    auth_csrf_cookie_name = _as_str(
        os.environ.get("AUTH_CSRF_COOKIE_NAME"), default="wsa_csrf"
    )
    auth_csrf_header_name = _as_str(
        os.environ.get("AUTH_CSRF_HEADER_NAME"), default="X-CSRF-Token"
    )
    auth_totp_enabled = _as_bool(os.environ.get("AUTH_TOTP_ENABLED"), True)
    auth_totp_secret = (
        os.environ.get("AUTH_TOTP_SECRET", "").strip().replace(" ", "") or None
    )
    auth_totp_issuer = _as_str(
        os.environ.get("AUTH_TOTP_ISSUER"), default=auth_jwt_issuer
    )
    auth_user_role = _as_str(os.environ.get("AUTH_USER_ROLE"), default="admin")
    auth_users = _as_json_list(os.environ.get("AUTH_USERS_JSON"))
    auth_login_max_attempts = max(
        1, _as_int(os.environ.get("AUTH_LOGIN_MAX_ATTEMPTS"), 5)
    )
    auth_login_window_s = max(
        30, _as_int(os.environ.get("AUTH_LOGIN_WINDOW_S"), 300)
    )
    auth_login_lockout_s = max(
        30, _as_int(os.environ.get("AUTH_LOGIN_LOCKOUT_S"), 900)
    )
    auth_session_cleanup_interval_s = max(
        60, _as_int(os.environ.get("AUTH_SESSION_CLEANUP_INTERVAL_S"), 3600)
    )
    auth_session_cleanup_max_age_s = max(
        0, _as_int(os.environ.get("AUTH_SESSION_CLEANUP_MAX_AGE_S"), 0)
    )
    auth_session_touch_interval_s = max(
        15, _as_int(os.environ.get("AUTH_SESSION_TOUCH_INTERVAL_S"), 60)
    )

    metrics_public = _as_bool(os.environ.get("METRICS_PUBLIC"), False)
    metrics_scrape_token = os.environ.get("METRICS_SCRAPE_TOKEN", "").strip() or None
    metrics_scrape_header = _as_str(
        os.environ.get("METRICS_SCRAPE_HEADER"), default="X-Metrics-Token"
    )
    rate_limit_enabled = _as_bool(os.environ.get("RATE_LIMIT_ENABLED"), True)
    rate_limit_requests_per_minute = max(
        1, _as_int(os.environ.get("RATE_LIMIT_REQUESTS_PER_MINUTE"), 600)
    )
    rate_limit_burst = max(1, _as_int(os.environ.get("RATE_LIMIT_BURST"), 120))
    rate_limit_scope = _as_str(
        os.environ.get("RATE_LIMIT_SCOPE"), default="ip"
    ).strip() or "ip"
    rate_limit_exempt_paths = _as_csv(os.environ.get("RATE_LIMIT_EXEMPT_PATHS"))
    rate_limit_bucket_ttl_s = max(
        30.0, _as_float(os.environ.get("RATE_LIMIT_BUCKET_TTL_S"), 600.0)
    )
    rate_limit_cleanup_interval_s = max(
        5.0, _as_float(os.environ.get("RATE_LIMIT_CLEANUP_INTERVAL_S"), 30.0)
    )
    rate_limit_trust_proxy_headers = _as_bool(
        os.environ.get("RATE_LIMIT_TRUST_PROXY_HEADERS"), False
    )
    files_upload_allowlist_only = _as_bool(
        os.environ.get("FILES_UPLOAD_ALLOWLIST_ONLY"), False
    )
    backup_max_zip_mb = max(1, _as_int(os.environ.get("BACKUP_MAX_ZIP_MB"), 512))
    backup_max_unpacked_mb = max(
        1, _as_int(os.environ.get("BACKUP_MAX_UNPACKED_MB"), 2048)
    )
    backup_max_file_mb = max(1, _as_int(os.environ.get("BACKUP_MAX_FILE_MB"), 256))
    backup_max_files = max(1, _as_int(os.environ.get("BACKUP_MAX_FILES"), 20000))
    backup_exclude_globs = _as_csv(os.environ.get("BACKUP_EXCLUDE_GLOBS"))
    backup_spool_max_mb = max(1, _as_int(os.environ.get("BACKUP_SPOOL_MAX_MB"), 50))
    blocking_max_workers = max(1, _as_int(os.environ.get("BLOCKING_MAX_WORKERS"), 4))
    blocking_max_queue = max(1, _as_int(os.environ.get("BLOCKING_MAX_QUEUE"), 16))
    blocking_queue_timeout_s = max(
        0.1, _as_float(os.environ.get("BLOCKING_QUEUE_TIMEOUT_S"), 2.0)
    )
    ddp_blocking_max_workers = max(
        1, _as_int(os.environ.get("DDP_BLOCKING_MAX_WORKERS"), 1)
    )
    ddp_blocking_max_queue = max(
        1, _as_int(os.environ.get("DDP_BLOCKING_MAX_QUEUE"), 2)
    )
    ddp_blocking_queue_timeout_s = max(
        0.01, _as_float(os.environ.get("DDP_BLOCKING_QUEUE_TIMEOUT_S"), 0.05)
    )
    cpu_pool_max_workers = max(
        1, _as_int(os.environ.get("CPU_POOL_MAX_WORKERS"), 2)
    )
    cpu_pool_max_queue = max(
        1, _as_int(os.environ.get("CPU_POOL_MAX_QUEUE"), 8)
    )
    cpu_pool_queue_timeout_s = max(
        0.1, _as_float(os.environ.get("CPU_POOL_QUEUE_TIMEOUT_S"), 2.0)
    )

    if not auth_enabled:
        raise RuntimeError("AUTH_ENABLED must be true (auth is required)")
    if not auth_password:
        raise RuntimeError("AUTH_PASSWORD is required when AUTH_ENABLED=true")
    if not auth_jwt_secret:
        raise RuntimeError("AUTH_JWT_SECRET is required when AUTH_ENABLED=true")
    if not auth_totp_enabled:
        raise RuntimeError("AUTH_TOTP_ENABLED must be true (TOTP is required)")
    if not auth_totp_secret:
        raise RuntimeError("AUTH_TOTP_SECRET is required when AUTH_TOTP_ENABLED=true")

    if controller_kind == "pixel":
        if not pixel_host:
            raise RuntimeError(
                "PIXEL_HOST is required when CONTROLLER_KIND=pixel (e.g. 10.0.0.60)"
            )
        if pixel_count <= 0:
            raise RuntimeError(
                "PIXEL_COUNT is required when CONTROLLER_KIND=pixel (e.g. 50)"
            )
        if pixel_protocol not in ("e131", "artnet"):
            raise RuntimeError(
                "PIXEL_PROTOCOL must be 'e131' or 'artnet' when CONTROLLER_KIND=pixel"
            )

    return Settings(
        controller_kind=controller_kind,
        wled_tree_url=wled_tree_url.rstrip("/"),
        wled_http_timeout_s=wled_http_timeout_s,
        wled_max_bri=wled_max_bri,
        wled_command_cooldown_ms=wled_command_cooldown_ms,
        wled_segment_ids=seg_ids,
        wled_replicate_to_all_segments=replicate_to_all,
        quad_right_segment_id=quad_right_segment_id,
        quad_order_from_street=quad_order_from_street,
        quad_default_start_pos=quad_default_start_pos,
        tree_runs=tree_runs,
        tree_pixels_per_run=tree_pixels_per_run,
        tree_segment_len=tree_segment_len,
        tree_segments_per_run=tree_segments_per_run,
        ddp_host=ddp_host,
        ddp_port=ddp_port,
        ddp_destination_id=ddp_destination_id,
        ddp_max_pixels_per_packet=ddp_max_pixels_per_packet,
        ddp_fps_default=ddp_fps_default,
        ddp_fps_max=ddp_fps_max,
        ddp_drop_late_frames=ddp_drop_late_frames,
        ddp_backpressure_max_lag_s=ddp_backpressure_max_lag_s,
        ddp_use_cpu_pool=ddp_use_cpu_pool,
        sequence_preview_width=sequence_preview_width,
        sequence_preview_height=sequence_preview_height,
        sequence_preview_fps=sequence_preview_fps,
        sequence_preview_max_s=sequence_preview_max_s,
        sequence_preview_cache_max_mb=sequence_preview_cache_max_mb,
        sequence_preview_cache_max_days=sequence_preview_cache_max_days,
        waveform_cache_max_mb=waveform_cache_max_mb,
        waveform_cache_max_days=waveform_cache_max_days,
        waveform_points_default=waveform_points_default,
        pixel_host=pixel_host,
        pixel_port=pixel_port,
        pixel_protocol=pixel_protocol,
        pixel_universe_start=pixel_universe_start,
        pixel_channels_per_universe=pixel_channels_per_universe,
        pixel_count=pixel_count,
        pixel_priority=pixel_priority,
        pixel_source_name=pixel_source_name,
        data_dir=data_dir,
        database_url=database_url,
        db_migrate_on_startup=db_migrate_on_startup,
        job_history_max_rows=job_history_max_rows,
        job_history_max_days=job_history_max_days,
        job_history_maintenance_interval_s=job_history_maintenance_interval_s,
        metrics_history_interval_s=metrics_history_interval_s,
        metrics_history_max_rows=metrics_history_max_rows,
        metrics_history_max_days=metrics_history_max_days,
        metrics_history_maintenance_interval_s=metrics_history_maintenance_interval_s,
        scheduler_events_max_rows=scheduler_events_max_rows,
        scheduler_events_max_days=scheduler_events_max_days,
        scheduler_events_maintenance_interval_s=scheduler_events_maintenance_interval_s,
        pack_ingests_max_rows=pack_ingests_max_rows,
        pack_ingests_max_days=pack_ingests_max_days,
        pack_ingests_maintenance_interval_s=pack_ingests_maintenance_interval_s,
        sequence_meta_max_rows=sequence_meta_max_rows,
        sequence_meta_max_days=sequence_meta_max_days,
        sequence_meta_maintenance_interval_s=sequence_meta_maintenance_interval_s,
        audio_analyses_max_rows=audio_analyses_max_rows,
        audio_analyses_max_days=audio_analyses_max_days,
        audio_analyses_maintenance_interval_s=audio_analyses_maintenance_interval_s,
        show_configs_max_rows=show_configs_max_rows,
        show_configs_max_days=show_configs_max_days,
        show_configs_maintenance_interval_s=show_configs_maintenance_interval_s,
        fseq_exports_max_rows=fseq_exports_max_rows,
        fseq_exports_max_days=fseq_exports_max_days,
        fseq_exports_maintenance_interval_s=fseq_exports_maintenance_interval_s,
        fpp_scripts_max_rows=fpp_scripts_max_rows,
        fpp_scripts_max_days=fpp_scripts_max_days,
        fpp_scripts_maintenance_interval_s=fpp_scripts_maintenance_interval_s,
        audit_log_max_rows=audit_log_max_rows,
        audit_log_max_days=audit_log_max_days,
        audit_log_maintenance_interval_s=audit_log_maintenance_interval_s,
        events_history_max_rows=events_history_max_rows,
        events_history_max_days=events_history_max_days,
        events_history_maintenance_interval_s=events_history_maintenance_interval_s,
        events_spool_path=events_spool_path,
        events_spool_max_mb=events_spool_max_mb,
        events_spool_flush_interval_s=events_spool_flush_interval_s,
        orchestration_runs_max_rows=orchestration_runs_max_rows,
        orchestration_runs_max_days=orchestration_runs_max_days,
        orchestration_runs_maintenance_interval_s=orchestration_runs_maintenance_interval_s,
        agent_history_max_rows=agent_history_max_rows,
        agent_history_max_days=agent_history_max_days,
        agent_history_maintenance_interval_s=agent_history_maintenance_interval_s,
        agent_history_interval_s=agent_history_interval_s,
        db_reconcile_on_startup=bool(db_reconcile_on_startup),
        db_reconcile_interval_s=db_reconcile_interval_s,
        db_reconcile_scan_limit=db_reconcile_scan_limit,
        db_reconcile_include_audio=bool(db_reconcile_include_audio),
        precompute_previews_on_ingest=bool(precompute_previews_on_ingest),
        precompute_waveforms_on_ingest=bool(precompute_waveforms_on_ingest),
        precompute_previews_on_reconcile=bool(precompute_previews_on_reconcile),
        precompute_waveforms_on_reconcile=bool(precompute_waveforms_on_reconcile),
        job_max_jobs=job_max_jobs,
        job_queue_size=job_queue_size,
        job_worker_count=job_worker_count,
        openai_api_key=openai_api_key,
        openai_model=openai_model,
        openai_stt_model=openai_stt_model,
        agent_id=agent_id,
        agent_name=agent_name,
        agent_role=agent_role,
        agent_base_url=agent_base_url,
        agent_tags=agent_tags,
        scheduler_leader_roles=scheduler_leader_roles,
        fleet_stale_after_s=float(fleet_stale_after_s),
        fleet_db_discovery_enabled=bool(fleet_db_discovery_enabled),
        fleet_health_cache_ttl_s=float(fleet_health_cache_ttl_s),
        a2a_api_key=a2a_api_key,
        a2a_peers=a2a_peers,
        a2a_http_timeout_s=a2a_http_timeout_s,
        outbound_retry_attempts=outbound_retry_attempts,
        outbound_retry_backoff_base_s=outbound_retry_backoff_base_s,
        outbound_retry_backoff_max_s=outbound_retry_backoff_max_s,
        outbound_retry_status_codes=tuple(outbound_retry_status_codes),
        fpp_base_url=fpp_base_url,
        fpp_http_timeout_s=fpp_http_timeout_s,
        fpp_headers=fpp_headers,
        ledfx_base_url=ledfx_base_url,
        ledfx_http_timeout_s=ledfx_http_timeout_s,
        ledfx_headers=ledfx_headers,
        ledfx_fleet_cache_ttl_s=ledfx_fleet_cache_ttl_s,
        ui_enabled=ui_enabled,
        auth_enabled=auth_enabled,
        auth_username=auth_username,
        auth_password=auth_password,
        auth_jwt_secret=auth_jwt_secret,
        auth_jwt_ttl_s=auth_jwt_ttl_s,
        auth_jwt_issuer=auth_jwt_issuer,
        auth_cookie_name=auth_cookie_name,
        auth_cookie_secure=auth_cookie_secure,
        auth_csrf_enabled=auth_csrf_enabled,
        auth_csrf_cookie_name=auth_csrf_cookie_name,
        auth_csrf_header_name=auth_csrf_header_name,
        auth_totp_enabled=auth_totp_enabled,
        auth_totp_secret=auth_totp_secret,
        auth_totp_issuer=auth_totp_issuer,
        auth_user_role=auth_user_role,
        auth_users=auth_users,
        auth_login_max_attempts=auth_login_max_attempts,
        auth_login_window_s=auth_login_window_s,
        auth_login_lockout_s=auth_login_lockout_s,
        auth_session_cleanup_interval_s=auth_session_cleanup_interval_s,
        auth_session_cleanup_max_age_s=auth_session_cleanup_max_age_s,
        auth_session_touch_interval_s=auth_session_touch_interval_s,
        metrics_public=metrics_public,
        metrics_scrape_token=metrics_scrape_token,
        metrics_scrape_header=metrics_scrape_header,
        rate_limit_enabled=rate_limit_enabled,
        rate_limit_requests_per_minute=rate_limit_requests_per_minute,
        rate_limit_burst=rate_limit_burst,
        rate_limit_scope=rate_limit_scope,
        rate_limit_exempt_paths=rate_limit_exempt_paths,
        rate_limit_bucket_ttl_s=rate_limit_bucket_ttl_s,
        rate_limit_cleanup_interval_s=rate_limit_cleanup_interval_s,
        rate_limit_trust_proxy_headers=rate_limit_trust_proxy_headers,
        mqtt_enabled=mqtt_enabled,
        mqtt_url=mqtt_url,
        mqtt_username=mqtt_username,
        mqtt_password=mqtt_password,
        mqtt_base_topic=mqtt_base_topic,
        mqtt_qos=mqtt_qos,
        mqtt_status_interval_s=mqtt_status_interval_s,
        mqtt_reconnect_interval_s=mqtt_reconnect_interval_s,
        ha_mqtt_discovery_enabled=ha_mqtt_discovery_enabled,
        ha_mqtt_discovery_prefix=ha_mqtt_discovery_prefix,
        ha_mqtt_entity_prefix=ha_mqtt_entity_prefix,
        files_upload_allowlist_only=files_upload_allowlist_only,
        backup_max_zip_mb=backup_max_zip_mb,
        backup_max_unpacked_mb=backup_max_unpacked_mb,
        backup_max_file_mb=backup_max_file_mb,
        backup_max_files=backup_max_files,
        backup_exclude_globs=backup_exclude_globs,
        backup_spool_max_mb=backup_spool_max_mb,
        blocking_max_workers=blocking_max_workers,
        blocking_max_queue=blocking_max_queue,
        blocking_queue_timeout_s=blocking_queue_timeout_s,
        ddp_blocking_max_workers=ddp_blocking_max_workers,
        ddp_blocking_max_queue=ddp_blocking_max_queue,
        ddp_blocking_queue_timeout_s=ddp_blocking_queue_timeout_s,
        cpu_pool_max_workers=cpu_pool_max_workers,
        cpu_pool_max_queue=cpu_pool_max_queue,
        cpu_pool_queue_timeout_s=cpu_pool_queue_timeout_s,
    )
