from __future__ import annotations

import os
import json
from dataclasses import dataclass
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
    job_history_max_rows: int
    job_history_max_days: int
    job_history_maintenance_interval_s: int
    scheduler_events_max_rows: int
    scheduler_events_max_days: int
    scheduler_events_maintenance_interval_s: int
    db_reconcile_on_startup: bool

    # OpenAI
    openai_api_key: str | None
    openai_model: str

    # Agent identity / A2A
    agent_id: str
    agent_name: str
    agent_role: str
    agent_base_url: str | None
    agent_tags: tuple[str, ...]
    scheduler_leader_roles: tuple[str, ...]
    a2a_api_key: str | None
    a2a_peers: tuple[str, ...]
    a2a_http_timeout_s: float

    # Falcon Player (FPP) integration (optional)
    fpp_base_url: str
    fpp_http_timeout_s: float
    fpp_headers: tuple[tuple[str, str], ...]

    # UI + auth (optional)
    ui_enabled: bool
    auth_enabled: bool
    auth_username: str
    auth_password: str | None
    auth_jwt_secret: str | None
    auth_jwt_ttl_s: int
    auth_jwt_issuer: str
    auth_cookie_name: str
    auth_cookie_secure: bool
    auth_totp_enabled: bool
    auth_totp_secret: str | None
    auth_totp_issuer: str

    # Metrics / Prometheus
    metrics_public: bool
    metrics_scrape_token: str | None
    metrics_scrape_header: str

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
    job_history_max_rows = max(0, _as_int(os.environ.get("JOB_HISTORY_MAX_ROWS"), 2000))
    job_history_max_days = max(0, _as_int(os.environ.get("JOB_HISTORY_MAX_DAYS"), 30))
    job_history_maintenance_interval_s = max(
        60, _as_int(os.environ.get("JOB_HISTORY_MAINTENANCE_INTERVAL_S"), 3600)
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
    db_reconcile_on_startup = _as_bool(
        os.environ.get("DB_RECONCILE_ON_STARTUP"), default=True
    )

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

    openai_api_key = os.environ.get("OPENAI_API_KEY", "").strip() or None
    openai_model = os.environ.get("OPENAI_MODEL", "gpt-5-mini").strip() or "gpt-5-mini"

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

    scheduler_leader_roles = _as_csv(os.environ.get("SCHEDULER_LEADER_ROLES"))
    if not scheduler_leader_roles:
        scheduler_leader_roles = ("tree", "device")
    a2a_api_key = os.environ.get("A2A_API_KEY", "").strip() or None
    a2a_peers = _as_csv(os.environ.get("A2A_PEERS"))
    a2a_http_timeout_s = max(0.5, _as_float(os.environ.get("A2A_HTTP_TIMEOUT_S"), 2.5))

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

    ui_enabled = _as_bool(os.environ.get("UI_ENABLED"), True)
    auth_enabled = _as_bool(os.environ.get("AUTH_ENABLED"), False)
    auth_username = _as_str(os.environ.get("AUTH_USERNAME"), default="admin")
    auth_password = os.environ.get("AUTH_PASSWORD", "").strip() or None
    auth_jwt_secret = os.environ.get("AUTH_JWT_SECRET", "").strip() or None
    auth_jwt_ttl_s = max(60, _as_int(os.environ.get("AUTH_JWT_TTL_S"), 12 * 60 * 60))
    auth_jwt_issuer = _as_str(
        os.environ.get("AUTH_JWT_ISSUER"), default="wled-show-agent"
    )
    auth_cookie_name = _as_str(os.environ.get("AUTH_COOKIE_NAME"), default="wsa_token")
    auth_cookie_secure = _as_bool(os.environ.get("AUTH_COOKIE_SECURE"), False)
    auth_totp_enabled = _as_bool(os.environ.get("AUTH_TOTP_ENABLED"), False)
    auth_totp_secret = (
        os.environ.get("AUTH_TOTP_SECRET", "").strip().replace(" ", "") or None
    )
    auth_totp_issuer = _as_str(
        os.environ.get("AUTH_TOTP_ISSUER"), default=auth_jwt_issuer
    )

    metrics_public = _as_bool(os.environ.get("METRICS_PUBLIC"), False)
    metrics_scrape_token = os.environ.get("METRICS_SCRAPE_TOKEN", "").strip() or None
    metrics_scrape_header = _as_str(
        os.environ.get("METRICS_SCRAPE_HEADER"), default="X-Metrics-Token"
    )

    if auth_enabled:
        if not auth_password:
            raise RuntimeError("AUTH_PASSWORD is required when AUTH_ENABLED=true")
        if not auth_jwt_secret:
            raise RuntimeError("AUTH_JWT_SECRET is required when AUTH_ENABLED=true")
        if auth_totp_enabled and not auth_totp_secret:
            raise RuntimeError(
                "AUTH_TOTP_SECRET is required when AUTH_TOTP_ENABLED=true"
            )

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
        job_history_max_rows=job_history_max_rows,
        job_history_max_days=job_history_max_days,
        job_history_maintenance_interval_s=job_history_maintenance_interval_s,
        scheduler_events_max_rows=scheduler_events_max_rows,
        scheduler_events_max_days=scheduler_events_max_days,
        scheduler_events_maintenance_interval_s=scheduler_events_maintenance_interval_s,
        db_reconcile_on_startup=bool(db_reconcile_on_startup),
        openai_api_key=openai_api_key,
        openai_model=openai_model,
        agent_id=agent_id,
        agent_name=agent_name,
        agent_role=agent_role,
        agent_base_url=agent_base_url,
        agent_tags=agent_tags,
        scheduler_leader_roles=scheduler_leader_roles,
        a2a_api_key=a2a_api_key,
        a2a_peers=a2a_peers,
        a2a_http_timeout_s=a2a_http_timeout_s,
        fpp_base_url=fpp_base_url,
        fpp_http_timeout_s=fpp_http_timeout_s,
        fpp_headers=fpp_headers,
        ui_enabled=ui_enabled,
        auth_enabled=auth_enabled,
        auth_username=auth_username,
        auth_password=auth_password,
        auth_jwt_secret=auth_jwt_secret,
        auth_jwt_ttl_s=auth_jwt_ttl_s,
        auth_jwt_issuer=auth_jwt_issuer,
        auth_cookie_name=auth_cookie_name,
        auth_cookie_secure=auth_cookie_secure,
        auth_totp_enabled=auth_totp_enabled,
        auth_totp_secret=auth_totp_secret,
        auth_totp_issuer=auth_totp_issuer,
        metrics_public=metrics_public,
        metrics_scrape_token=metrics_scrape_token,
        metrics_scrape_header=metrics_scrape_header,
    )
