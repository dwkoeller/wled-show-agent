from __future__ import annotations

from typing import Any, Dict, List, Optional

from sqlalchemy import Column, Index
from sqlalchemy.types import JSON
from sqlmodel import Field, SQLModel


class SchemaVersion(SQLModel, table=True):
    __tablename__ = "schema_version"

    id: int = Field(default=1, primary_key=True)
    version: int = Field(index=True)
    updated_at: float = Field(index=True)


class JobRecord(SQLModel, table=True):
    __tablename__ = "jobs"
    __table_args__ = (
        Index("ix_jobs_agent_created_at", "agent_id", "created_at"),
        Index("ix_jobs_agent_status_created_at", "agent_id", "status", "created_at"),
    )

    agent_id: str = Field(primary_key=True, max_length=128)
    id: str = Field(primary_key=True, max_length=64)

    kind: str = Field(index=True, max_length=128)
    status: str = Field(index=True, max_length=32)
    created_at: float = Field(index=True)
    started_at: Optional[float] = None
    finished_at: Optional[float] = None
    updated_at: float = Field(index=True)

    payload: Dict[str, Any] = Field(sa_column=Column(JSON), default_factory=dict)


class KVRecord(SQLModel, table=True):
    __tablename__ = "kv"
    __table_args__ = (Index("ix_kv_agent_updated_at", "agent_id", "updated_at"),)

    agent_id: str = Field(primary_key=True, max_length=128)
    key: str = Field(primary_key=True, max_length=256)

    updated_at: float = Field(index=True)
    value: Dict[str, Any] = Field(sa_column=Column(JSON), default_factory=dict)


class GlobalKVRecord(SQLModel, table=True):
    __tablename__ = "kv_global"

    key: str = Field(primary_key=True, max_length=256)
    updated_at: float = Field(index=True)
    value: Dict[str, Any] = Field(sa_column=Column(JSON), default_factory=dict)


class PackIngestRecord(SQLModel, table=True):
    """
    Metadata for uploaded/unpacked zip packs under DATA_DIR.

    This stores metadata only (paths, sizes, timestamps) and not the file contents.
    """

    __tablename__ = "pack_ingests"
    __table_args__ = (
        Index("ix_pack_ingests_agent_updated_at", "agent_id", "updated_at"),
    )

    agent_id: str = Field(primary_key=True, max_length=128)
    dest_dir: str = Field(primary_key=True, max_length=512)  # relative to DATA_DIR

    created_at: float = Field(index=True)
    updated_at: float = Field(index=True)

    source_name: str | None = Field(default=None, max_length=256)
    manifest_path: str | None = Field(default=None, max_length=512)

    uploaded_bytes: int = Field(default=0)
    unpacked_bytes: int = Field(default=0)
    file_count: int = Field(default=0)


class SequenceMetaRecord(SQLModel, table=True):
    """
    Metadata for sequences stored under DATA_DIR/sequences.
    """

    __tablename__ = "sequence_meta"
    __table_args__ = (
        Index("ix_sequence_meta_agent_updated_at", "agent_id", "updated_at"),
    )

    agent_id: str = Field(primary_key=True, max_length=128)
    file: str = Field(
        primary_key=True, max_length=512
    )  # relative path under DATA_DIR/sequences

    created_at: float = Field(index=True)
    updated_at: float = Field(index=True)

    duration_s: float = Field(default=0.0)
    steps_total: int = Field(default=0)


class ShowConfigRecord(SQLModel, table=True):
    """
    Metadata for show config JSON files under DATA_DIR/show.
    """

    __tablename__ = "show_configs"
    __table_args__ = (
        Index("ix_show_configs_agent_updated_at", "agent_id", "updated_at"),
    )

    agent_id: str = Field(primary_key=True, max_length=128)
    file: str = Field(primary_key=True, max_length=512)

    created_at: float = Field(index=True)
    updated_at: float = Field(index=True)

    name: str = Field(default="", max_length=256)
    props_total: int = Field(default=0)
    groups_total: int = Field(default=0)
    coordinator_base_url: str | None = Field(default=None, max_length=512)
    fpp_base_url: str | None = Field(default=None, max_length=512)
    payload: Dict[str, Any] = Field(sa_column=Column(JSON), default_factory=dict)


class FseqExportRecord(SQLModel, table=True):
    """
    Metadata for FSEQ exports written under DATA_DIR/fseq.
    """

    __tablename__ = "fseq_exports"
    __table_args__ = (
        Index("ix_fseq_exports_agent_updated_at", "agent_id", "updated_at"),
    )

    agent_id: str = Field(primary_key=True, max_length=128)
    file: str = Field(primary_key=True, max_length=512)

    created_at: float = Field(index=True)
    updated_at: float = Field(index=True)

    source_sequence: str | None = Field(default=None, max_length=512)
    bytes_written: int = Field(default=0)
    frames: int | None = None
    channels: int | None = None
    step_ms: int | None = None
    duration_s: float | None = None
    payload: Dict[str, Any] = Field(sa_column=Column(JSON), default_factory=dict)


class FppScriptRecord(SQLModel, table=True):
    """
    Metadata for FPP helper scripts written under DATA_DIR/fpp/scripts.
    """

    __tablename__ = "fpp_scripts"
    __table_args__ = (
        Index("ix_fpp_scripts_agent_updated_at", "agent_id", "updated_at"),
    )

    agent_id: str = Field(primary_key=True, max_length=128)
    file: str = Field(primary_key=True, max_length=512)

    created_at: float = Field(index=True)
    updated_at: float = Field(index=True)

    kind: str = Field(default="", max_length=64)
    bytes_written: int = Field(default=0)
    payload: Dict[str, Any] = Field(sa_column=Column(JSON), default_factory=dict)


class AudioAnalysisRecord(SQLModel, table=True):
    """
    Metadata for audio analysis runs (beats/BPM extraction).
    """

    __tablename__ = "audio_analyses"
    __table_args__ = (
        Index("ix_audio_analyses_agent_created_at", "agent_id", "created_at"),
        Index("ix_audio_analyses_agent_updated_at", "agent_id", "updated_at"),
    )

    agent_id: str = Field(primary_key=True, max_length=128)
    id: str = Field(primary_key=True, max_length=64)

    created_at: float = Field(index=True)
    updated_at: float = Field(index=True)

    source_path: str | None = Field(default=None, max_length=512)
    beats_path: str | None = Field(default=None, max_length=512)
    prefer_ffmpeg: bool = Field(default=False)

    bpm: float | None = None
    beat_count: int | None = None

    error: str | None = Field(default=None, max_length=512)


class LastAppliedRecord(SQLModel, table=True):
    """
    Small UI-facing state: last applied look/sequence (metadata only).
    """

    __tablename__ = "last_applied"
    __table_args__ = (
        Index("ix_last_applied_agent_updated_at", "agent_id", "updated_at"),
    )

    agent_id: str = Field(primary_key=True, max_length=128)
    kind: str = Field(primary_key=True, max_length=32)  # 'look' or 'sequence'

    updated_at: float = Field(index=True)

    name: str | None = Field(default=None, max_length=256)
    file: str | None = Field(default=None, max_length=512)
    payload: Dict[str, Any] = Field(sa_column=Column(JSON), default_factory=dict)


class AgentHeartbeatRecord(SQLModel, table=True):
    """
    Fleet presence + capabilities snapshot for each agent.

    This is stored globally in the shared DB (not per-agent partitioned), keyed by agent_id.
    """

    __tablename__ = "agent_heartbeats"
    __table_args__ = (
        Index("ix_agent_heartbeats_role_updated_at", "role", "updated_at"),
    )

    agent_id: str = Field(primary_key=True, max_length=128)

    updated_at: float = Field(index=True)
    started_at: float = Field(default=0.0, index=True)

    name: str = Field(default="", max_length=256)
    role: str = Field(default="", max_length=128)
    controller_kind: str = Field(default="", max_length=32)
    version: str = Field(default="", max_length=64)

    payload: Dict[str, Any] = Field(sa_column=Column(JSON), default_factory=dict)


class AgentOverrideRecord(SQLModel, table=True):
    """
    Operator-managed overrides for fleet targeting (tags/role).
    """

    __tablename__ = "agent_overrides"
    __table_args__ = (
        Index("ix_agent_overrides_updated_at", "updated_at"),
    )

    agent_id: str = Field(primary_key=True, max_length=128)
    updated_at: float = Field(index=True)
    updated_by: str | None = Field(default=None, max_length=128)
    role: str | None = Field(default=None, max_length=128)
    tags: List[str] = Field(sa_column=Column(JSON), default_factory=list)


class AgentHeartbeatHistoryRecord(SQLModel, table=True):
    """
    Periodic snapshot history of agent heartbeats for fleet dashboards.
    """

    __tablename__ = "agent_heartbeat_history"
    __table_args__ = (
        Index(
            "ix_agent_heartbeat_history_agent_created_at", "agent_id", "created_at"
        ),
    )

    id: int | None = Field(default=None, primary_key=True)
    agent_id: str = Field(index=True, max_length=128)

    created_at: float = Field(index=True)
    updated_at: float = Field(index=True)

    name: str = Field(default="", max_length=256)
    role: str = Field(default="", max_length=128)
    controller_kind: str = Field(default="", max_length=32)
    version: str = Field(default="", max_length=64)
    base_url: str | None = Field(default=None, max_length=512)

    payload: Dict[str, Any] = Field(sa_column=Column(JSON), default_factory=dict)


class AgentHeartbeatHistoryTagRecord(SQLModel, table=True):
    """
    Normalized tags for agent heartbeat history (fast tag filtering).
    """

    __tablename__ = "agent_heartbeat_history_tags"
    __table_args__ = (
        Index("ix_agent_hb_tags_tag_created_at", "tag", "created_at"),
        Index("ix_agent_hb_tags_agent_created_at", "agent_id", "created_at"),
        Index("ix_agent_hb_tags_history_id", "history_id"),
    )

    id: int | None = Field(default=None, primary_key=True)
    history_id: int = Field()
    agent_id: str = Field(max_length=128)
    tag: str = Field(max_length=128)
    created_at: float = Field()


class LeaseRecord(SQLModel, table=True):
    """
    Small DB-backed lease/lock record used for fleet-wide leader election (scheduler).
    """

    __tablename__ = "leases"

    key: str = Field(primary_key=True, max_length=128)
    owner_id: str = Field(index=True, max_length=128)
    expires_at: float = Field(index=True)
    updated_at: float = Field(index=True)


class SchedulerEventRecord(SQLModel, table=True):
    """
    Scheduler action history (for debugging + UI visibility).
    """

    __tablename__ = "scheduler_events"
    __table_args__ = (
        Index("ix_scheduler_events_agent_created_at", "agent_id", "created_at"),
    )

    id: int | None = Field(default=None, primary_key=True)
    agent_id: str = Field(index=True, max_length=128)

    created_at: float = Field(index=True)
    action: str = Field(index=True, max_length=128)
    scope: str = Field(index=True, max_length=32)
    reason: str = Field(default="", max_length=64)

    ok: bool = Field(default=True, index=True)
    duration_s: float = Field(default=0.0)
    error: str | None = Field(default=None, max_length=512)

    payload: Dict[str, Any] = Field(sa_column=Column(JSON), default_factory=dict)


class AuditLogRecord(SQLModel, table=True):
    """
    Audit log for auth/admin actions (small, append-only).
    """

    __tablename__ = "audit_log"
    __table_args__ = (
        Index("ix_audit_log_agent_created_at", "agent_id", "created_at"),
        Index("ix_audit_log_action_created_at", "action", "created_at"),
    )

    id: int | None = Field(default=None, primary_key=True)
    agent_id: str = Field(index=True, max_length=128)

    created_at: float = Field(index=True)
    actor: str = Field(index=True, max_length=128)
    action: str = Field(index=True, max_length=128)
    resource: str | None = Field(default=None, max_length=256)

    ok: bool = Field(default=True, index=True)
    error: str | None = Field(default=None, max_length=512)

    ip: str | None = Field(default=None, max_length=64)
    user_agent: str | None = Field(default=None, max_length=256)
    request_id: str | None = Field(default=None, max_length=64)

    payload: Dict[str, Any] = Field(sa_column=Column(JSON), default_factory=dict)


class EventLogRecord(SQLModel, table=True):
    """
    Persisted SSE event history for diagnostics.
    """

    __tablename__ = "event_log"
    __table_args__ = (
        Index("ix_event_log_agent_created_at", "agent_id", "created_at"),
        Index("ix_event_log_type_created_at", "event_type", "created_at"),
        Index("ix_event_log_event_created_at", "event", "created_at"),
        Index("ix_event_log_created_id", "created_at", "id"),
    )

    id: int | None = Field(default=None, primary_key=True)
    agent_id: str = Field(index=True, max_length=128)

    created_at: float = Field(index=True)
    event_type: str = Field(index=True, max_length=64)
    event: str | None = Field(default=None, max_length=128)

    payload: Dict[str, Any] = Field(sa_column=Column(JSON), default_factory=dict)


class MetricsSampleRecord(SQLModel, table=True):
    """
    Time-series snapshots from /v1/metrics for UI charts.
    """

    __tablename__ = "metrics_samples"
    __table_args__ = (
        Index("ix_metrics_samples_agent_created_at", "agent_id", "created_at"),
    )

    id: int | None = Field(default=None, primary_key=True)
    agent_id: str = Field(index=True, max_length=128)
    created_at: float = Field(index=True)

    jobs_count: int = Field(default=0)
    scheduler_ok: bool = Field(default=True)
    scheduler_running: bool = Field(default=False)
    scheduler_in_window: bool = Field(default=False)

    outbound_failures: int = Field(default=0)
    outbound_retries: int = Field(default=0)

    spool_dropped: int = Field(default=0)
    spool_queued_events: int = Field(default=0)
    spool_queued_bytes: int = Field(default=0)


class AuthUserRecord(SQLModel, table=True):
    """
    Auth users stored in SQL for multi-user logins.
    """

    __tablename__ = "auth_users"
    __table_args__ = (
        Index("ix_auth_users_role", "role"),
    )

    username: str = Field(primary_key=True, max_length=64)
    password_hash: str = Field(max_length=256)
    totp_secret: str = Field(max_length=64)
    role: str = Field(default="user", max_length=32)
    disabled: bool = Field(default=False)
    ip_allowlist: List[str] = Field(sa_column=Column(JSON), default_factory=list)
    created_at: float = Field(index=True)
    updated_at: float = Field(index=True)
    last_login_at: float | None = Field(default=None, index=True)


class AuthSessionRecord(SQLModel, table=True):
    """
    JWT sessions with revocation support.
    """

    __tablename__ = "auth_sessions"
    __table_args__ = (
        Index("ix_auth_sessions_user_created_at", "username", "created_at"),
    )

    jti: str = Field(primary_key=True, max_length=64)
    username: str = Field(index=True, max_length=64)
    created_at: float = Field(index=True)
    expires_at: float = Field(index=True)
    revoked_at: float | None = Field(default=None, index=True)
    last_seen_at: float | None = Field(default=None, index=True)
    ip: str | None = Field(default=None, max_length=64)
    user_agent: str | None = Field(default=None, max_length=256)


class AuthLoginAttemptRecord(SQLModel, table=True):
    """
    Failed login tracking for rate limiting / lockout.
    """

    __tablename__ = "auth_login_attempts"
    username: str = Field(primary_key=True, max_length=64)
    ip: str = Field(primary_key=True, max_length=64)
    failed_count: int = Field(default=0)
    first_failed_at: float = Field(index=True)
    last_failed_at: float = Field(index=True)
    locked_until: float | None = Field(default=None, index=True)


class AuthApiKeyRecord(SQLModel, table=True):
    """
    Per-user API keys (hashed, with optional expiration).
    """

    __tablename__ = "auth_api_keys"
    __table_args__ = (
        Index("ix_auth_api_keys_key_hash", "key_hash", unique=True),
    )

    id: int | None = Field(default=None, primary_key=True)
    username: str = Field(index=True, max_length=64)
    label: str | None = Field(default=None, max_length=128)
    prefix: str | None = Field(default=None, max_length=16)
    key_hash: str = Field(max_length=128)
    created_at: float = Field(index=True)
    last_used_at: float | None = Field(default=None, index=True)
    revoked_at: float | None = Field(default=None, index=True)
    expires_at: float | None = Field(default=None, index=True)


class AuthPasswordResetRecord(SQLModel, table=True):
    """
    One-time password reset tokens (hashed).
    """

    __tablename__ = "auth_password_resets"
    __table_args__ = (
        Index("ix_auth_password_resets_token_hash", "token_hash", unique=True),
    )

    id: int | None = Field(default=None, primary_key=True)
    username: str = Field(index=True, max_length=64)
    token_hash: str = Field(max_length=128)
    created_at: float = Field(index=True)
    expires_at: float = Field(index=True)
    used_at: float | None = Field(default=None, index=True)
    created_by: str | None = Field(default=None, max_length=64)
    used_ip: str | None = Field(default=None, max_length=64)


class OrchestrationPresetRecord(SQLModel, table=True):
    """
    Saved orchestration presets (local or fleet payloads).
    """

    __tablename__ = "orchestration_presets"
    __table_args__ = (
        Index("ix_orchestration_presets_scope_name", "scope", "name", unique=True),
    )

    id: int | None = Field(default=None, primary_key=True)
    name: str = Field(max_length=128)
    scope: str = Field(default="local", max_length=32)
    description: str | None = Field(default=None, max_length=256)
    tags: List[str] = Field(sa_column=Column(JSON), default_factory=list)
    version: int = Field(default=1)
    created_at: float = Field(index=True)
    updated_at: float = Field(index=True)
    payload: Dict[str, Any] = Field(sa_column=Column(JSON), default_factory=dict)


class ReconcileRunRecord(SQLModel, table=True):
    """
    Reconcile run history (startup, scheduled, manual).
    """

    __tablename__ = "reconcile_runs"
    __table_args__ = (
        Index("ix_reconcile_runs_agent_started_at", "agent_id", "started_at"),
        Index("ix_reconcile_runs_status_started_at", "status", "started_at"),
    )

    id: int | None = Field(default=None, primary_key=True)
    agent_id: str = Field(index=True, max_length=128)
    started_at: float = Field(index=True)
    finished_at: float | None = Field(default=None, index=True)
    status: str = Field(index=True, max_length=32)
    source: str = Field(default="manual", max_length=32)
    error: str | None = Field(default=None, max_length=512)
    cancel_requested: bool = Field(default=False, index=True)
    options: Dict[str, Any] = Field(sa_column=Column(JSON), default_factory=dict)
    result: Dict[str, Any] = Field(sa_column=Column(JSON), default_factory=dict)


class OrchestrationRunRecord(SQLModel, table=True):
    """
    Orchestration run history (local + fleet).
    """

    __tablename__ = "orchestration_runs"
    __table_args__ = (
        Index("ix_orchestration_runs_agent_started_at", "agent_id", "started_at"),
        Index("ix_orchestration_runs_scope_started_at", "scope", "started_at"),
        Index("ix_orchestration_runs_status_started_at", "status", "started_at"),
    )

    run_id: str = Field(primary_key=True, max_length=64)
    agent_id: str = Field(index=True, max_length=128)

    created_at: float = Field(index=True)
    updated_at: float = Field(index=True)
    started_at: float = Field(index=True)
    finished_at: float | None = None

    name: str | None = Field(default=None, max_length=256)
    scope: str = Field(default="local", max_length=32)
    status: str = Field(default="running", max_length=32)

    steps_total: int = Field(default=0)
    loop: bool = Field(default=False)
    include_self: bool = Field(default=True)
    duration_s: float = Field(default=0.0)

    error: str | None = Field(default=None, max_length=512)
    payload: Dict[str, Any] = Field(sa_column=Column(JSON), default_factory=dict)


class OrchestrationStepRecord(SQLModel, table=True):
    """
    Per-step orchestration execution history (local + fleet).
    """

    __tablename__ = "orchestration_steps"
    __table_args__ = (
        Index("ix_orchestration_steps_run_created_at", "run_id", "created_at"),
        Index("ix_orchestration_steps_agent_created_at", "agent_id", "created_at"),
        Index("ix_orchestration_steps_status_created_at", "status", "created_at"),
    )

    id: int | None = Field(default=None, primary_key=True)
    run_id: str = Field(index=True, max_length=64)
    agent_id: str = Field(index=True, max_length=128)

    created_at: float = Field(index=True)
    updated_at: float = Field(index=True)
    started_at: float = Field(index=True)
    finished_at: float | None = None

    step_index: int = Field(index=True)
    iteration: int = Field(default=0, index=True)
    kind: str = Field(default="", max_length=32)
    status: str = Field(default="completed", max_length=32)
    ok: bool = Field(default=True, index=True)
    duration_s: float = Field(default=0.0)

    error: str | None = Field(default=None, max_length=512)
    payload: Dict[str, Any] = Field(sa_column=Column(JSON), default_factory=dict)


class OrchestrationPeerResultRecord(SQLModel, table=True):
    """
    Per-peer orchestration results (fleet runs).
    """

    __tablename__ = "orchestration_peer_results"
    __table_args__ = (
        Index("ix_orchestration_peer_run_created_at", "run_id", "created_at"),
        Index("ix_orchestration_peer_peer_created_at", "peer_id", "created_at"),
        Index("ix_orchestration_peer_status_created_at", "status", "created_at"),
    )

    id: int | None = Field(default=None, primary_key=True)
    run_id: str = Field(index=True, max_length=64)
    agent_id: str = Field(index=True, max_length=128)
    peer_id: str = Field(index=True, max_length=128)

    created_at: float = Field(index=True)
    updated_at: float = Field(index=True)
    started_at: float = Field(index=True)
    finished_at: float | None = None

    step_index: int = Field(index=True)
    iteration: int = Field(default=0, index=True)
    action: str = Field(default="", max_length=64)
    status: str = Field(default="completed", max_length=32)
    ok: bool = Field(default=True, index=True)
    duration_s: float = Field(default=0.0)

    error: str | None = Field(default=None, max_length=512)
    payload: Dict[str, Any] = Field(sa_column=Column(JSON), default_factory=dict)
