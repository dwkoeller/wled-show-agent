from __future__ import annotations

import time
from typing import Any, Dict, List, Optional

from sqlalchemy import Column, Index
from sqlalchemy.types import JSON
from sqlmodel import Field, Session, SQLModel, create_engine, select


def normalize_database_url(raw: str) -> str:
    """
    Normalize common DB URL variants to SQLAlchemy-compatible URLs.

    - mysql://... -> mysql+pymysql://...
    """
    url = str(raw or "").strip()
    if not url:
        raise ValueError("Empty database URL")
    if url.startswith("mysql://"):
        return "mysql+pymysql://" + url[len("mysql://") :]
    return url


def create_db_engine(database_url: str, *, echo: bool = False):  # type: ignore[no-untyped-def]
    url = normalize_database_url(database_url)
    connect_args: Dict[str, Any] = {}
    if url.startswith("sqlite:"):
        connect_args["check_same_thread"] = False
    return create_engine(
        url,
        echo=bool(echo),
        pool_pre_ping=True,
        connect_args=connect_args,
    )


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


def init_db(engine) -> None:  # type: ignore[no-untyped-def]
    SQLModel.metadata.create_all(engine)


class SQLJobStore:
    def __init__(self, *, engine, agent_id: str) -> None:  # type: ignore[no-untyped-def]
        self._engine = engine
        self._agent_id = str(agent_id)

    def list_jobs(self, *, limit: int) -> List[Dict[str, Any]]:
        lim = max(1, int(limit))
        with Session(self._engine) as session:
            stmt = (
                select(JobRecord)
                .where(JobRecord.agent_id == self._agent_id)
                .order_by(JobRecord.created_at.desc())
                .limit(lim)
            )
            rows = session.exec(stmt).all()
            return [dict(r.payload or {}) for r in rows]

    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        jid = str(job_id)
        with Session(self._engine) as session:
            rec = session.get(JobRecord, (self._agent_id, jid))
            if not rec:
                return None
            return dict(rec.payload or {})

    def upsert_job(self, job: Dict[str, Any]) -> None:
        if not isinstance(job, dict):
            raise ValueError("job must be a dict")
        jid = str(job.get("id") or "").strip()
        if not jid:
            raise ValueError("job.id is required")

        now = time.time()
        kind = str(job.get("kind") or "")
        status = str(job.get("status") or "")
        created_at = float(job.get("created_at") or now)

        started_at_raw = job.get("started_at")
        started_at = float(started_at_raw) if started_at_raw is not None else None
        finished_at_raw = job.get("finished_at")
        finished_at = float(finished_at_raw) if finished_at_raw is not None else None

        with Session(self._engine) as session:
            rec = session.get(JobRecord, (self._agent_id, jid))
            if not rec:
                rec = JobRecord(
                    agent_id=self._agent_id,
                    id=jid,
                    kind=kind,
                    status=status,
                    created_at=created_at,
                    started_at=started_at,
                    finished_at=finished_at,
                    updated_at=now,
                    payload=dict(job),
                )
                session.add(rec)
            else:
                rec.kind = kind
                rec.status = status
                rec.created_at = created_at
                rec.started_at = started_at
                rec.finished_at = finished_at
                rec.updated_at = now
                rec.payload = dict(job)
            session.commit()

    def mark_in_flight_failed(self, *, reason: str = "Server restarted") -> int:
        now = time.time()
        updated = 0
        with Session(self._engine) as session:
            stmt = select(JobRecord).where(
                JobRecord.agent_id == self._agent_id,
                JobRecord.status.in_(("queued", "running")),
            )
            recs = session.exec(stmt).all()
            for rec in recs:
                payload = dict(rec.payload or {})
                payload["status"] = "failed"
                payload["finished_at"] = payload.get("finished_at") or now
                payload["error"] = payload.get("error") or str(reason)
                rec.status = "failed"
                rec.finished_at = float(payload["finished_at"] or now)
                rec.updated_at = now
                rec.payload = payload
                updated += 1
            session.commit()
        return updated


class SQLKVStore:
    def __init__(self, *, engine, agent_id: str) -> None:  # type: ignore[no-untyped-def]
        self._engine = engine
        self._agent_id = str(agent_id)

    def get_json(self, key: str) -> Optional[Dict[str, Any]]:
        k = str(key)
        with Session(self._engine) as session:
            rec = session.get(KVRecord, (self._agent_id, k))
            if not rec:
                return None
            return dict(rec.value or {})

    def set_json(self, key: str, value: Dict[str, Any]) -> None:
        if not isinstance(value, dict):
            raise ValueError("value must be a dict")
        now = time.time()
        k = str(key)
        with Session(self._engine) as session:
            rec = session.get(KVRecord, (self._agent_id, k))
            if not rec:
                rec = KVRecord(
                    agent_id=self._agent_id,
                    key=k,
                    updated_at=now,
                    value=dict(value),
                )
                session.add(rec)
            else:
                rec.updated_at = now
                rec.value = dict(value)
            session.commit()
