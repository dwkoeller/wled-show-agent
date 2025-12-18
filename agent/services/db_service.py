from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Sequence

from sqlalchemy import delete, func
from sqlalchemy.exc import IntegrityError
from sqlmodel import Session, SQLModel, select

from sql_store import (
    AudioAnalysisRecord,
    AgentHeartbeatRecord,
    GlobalKVRecord,
    JobRecord,
    KVRecord,
    LastAppliedRecord,
    LeaseRecord,
    PackIngestRecord,
    SchemaVersion,
    SchedulerEventRecord,
    SequenceMetaRecord,
    create_db_engine,
)


LATEST_SCHEMA_VERSION = 1


def _now() -> float:
    return time.time()


@dataclass(frozen=True)
class DatabaseHealth:
    ok: bool
    detail: str


class DatabaseService:
    """
    SQLModel-based DB service with an async API.

    Implementation note:
    - Uses a *sync* SQLAlchemy engine (pymysql/sqlite) and runs blocking DB work in
      `asyncio.to_thread(...)` so FastAPI async endpoints don't block the event loop.
    - This avoids SQLAlchemy AsyncEngine's dependency on `greenlet` (which may not be
      available on all Python versions).
    """

    def __init__(self, *, database_url: str, agent_id: str, echo: bool = False) -> None:
        self.database_url = str(database_url).strip()
        self.agent_id = str(agent_id)
        self.engine = create_db_engine(self.database_url, echo=bool(echo))

    async def init(self) -> None:
        # In fleet mode, multiple agents may start concurrently and attempt create_all.
        # Retry once to tolerate "already exists" races.
        for attempt in range(2):
            try:
                await asyncio.to_thread(SQLModel.metadata.create_all, self.engine)
                break
            except Exception:
                if attempt >= 1:
                    raise
                await asyncio.sleep(0.25)

        for attempt in range(2):
            try:
                await asyncio.to_thread(self._ensure_schema_version_sync)
                break
            except IntegrityError:
                if attempt >= 1:
                    break
                await asyncio.sleep(0.05)

    def _ensure_schema_version_sync(self) -> None:
        now = _now()
        with Session(self.engine) as session:
            rec = session.get(SchemaVersion, 1)
            if rec is None:
                rec = SchemaVersion(id=1, version=LATEST_SCHEMA_VERSION, updated_at=now)
                session.add(rec)
            else:
                if int(rec.version) != int(LATEST_SCHEMA_VERSION):
                    rec.version = int(LATEST_SCHEMA_VERSION)
                rec.updated_at = now
            session.commit()

    async def close(self) -> None:
        try:
            await asyncio.to_thread(self.engine.dispose)
        except Exception:
            try:
                self.engine.dispose()
            except Exception:
                pass

    async def health(self) -> DatabaseHealth:
        def _op() -> None:
            with Session(self.engine) as session:
                res = session.exec(select(func.count()).select_from(SchemaVersion))
                _ = res.one()

        try:
            await asyncio.to_thread(_op)
            return DatabaseHealth(ok=True, detail="ok")
        except Exception as e:
            return DatabaseHealth(ok=False, detail=str(e))

    # ---- Fleet presence ----

    async def upsert_agent_heartbeat(
        self,
        *,
        agent_id: str,
        started_at: float,
        name: str,
        role: str,
        controller_kind: str,
        version: str,
        payload: Dict[str, Any] | None = None,
    ) -> None:
        aid = str(agent_id).strip()
        if not aid:
            return

        def _op() -> None:
            now = _now()
            with Session(self.engine) as session:
                rec = session.get(AgentHeartbeatRecord, aid)
                if rec is None:
                    rec = AgentHeartbeatRecord(
                        agent_id=aid,
                        updated_at=now,
                        started_at=float(started_at or 0.0),
                        name=str(name or ""),
                        role=str(role or ""),
                        controller_kind=str(controller_kind or ""),
                        version=str(version or ""),
                        payload=dict(payload or {}),
                    )
                    session.add(rec)
                else:
                    rec.updated_at = now
                    rec.started_at = float(started_at or rec.started_at or 0.0)
                    rec.name = str(name or rec.name or "")
                    rec.role = str(role or rec.role or "")
                    rec.controller_kind = str(
                        controller_kind or rec.controller_kind or ""
                    )
                    rec.version = str(version or rec.version or "")
                    rec.payload = dict(payload or {})
                session.commit()

        await asyncio.to_thread(_op)

    async def list_agent_heartbeats(self, *, limit: int = 200) -> list[dict[str, Any]]:
        lim = max(1, int(limit))

        def _op() -> list[dict[str, Any]]:
            with Session(self.engine) as session:
                stmt = select(AgentHeartbeatRecord).order_by(
                    AgentHeartbeatRecord.updated_at.desc()
                )
                stmt = stmt.limit(lim)
                rows = session.exec(stmt).all()
                out: list[dict[str, Any]] = []
                for r in rows:
                    out.append(
                        {
                            "agent_id": r.agent_id,
                            "updated_at": float(r.updated_at),
                            "started_at": float(r.started_at or 0.0),
                            "name": str(r.name or ""),
                            "role": str(r.role or ""),
                            "controller_kind": str(r.controller_kind or ""),
                            "version": str(r.version or ""),
                            "payload": dict(r.payload or {}),
                        }
                    )
                return out

        return await asyncio.to_thread(_op)

    async def get_agent_heartbeat(self, *, agent_id: str) -> dict[str, Any] | None:
        aid = str(agent_id).strip()
        if not aid:
            return None

        def _op() -> dict[str, Any] | None:
            with Session(self.engine) as session:
                r = session.get(AgentHeartbeatRecord, aid)
                if r is None:
                    return None
                return {
                    "agent_id": r.agent_id,
                    "updated_at": float(r.updated_at),
                    "started_at": float(r.started_at or 0.0),
                    "name": str(r.name or ""),
                    "role": str(r.role or ""),
                    "controller_kind": str(r.controller_kind or ""),
                    "version": str(r.version or ""),
                    "payload": dict(r.payload or {}),
                }

        return await asyncio.to_thread(_op)

    # ---- DB leases / locks ----

    async def get_lease(self, key: str) -> dict[str, Any] | None:
        k = str(key).strip()
        if not k:
            return None

        def _op() -> dict[str, Any] | None:
            with Session(self.engine) as session:
                rec = session.get(LeaseRecord, k)
                if rec is None:
                    return None
                return {
                    "key": rec.key,
                    "owner_id": rec.owner_id,
                    "expires_at": float(rec.expires_at),
                    "updated_at": float(rec.updated_at),
                }

        return await asyncio.to_thread(_op)

    async def try_acquire_lease(
        self,
        *,
        key: str,
        owner_id: str,
        ttl_s: float = 15.0,
    ) -> bool:
        """
        Acquire or renew a small DB-backed lease (best-effort, cross-agent).

        Returns True if the lease is owned by `owner_id` after the call.
        """
        k = str(key).strip()
        owner = str(owner_id).strip()
        if not k or not owner:
            return False

        ttl = max(1.0, float(ttl_s))

        def _op() -> bool:
            now = _now()
            with Session(self.engine) as session:
                stmt = select(LeaseRecord).where(LeaseRecord.key == k).with_for_update()
                rec = session.exec(stmt).one_or_none()
                if rec is None:
                    session.add(
                        LeaseRecord(
                            key=k,
                            owner_id=owner,
                            expires_at=now + ttl,
                            updated_at=now,
                        )
                    )
                    session.commit()
                    return True

                if str(rec.owner_id) == owner or float(rec.expires_at or 0.0) < now:
                    rec.owner_id = owner
                    rec.expires_at = now + ttl
                    rec.updated_at = now
                    session.commit()
                    return True

                return False

        for attempt in range(2):
            try:
                return await asyncio.to_thread(_op)
            except IntegrityError:
                if attempt >= 1:
                    return False
                await asyncio.sleep(0.05)
            except Exception:
                return False
        return False

    # ---- Scheduler events ----

    async def add_scheduler_event(
        self,
        *,
        agent_id: str,
        action: str,
        scope: str,
        reason: str,
        ok: bool,
        duration_s: float,
        error: str | None = None,
        payload: Dict[str, Any] | None = None,
    ) -> None:
        aid = str(agent_id).strip()
        act = str(action).strip()
        if not aid or not act:
            return

        def _op() -> None:
            now = _now()
            with Session(self.engine) as session:
                rec = SchedulerEventRecord(
                    agent_id=aid,
                    created_at=now,
                    action=act,
                    scope=str(scope or ""),
                    reason=str(reason or ""),
                    ok=bool(ok),
                    duration_s=max(0.0, float(duration_s)),
                    error=str(error)[:512] if error else None,
                    payload=dict(payload or {}),
                )
                session.add(rec)
                session.commit()

        await asyncio.to_thread(_op)

    async def list_scheduler_events(
        self,
        *,
        limit: int = 200,
        agent_id: str | None = None,
    ) -> list[dict[str, Any]]:
        lim = max(1, int(limit))
        aid = str(agent_id).strip() if agent_id else ""

        def _op() -> list[dict[str, Any]]:
            with Session(self.engine) as session:
                stmt = select(SchedulerEventRecord).order_by(
                    SchedulerEventRecord.created_at.desc()
                )
                if aid:
                    stmt = stmt.where(SchedulerEventRecord.agent_id == aid)
                stmt = stmt.limit(lim)
                rows = session.exec(stmt).all()
                out: list[dict[str, Any]] = []
                for r in rows:
                    out.append(
                        {
                            "id": int(r.id) if r.id is not None else None,
                            "agent_id": str(r.agent_id),
                            "created_at": float(r.created_at),
                            "action": str(r.action),
                            "scope": str(r.scope),
                            "reason": str(r.reason),
                            "ok": bool(r.ok),
                            "duration_s": float(r.duration_s or 0.0),
                            "error": str(r.error) if r.error else None,
                            "payload": dict(r.payload or {}),
                        }
                    )
                return out

        return await asyncio.to_thread(_op)

    async def enforce_scheduler_events_retention(
        self,
        *,
        max_rows: Optional[int],
        max_days: Optional[int],
        batch_size: int = 1000,
    ) -> Dict[str, Any]:
        """
        Best-effort retention for scheduler action history (global).
        """
        max_rows_i = int(max_rows) if max_rows is not None else 0
        max_days_i = int(max_days) if max_days is not None else 0
        batch = max(1, min(10_000, int(batch_size)))

        def _op() -> Dict[str, Any]:
            deleted_by_days = 0
            deleted_by_rows = 0

            with Session(self.engine) as session:
                if max_days_i > 0:
                    cutoff = _now() - (max_days_i * 86400.0)
                    stmt = delete(SchedulerEventRecord).where(
                        SchedulerEventRecord.created_at < float(cutoff)
                    )
                    res = session.exec(stmt)  # type: ignore[arg-type]
                    deleted_by_days = int(getattr(res, "rowcount", 0) or 0)

                if max_rows_i > 0:
                    res = session.exec(
                        select(func.count()).select_from(SchedulerEventRecord)
                    )
                    total = int(res.one() or 0)
                    excess = total - max_rows_i
                    if excess > 0:
                        limit = min(excess, batch)
                        ids = session.exec(
                            select(SchedulerEventRecord.id)
                            .order_by(SchedulerEventRecord.created_at.asc())
                            .limit(limit)
                        ).all()
                        ids_list = [int(x) for x in ids if x is not None]
                        if ids_list:
                            stmt_del = delete(SchedulerEventRecord).where(
                                SchedulerEventRecord.id.in_(ids_list)
                            )
                            res = session.exec(stmt_del)  # type: ignore[arg-type]
                            deleted_by_rows = int(getattr(res, "rowcount", 0) or 0)

                session.commit()

            return {
                "ok": True,
                "deleted_by_days": deleted_by_days,
                "deleted_by_rows": deleted_by_rows,
                "max_days": max_days_i,
                "max_rows": max_rows_i,
            }

        return await asyncio.to_thread(_op)

    # ---- Jobs persistence ----

    async def list_jobs(self, *, limit: int) -> list[dict[str, Any]]:
        lim = max(1, int(limit))

        def _op() -> list[dict[str, Any]]:
            with Session(self.engine) as session:
                stmt = (
                    select(JobRecord)
                    .where(JobRecord.agent_id == self.agent_id)
                    .order_by(JobRecord.created_at.desc())
                    .limit(lim)
                )
                rows = session.exec(stmt).all()
                return [dict(r.payload or {}) for r in rows]

        return await asyncio.to_thread(_op)

    async def get_job(self, job_id: str) -> dict[str, Any] | None:
        jid = str(job_id)

        def _op() -> dict[str, Any] | None:
            with Session(self.engine) as session:
                rec = session.get(JobRecord, (self.agent_id, jid))
                return dict(rec.payload or {}) if rec else None

        return await asyncio.to_thread(_op)

    async def upsert_job(self, job: dict[str, Any]) -> None:
        now = _now()
        jid = str(job.get("id") or "").strip()
        if not jid:
            return

        kind = str(job.get("kind") or "")
        status = str(job.get("status") or "")
        created_at = float(job.get("created_at") or now)

        started_at_raw = job.get("started_at")
        started_at = float(started_at_raw) if started_at_raw is not None else None
        finished_at_raw = job.get("finished_at")
        finished_at = float(finished_at_raw) if finished_at_raw is not None else None

        def _op() -> None:
            with Session(self.engine) as session:
                rec = session.get(JobRecord, (self.agent_id, jid))
                if rec is None:
                    rec = JobRecord(
                        agent_id=self.agent_id,
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

        await asyncio.to_thread(_op)

    async def mark_in_flight_failed(self, *, reason: str = "Server restarted") -> int:
        now = _now()

        def _op() -> int:
            updated = 0
            with Session(self.engine) as session:
                stmt = select(JobRecord).where(
                    JobRecord.agent_id == self.agent_id,
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

        return await asyncio.to_thread(_op)

    # ---- KV persistence ----

    async def kv_get_json(self, key: str) -> dict[str, Any] | None:
        k = str(key)

        def _op() -> dict[str, Any] | None:
            with Session(self.engine) as session:
                rec = session.get(KVRecord, (self.agent_id, k))
                return dict(rec.value or {}) if rec else None

        return await asyncio.to_thread(_op)

    async def global_kv_get_json(self, key: str) -> dict[str, Any] | None:
        k = str(key)

        def _op() -> dict[str, Any] | None:
            with Session(self.engine) as session:
                rec = session.get(GlobalKVRecord, k)
                return dict(rec.value or {}) if rec else None

        return await asyncio.to_thread(_op)

    async def kv_set_json(self, key: str, value: dict[str, Any]) -> None:
        now = _now()
        k = str(key)

        def _op() -> None:
            with Session(self.engine) as session:
                rec = session.get(KVRecord, (self.agent_id, k))
                if rec is None:
                    rec = KVRecord(
                        agent_id=self.agent_id,
                        key=k,
                        updated_at=now,
                        value=dict(value or {}),
                    )
                    session.add(rec)
                else:
                    rec.updated_at = now
                    rec.value = dict(value or {})
                session.commit()

        await asyncio.to_thread(_op)

    async def global_kv_set_json(self, key: str, value: dict[str, Any]) -> None:
        now = _now()
        k = str(key)

        def _op() -> None:
            with Session(self.engine) as session:
                rec = session.get(GlobalKVRecord, k)
                if rec is None:
                    rec = GlobalKVRecord(
                        key=k,
                        updated_at=now,
                        value=dict(value or {}),
                    )
                    session.add(rec)
                else:
                    rec.updated_at = now
                    rec.value = dict(value or {})
                session.commit()

        await asyncio.to_thread(_op)

    # ---- Jobs retention ----

    async def enforce_job_retention(
        self,
        *,
        max_rows: Optional[int],
        max_days: Optional[int],
    ) -> Dict[str, Any]:
        """
        Best-effort retention for this agent_id.
        """
        agent_id = self.agent_id
        max_rows_i = int(max_rows) if max_rows is not None else 0
        max_days_i = int(max_days) if max_days is not None else 0

        def _op() -> Dict[str, Any]:
            deleted_by_days = 0
            deleted_by_rows = 0

            with Session(self.engine) as session:
                if max_days_i > 0:
                    cutoff = _now() - (max_days_i * 86400.0)
                    stmt = delete(JobRecord).where(
                        JobRecord.agent_id == agent_id,
                        JobRecord.created_at < float(cutoff),
                    )
                    res = session.exec(stmt)  # type: ignore[arg-type]
                    deleted_by_days = int(getattr(res, "rowcount", 0) or 0)

                if max_rows_i > 0:
                    stmt_ids = (
                        select(JobRecord.id)
                        .where(JobRecord.agent_id == agent_id)
                        .order_by(JobRecord.created_at.desc())
                        .offset(max_rows_i)
                    )
                    ids = session.exec(stmt_ids).all()
                    ids_list = [str(x) for x in ids if x]
                    if ids_list:
                        stmt_del = delete(JobRecord).where(
                            JobRecord.agent_id == agent_id, JobRecord.id.in_(ids_list)
                        )
                        res = session.exec(stmt_del)  # type: ignore[arg-type]
                        deleted_by_rows = int(getattr(res, "rowcount", 0) or 0)

                session.commit()

            return {
                "ok": True,
                "deleted_by_days": deleted_by_days,
                "deleted_by_rows": deleted_by_rows,
            }

        return await asyncio.to_thread(_op)

    # ---- Metadata upserts ----

    async def upsert_pack_ingest(
        self,
        *,
        dest_dir: str,
        source_name: str | None,
        manifest_path: str | None,
        uploaded_bytes: int,
        unpacked_bytes: int,
        file_count: int,
    ) -> None:
        now = _now()
        key = (self.agent_id, str(dest_dir))

        def _op() -> None:
            with Session(self.engine) as session:
                rec = session.get(PackIngestRecord, key)
                if rec is None:
                    rec = PackIngestRecord(
                        agent_id=self.agent_id,
                        dest_dir=str(dest_dir),
                        created_at=now,
                        updated_at=now,
                        source_name=str(source_name) if source_name else None,
                        manifest_path=str(manifest_path) if manifest_path else None,
                        uploaded_bytes=int(uploaded_bytes),
                        unpacked_bytes=int(unpacked_bytes),
                        file_count=int(file_count),
                    )
                    session.add(rec)
                else:
                    rec.updated_at = now
                    rec.source_name = str(source_name) if source_name else None
                    rec.manifest_path = str(manifest_path) if manifest_path else None
                    rec.uploaded_bytes = int(uploaded_bytes)
                    rec.unpacked_bytes = int(unpacked_bytes)
                    rec.file_count = int(file_count)
                session.commit()

        await asyncio.to_thread(_op)

    async def upsert_sequence_meta(
        self, *, file: str, duration_s: float, steps_total: int
    ) -> None:
        now = _now()
        key = (self.agent_id, str(file))

        def _op() -> None:
            with Session(self.engine) as session:
                rec = session.get(SequenceMetaRecord, key)
                if rec is None:
                    rec = SequenceMetaRecord(
                        agent_id=self.agent_id,
                        file=str(file),
                        created_at=now,
                        updated_at=now,
                        duration_s=float(duration_s),
                        steps_total=int(steps_total),
                    )
                    session.add(rec)
                else:
                    rec.updated_at = now
                    rec.duration_s = float(duration_s)
                    rec.steps_total = int(steps_total)
                session.commit()

        await asyncio.to_thread(_op)

    async def add_audio_analysis(
        self,
        *,
        analysis_id: str,
        source_path: str | None,
        beats_path: str | None,
        prefer_ffmpeg: bool,
        bpm: float | None,
        beat_count: int | None,
        error: str | None,
    ) -> None:
        now = _now()

        def _op() -> None:
            with Session(self.engine) as session:
                rec = AudioAnalysisRecord(
                    agent_id=self.agent_id,
                    id=str(analysis_id),
                    created_at=now,
                    updated_at=now,
                    source_path=str(source_path) if source_path else None,
                    beats_path=str(beats_path) if beats_path else None,
                    prefer_ffmpeg=bool(prefer_ffmpeg),
                    bpm=float(bpm) if bpm is not None else None,
                    beat_count=int(beat_count) if beat_count is not None else None,
                    error=str(error) if error else None,
                )
                session.add(rec)
                session.commit()

        await asyncio.to_thread(_op)

    async def set_last_applied(
        self,
        *,
        kind: str,
        name: str | None,
        file: str | None,
        payload: Dict[str, Any] | None = None,
    ) -> None:
        now = _now()
        k = str(kind).strip().lower()
        key = (self.agent_id, k)

        def _op() -> None:
            with Session(self.engine) as session:
                rec = session.get(LastAppliedRecord, key)
                if rec is None:
                    rec = LastAppliedRecord(
                        agent_id=self.agent_id,
                        kind=k,
                        updated_at=now,
                        name=str(name) if name else None,
                        file=str(file) if file else None,
                        payload=dict(payload or {}),
                    )
                    session.add(rec)
                else:
                    rec.updated_at = now
                    rec.name = str(name) if name else None
                    rec.file = str(file) if file else None
                    rec.payload = dict(payload or {})
                session.commit()

        await asyncio.to_thread(_op)

    # ---- SQL-backed metadata listing (UI) ----

    async def list_pack_ingests(self, *, limit: int = 200) -> list[dict[str, Any]]:
        lim = max(1, int(limit))

        def _op() -> list[dict[str, Any]]:
            with Session(self.engine) as session:
                stmt = (
                    select(PackIngestRecord)
                    .where(PackIngestRecord.agent_id == self.agent_id)
                    .order_by(PackIngestRecord.updated_at.desc())
                    .limit(lim)
                )
                rows = session.exec(stmt).all()
                return [r.model_dump() for r in rows]

        return await asyncio.to_thread(_op)

    async def list_sequence_meta(self, *, limit: int = 500) -> list[dict[str, Any]]:
        lim = max(1, int(limit))

        def _op() -> list[dict[str, Any]]:
            with Session(self.engine) as session:
                stmt = (
                    select(SequenceMetaRecord)
                    .where(SequenceMetaRecord.agent_id == self.agent_id)
                    .order_by(SequenceMetaRecord.updated_at.desc())
                    .limit(lim)
                )
                rows = session.exec(stmt).all()
                return [r.model_dump() for r in rows]

        return await asyncio.to_thread(_op)

    async def list_audio_analyses(self, *, limit: int = 200) -> list[dict[str, Any]]:
        lim = max(1, int(limit))

        def _op() -> list[dict[str, Any]]:
            with Session(self.engine) as session:
                stmt = (
                    select(AudioAnalysisRecord)
                    .where(AudioAnalysisRecord.agent_id == self.agent_id)
                    .order_by(AudioAnalysisRecord.created_at.desc())
                    .limit(lim)
                )
                rows = session.exec(stmt).all()
                return [r.model_dump() for r in rows]

        return await asyncio.to_thread(_op)

    async def list_last_applied(self) -> list[dict[str, Any]]:
        def _op() -> list[dict[str, Any]]:
            with Session(self.engine) as session:
                stmt = (
                    select(LastAppliedRecord)
                    .where(LastAppliedRecord.agent_id == self.agent_id)
                    .order_by(LastAppliedRecord.updated_at.desc())
                )
                rows = session.exec(stmt).all()
                return [r.model_dump() for r in rows]

        return await asyncio.to_thread(_op)
