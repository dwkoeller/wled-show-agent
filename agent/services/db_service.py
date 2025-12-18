from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Sequence

from sqlalchemy import delete, func
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine
from sqlmodel import SQLModel, select
from sqlmodel.ext.asyncio.session import AsyncSession

from sql_store import (
    AudioAnalysisRecord,
    JobRecord,
    KVRecord,
    LastAppliedRecord,
    PackIngestRecord,
    SchemaVersion,
    SequenceMetaRecord,
)
from sql_store_async import normalize_database_url_async


LATEST_SCHEMA_VERSION = 1


def _now() -> float:
    return time.time()


@dataclass(frozen=True)
class DatabaseHealth:
    ok: bool
    detail: str


class DatabaseService:
    def __init__(self, *, database_url: str, agent_id: str, echo: bool = False) -> None:
        self.database_url = normalize_database_url_async(database_url)
        self.agent_id = str(agent_id)
        self.engine: AsyncEngine = create_async_engine(
            self.database_url,
            echo=bool(echo),
            pool_pre_ping=True,
        )
        self.sessionmaker: async_sessionmaker[AsyncSession] = async_sessionmaker(
            self.engine, class_=AsyncSession, expire_on_commit=False
        )

    async def init(self) -> None:
        async with self.engine.begin() as conn:
            await conn.run_sync(SQLModel.metadata.create_all)

        async with self.sessionmaker() as session:
            await self._ensure_schema_version(session)
            await session.commit()

    async def close(self) -> None:
        await self.engine.dispose()

    async def health(self) -> DatabaseHealth:
        try:
            async with self.sessionmaker() as session:
                res = await session.exec(
                    select(func.count()).select_from(SchemaVersion)
                )
                _ = res.one()
            return DatabaseHealth(ok=True, detail="ok")
        except Exception as e:
            return DatabaseHealth(ok=False, detail=str(e))

    async def _ensure_schema_version(self, session: AsyncSession) -> None:
        now = _now()
        rec = await session.get(SchemaVersion, 1)
        if rec is None:
            rec = SchemaVersion(id=1, version=LATEST_SCHEMA_VERSION, updated_at=now)
            session.add(rec)
            return
        if int(rec.version) != int(LATEST_SCHEMA_VERSION):
            rec.version = int(LATEST_SCHEMA_VERSION)
        rec.updated_at = now

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

        deleted_by_days = 0
        deleted_by_rows = 0

        async with self.sessionmaker() as session:
            if max_days_i > 0:
                cutoff = _now() - (max_days_i * 86400.0)
                stmt = delete(JobRecord).where(
                    JobRecord.agent_id == agent_id,
                    JobRecord.created_at < float(cutoff),
                )
                res = await session.exec(stmt)  # type: ignore[arg-type]
                deleted_by_days = int(getattr(res, "rowcount", 0) or 0)

            if max_rows_i > 0:
                # Find jobs beyond the newest N by created_at and delete them.
                stmt_ids = (
                    select(JobRecord.id)
                    .where(JobRecord.agent_id == agent_id)
                    .order_by(JobRecord.created_at.desc())
                    .offset(max_rows_i)
                )
                ids = (await session.exec(stmt_ids)).all()
                ids_list = [str(x) for x in ids if x]
                if ids_list:
                    stmt_del = delete(JobRecord).where(
                        JobRecord.agent_id == agent_id, JobRecord.id.in_(ids_list)
                    )
                    res = await session.exec(stmt_del)  # type: ignore[arg-type]
                    deleted_by_rows = int(getattr(res, "rowcount", 0) or 0)

            await session.commit()

        return {
            "ok": True,
            "deleted_by_days": deleted_by_days,
            "deleted_by_rows": deleted_by_rows,
        }

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
        async with self.sessionmaker() as session:
            rec = await session.get(PackIngestRecord, key)
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
            await session.commit()

    async def upsert_sequence_meta(
        self, *, file: str, duration_s: float, steps_total: int
    ) -> None:
        now = _now()
        key = (self.agent_id, str(file))
        async with self.sessionmaker() as session:
            rec = await session.get(SequenceMetaRecord, key)
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
            await session.commit()

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
        async with self.sessionmaker() as session:
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
            await session.commit()

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
        async with self.sessionmaker() as session:
            rec = await session.get(LastAppliedRecord, key)
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
            await session.commit()


class _AsyncToSyncBase:
    def __init__(
        self,
        *,
        loop: asyncio.AbstractEventLoop,
        timeout_s: float = 30.0,
    ) -> None:
        self._loop = loop
        self._timeout_s = float(timeout_s)

    def _run(self, coro):  # type: ignore[no-untyped-def]
        fut = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return fut.result(timeout=self._timeout_s)


class AsyncJobStoreSyncAdapter(_AsyncToSyncBase):
    def __init__(
        self,
        *,
        loop: asyncio.AbstractEventLoop,
        db: DatabaseService,
        timeout_s: float = 30.0,
    ) -> None:
        super().__init__(loop=loop, timeout_s=timeout_s)
        self._db = db

    def list_jobs(self, *, limit: int) -> list[dict[str, Any]]:
        async def _op() -> list[dict[str, Any]]:
            async with self._db.sessionmaker() as session:
                stmt = (
                    select(JobRecord)
                    .where(JobRecord.agent_id == self._db.agent_id)
                    .order_by(JobRecord.created_at.desc())
                    .limit(max(1, int(limit)))
                )
                rows = (await session.exec(stmt)).all()
                return [dict(r.payload or {}) for r in rows]

        return self._run(_op())

    def get_job(self, job_id: str) -> dict[str, Any] | None:
        async def _op() -> dict[str, Any] | None:
            async with self._db.sessionmaker() as session:
                rec = await session.get(JobRecord, (self._db.agent_id, str(job_id)))
                return dict(rec.payload or {}) if rec else None

        return self._run(_op())

    def upsert_job(self, job: dict[str, Any]) -> None:
        async def _op() -> None:
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
            finished_at = (
                float(finished_at_raw) if finished_at_raw is not None else None
            )

            async with self._db.sessionmaker() as session:
                rec = await session.get(JobRecord, (self._db.agent_id, jid))
                if rec is None:
                    rec = JobRecord(
                        agent_id=self._db.agent_id,
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
                await session.commit()

        self._run(_op())

    def mark_in_flight_failed(self, *, reason: str = "Server restarted") -> int:
        async def _op() -> int:
            now = _now()
            updated = 0
            async with self._db.sessionmaker() as session:
                stmt = select(JobRecord).where(
                    JobRecord.agent_id == self._db.agent_id,
                    JobRecord.status.in_(("queued", "running")),
                )
                recs = (await session.exec(stmt)).all()
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
                await session.commit()
            return updated

        return int(self._run(_op()))


class AsyncKVStoreSyncAdapter(_AsyncToSyncBase):
    def __init__(
        self,
        *,
        loop: asyncio.AbstractEventLoop,
        db: DatabaseService,
        timeout_s: float = 30.0,
    ) -> None:
        super().__init__(loop=loop, timeout_s=timeout_s)
        self._db = db

    def get_json(self, key: str) -> dict[str, Any] | None:
        async def _op() -> dict[str, Any] | None:
            async with self._db.sessionmaker() as session:
                rec = await session.get(KVRecord, (self._db.agent_id, str(key)))
                return dict(rec.value or {}) if rec else None

        return self._run(_op())

    def set_json(self, key: str, value: dict[str, Any]) -> None:
        async def _op() -> None:
            now = _now()
            k = str(key)
            async with self._db.sessionmaker() as session:
                rec = await session.get(KVRecord, (self._db.agent_id, k))
                if rec is None:
                    rec = KVRecord(
                        agent_id=self._db.agent_id,
                        key=k,
                        updated_at=now,
                        value=dict(value or {}),
                    )
                    session.add(rec)
                else:
                    rec.updated_at = now
                    rec.value = dict(value or {})
                await session.commit()

        self._run(_op())
