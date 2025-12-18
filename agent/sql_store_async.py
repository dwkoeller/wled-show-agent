from __future__ import annotations

import asyncio
import threading
import time
from typing import Any, Dict, List, Optional

from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlmodel import SQLModel, select
from sqlmodel.ext.asyncio.session import AsyncSession

from sql_store import JobRecord, KVRecord


def normalize_database_url_async(raw: str) -> str:
    """
    Normalize common DB URL variants to SQLAlchemy async-compatible URLs.

    - mysql://... -> mysql+aiomysql://...
    - mysql+pymysql://... -> mysql+aiomysql://...
    - sqlite://... -> sqlite+aiosqlite://...
    """
    url = str(raw or "").strip()
    if not url:
        raise ValueError("Empty database URL")
    if url.startswith("mysql://"):
        return "mysql+aiomysql://" + url[len("mysql://") :]
    if url.startswith("mysql+pymysql://"):
        return "mysql+aiomysql://" + url[len("mysql+pymysql://") :]
    if url.startswith("sqlite://") and not url.startswith("sqlite+aiosqlite://"):
        return "sqlite+aiosqlite://" + url[len("sqlite://") :]
    return url


def create_async_db_engine(database_url: str, *, echo: bool = False) -> AsyncEngine:
    url = normalize_database_url_async(database_url)
    return create_async_engine(
        url,
        echo=bool(echo),
        pool_pre_ping=True,
    )


async def init_async_db(engine: AsyncEngine) -> None:
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)


class AsyncSQLJobStore:
    def __init__(self, *, engine: AsyncEngine, agent_id: str) -> None:
        self._engine = engine
        self._agent_id = str(agent_id)

    async def list_jobs(self, *, limit: int) -> List[Dict[str, Any]]:
        lim = max(1, int(limit))
        async with AsyncSession(self._engine) as session:
            stmt = (
                select(JobRecord)
                .where(JobRecord.agent_id == self._agent_id)
                .order_by(JobRecord.created_at.desc())
                .limit(lim)
            )
            rows = (await session.exec(stmt)).all()
            return [dict(r.payload or {}) for r in rows]

    async def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        jid = str(job_id)
        async with AsyncSession(self._engine) as session:
            rec = await session.get(JobRecord, (self._agent_id, jid))
            if not rec:
                return None
            return dict(rec.payload or {})

    async def upsert_job(self, job: Dict[str, Any]) -> None:
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

        async with AsyncSession(self._engine) as session:
            rec = await session.get(JobRecord, (self._agent_id, jid))
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
            await session.commit()

    async def mark_in_flight_failed(self, *, reason: str = "Server restarted") -> int:
        now = time.time()
        updated = 0
        async with AsyncSession(self._engine) as session:
            stmt = select(JobRecord).where(
                JobRecord.agent_id == self._agent_id,
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


class AsyncSQLKVStore:
    def __init__(self, *, engine: AsyncEngine, agent_id: str) -> None:
        self._engine = engine
        self._agent_id = str(agent_id)

    async def get_json(self, key: str) -> Optional[Dict[str, Any]]:
        k = str(key)
        async with AsyncSession(self._engine) as session:
            rec = await session.get(KVRecord, (self._agent_id, k))
            if not rec:
                return None
            return dict(rec.value or {})

    async def set_json(self, key: str, value: Dict[str, Any]) -> None:
        if not isinstance(value, dict):
            raise ValueError("value must be a dict")
        now = time.time()
        k = str(key)
        async with AsyncSession(self._engine) as session:
            rec = await session.get(KVRecord, (self._agent_id, k))
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
            await session.commit()


class AsyncSQLDatabase:
    """
    Runs SQLModel async DB access on a dedicated event-loop thread.

    This keeps the rest of the app (which is still largely sync/threaded) compatible
    while using an async DB driver like aiomysql/aiosqlite underneath.
    """

    def __init__(
        self,
        *,
        database_url: str,
        agent_id: str,
        echo: bool = False,
        op_timeout_s: float = 30.0,
    ) -> None:
        self._database_url = str(database_url)
        self._agent_id = str(agent_id)
        self._echo = bool(echo)
        self._op_timeout_s = float(op_timeout_s)

        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(
            target=self._run_loop, name="sql_async_db", daemon=True
        )
        self._loop_ready = threading.Event()

        self._engine: AsyncEngine | None = None
        self._job_store: AsyncSQLJobStore | None = None
        self._kv_store: AsyncSQLKVStore | None = None

        self._thread.start()
        self._loop_ready.wait(timeout=5.0)

        self._run(self._init())

        self.job_store = _SyncJobStore(self)
        self.kv_store = _SyncKVStore(self)

    def _run_loop(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._loop_ready.set()
        self._loop.run_forever()

    def _run(self, coro):  # type: ignore[no-untyped-def]
        fut = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return fut.result(timeout=self._op_timeout_s)

    async def _init(self) -> None:
        self._engine = create_async_db_engine(self._database_url, echo=self._echo)
        await init_async_db(self._engine)
        self._job_store = AsyncSQLJobStore(engine=self._engine, agent_id=self._agent_id)
        self._kv_store = AsyncSQLKVStore(engine=self._engine, agent_id=self._agent_id)

    async def _shutdown(self) -> None:
        if self._engine is not None:
            await self._engine.dispose()

    def shutdown(self) -> None:
        try:
            self._run(self._shutdown())
        except Exception:
            pass
        try:
            self._loop.call_soon_threadsafe(self._loop.stop)
        except Exception:
            pass
        try:
            self._thread.join(timeout=2.0)
        except Exception:
            pass


class _SyncJobStore:
    def __init__(self, db: AsyncSQLDatabase) -> None:
        self._db = db

    def list_jobs(self, *, limit: int) -> List[Dict[str, Any]]:
        store = self._db._job_store
        if store is None:
            return []
        return self._db._run(store.list_jobs(limit=limit))

    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        store = self._db._job_store
        if store is None:
            return None
        return self._db._run(store.get_job(job_id))

    def upsert_job(self, job: Dict[str, Any]) -> None:
        store = self._db._job_store
        if store is None:
            return
        self._db._run(store.upsert_job(job))

    def mark_in_flight_failed(self, *, reason: str = "Server restarted") -> int:
        store = self._db._job_store
        if store is None:
            return 0
        return self._db._run(store.mark_in_flight_failed(reason=reason))


class _SyncKVStore:
    def __init__(self, db: AsyncSQLDatabase) -> None:
        self._db = db

    def get_json(self, key: str) -> Optional[Dict[str, Any]]:
        store = self._db._kv_store
        if store is None:
            return None
        return self._db._run(store.get_json(key))

    def set_json(self, key: str, value: Dict[str, Any]) -> None:
        store = self._db._kv_store
        if store is None:
            return
        self._db._run(store.set_json(key, value))
