from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

from alembic import command
from alembic.config import Config as AlembicConfig
from sqlalchemy import and_, delete, func, or_, select as sa_select
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from sql_store import (
    AgentHeartbeatHistoryRecord,
    AgentHeartbeatHistoryTagRecord,
    AudioAnalysisRecord,
    AuditLogRecord,
    EventLogRecord,
    AuthApiKeyRecord,
    AuthLoginAttemptRecord,
    AuthPasswordResetRecord,
    AuthSessionRecord,
    AuthUserRecord,
    AgentHeartbeatRecord,
    AgentOverrideRecord,
    FppScriptRecord,
    FseqExportRecord,
    GlobalKVRecord,
    JobRecord,
    KVRecord,
    LastAppliedRecord,
    LeaseRecord,
    MetricsSampleRecord,
    PackIngestRecord,
    OrchestrationPeerResultRecord,
    OrchestrationPresetRecord,
    OrchestrationRunRecord,
    OrchestrationStepRecord,
    ReconcileRunRecord,
    SchedulerEventRecord,
    ShowConfigRecord,
    SequenceMetaRecord,
)


def _now() -> float:
    return time.time()


def normalize_database_url_async(raw: str) -> str:
    """
    Normalize common DB URL variants to SQLAlchemy AsyncEngine-compatible URLs.

    - mysql://... -> mysql+aiomysql://...
    - mysql+pymysql://... -> mysql+aiomysql://...
    - sqlite:///... or sqlite:////... -> sqlite+aiosqlite:///... or sqlite+aiosqlite:////...
    """
    url = str(raw or "").strip()
    if not url:
        raise ValueError("Empty database URL")
    if url.startswith("mysql://"):
        return "mysql+aiomysql://" + url[len("mysql://") :]
    if url.startswith("mysql+pymysql://"):
        return "mysql+aiomysql://" + url[len("mysql+pymysql://") :]
    if url.startswith("sqlite:") and not url.startswith("sqlite+aiosqlite:"):
        return "sqlite+aiosqlite:" + url[len("sqlite:") :]
    return url


@dataclass(frozen=True)
class DatabaseHealth:
    ok: bool
    detail: str


class DatabaseService:
    """
    SQLModel-based DB service with an async API.

    Implementation note:
    - Uses a true SQLAlchemy AsyncEngine + AsyncSession.
    - Requires an async DB driver (aiomysql/aiosqlite) and `greenlet` for SQLAlchemy's
      asyncio support.
    """

    def __init__(
        self,
        *,
        database_url: str,
        agent_id: str,
        echo: bool = False,
        migrate_on_startup: bool = True,
    ) -> None:
        self.database_url = str(database_url).strip()
        self.agent_id = str(agent_id)
        self.migrate_on_startup = bool(migrate_on_startup)
        async_url = normalize_database_url_async(self.database_url)
        connect_args: Dict[str, Any] = {}
        if async_url.startswith("sqlite+aiosqlite:"):
            # SQLite driver uses a thread internally; disable same-thread checks.
            connect_args["check_same_thread"] = False
        self.engine: AsyncEngine = create_async_engine(
            async_url,
            echo=bool(echo),
            pool_pre_ping=True,
            connect_args=connect_args,
        )

    async def init(self) -> None:
        if not self.migrate_on_startup:
            health = await self.health()
            if not health.ok:
                raise RuntimeError(f"Database health check failed: {health.detail}")
            return
        # Use Alembic migrations for schema setup/updates.
        for attempt in range(2):
            try:
                await self._run_migrations()
                break
            except Exception:
                if attempt >= 1:
                    raise
                await asyncio.sleep(0.25)

    def _alembic_config(self) -> AlembicConfig:
        base_dir = Path(__file__).resolve().parents[1]
        ini_path = base_dir / "alembic.ini"
        cfg = AlembicConfig(str(ini_path))
        cfg.set_main_option("script_location", str(base_dir / "alembic"))
        cfg.set_main_option("sqlalchemy.url", self.database_url)
        return cfg

    async def _run_migrations(self) -> None:
        cfg = self._alembic_config()
        await asyncio.to_thread(command.upgrade, cfg, "head")

    async def close(self) -> None:
        try:
            await self.engine.dispose()
        except Exception:
            try:
                await self.engine.dispose()
            except Exception:
                pass

    async def health(self) -> DatabaseHealth:
        try:
            async with AsyncSession(self.engine) as session:
                res = await session.exec(sa_select(1))
                _ = res.one()
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
        now = _now()
        async with AsyncSession(self.engine) as session:
            rec = await session.get(AgentHeartbeatRecord, aid)
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
                rec.controller_kind = str(controller_kind or rec.controller_kind or "")
                rec.version = str(version or rec.version or "")
                rec.payload = dict(payload or {})
            await session.commit()

    async def list_agent_heartbeats(self, *, limit: int = 200) -> list[dict[str, Any]]:
        lim = max(1, int(limit))
        async with AsyncSession(self.engine) as session:
            stmt = (
                select(AgentHeartbeatRecord)
                .order_by(AgentHeartbeatRecord.updated_at.desc())
                .limit(lim)
            )
            rows = (await session.exec(stmt)).all()
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

    async def get_agent_heartbeat(self, *, agent_id: str) -> dict[str, Any] | None:
        aid = str(agent_id).strip()
        if not aid:
            return None
        async with AsyncSession(self.engine) as session:
            r = await session.get(AgentHeartbeatRecord, aid)
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

    async def list_agent_overrides(self, *, limit: int = 200) -> list[dict[str, Any]]:
        lim = max(1, int(limit))
        async with AsyncSession(self.engine) as session:
            stmt = (
                select(AgentOverrideRecord)
                .order_by(AgentOverrideRecord.updated_at.desc())
                .limit(lim)
            )
            rows = (await session.exec(stmt)).all()
            return [r.model_dump() for r in rows]

    async def get_agent_override(self, *, agent_id: str) -> dict[str, Any] | None:
        aid = str(agent_id).strip()
        if not aid:
            return None
        async with AsyncSession(self.engine) as session:
            rec = await session.get(AgentOverrideRecord, aid)
            return rec.model_dump() if rec is not None else None

    async def upsert_agent_override(
        self,
        *,
        agent_id: str,
        role: str | None,
        tags: list[str] | None,
        updated_by: str | None = None,
    ) -> dict[str, Any]:
        aid = str(agent_id).strip()
        if not aid:
            raise ValueError("agent_id is required")
        now = _now()
        role_val = str(role).strip() if role is not None else None
        tags_val = [str(t).strip() for t in (tags or []) if str(t).strip()]
        async with AsyncSession(self.engine) as session:
            rec = await session.get(AgentOverrideRecord, aid)
            if rec is None:
                rec = AgentOverrideRecord(
                    agent_id=aid,
                    updated_at=now,
                    updated_by=str(updated_by) if updated_by else None,
                    role=role_val,
                    tags=tags_val,
                )
                session.add(rec)
            else:
                rec.updated_at = now
                rec.updated_by = str(updated_by) if updated_by else rec.updated_by
                rec.role = role_val
                rec.tags = tags_val
            await session.commit()
            return rec.model_dump()

    async def delete_agent_override(self, *, agent_id: str) -> bool:
        aid = str(agent_id).strip()
        if not aid:
            return False
        async with AsyncSession(self.engine) as session:
            rec = await session.get(AgentOverrideRecord, aid)
            if rec is None:
                return False
            await session.delete(rec)
            await session.commit()
            return True

    async def add_agent_heartbeat_history(
        self,
        *,
        agent_id: str,
        created_at: float | None = None,
        updated_at: float,
        name: str,
        role: str,
        controller_kind: str,
        version: str,
        base_url: str | None = None,
        payload: Dict[str, Any] | None = None,
    ) -> None:
        aid = str(agent_id).strip()
        if not aid:
            return
        now = _now()
        created = float(created_at) if created_at is not None else now
        payload_dict = dict(payload or {})
        tags: list[str] = []
        raw_tags = payload_dict.get("tags")
        if isinstance(raw_tags, list):
            seen: set[str] = set()
            for t in raw_tags:
                tag = str(t or "").strip()
                if not tag or tag in seen:
                    continue
                seen.add(tag)
                tags.append(tag)
        async with AsyncSession(self.engine) as session:
            rec = AgentHeartbeatHistoryRecord(
                agent_id=aid,
                created_at=created,
                updated_at=float(updated_at or now),
                name=str(name or ""),
                role=str(role or ""),
                controller_kind=str(controller_kind or ""),
                version=str(version or ""),
                base_url=str(base_url).strip() or None,
                payload=payload_dict,
            )
            session.add(rec)
            await session.flush()
            if tags and rec.id is not None:
                for tag in tags:
                    session.add(
                        AgentHeartbeatHistoryTagRecord(
                            history_id=int(rec.id),
                            agent_id=aid,
                            tag=tag,
                            created_at=created,
                        )
                    )
            await session.commit()

    async def list_agent_heartbeat_history(
        self,
        *,
        limit: int = 200,
        agent_id: str | None = None,
        tag: str | None = None,
        role: str | None = None,
        since: float | None = None,
        until: float | None = None,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        lim = max(1, int(limit))
        aid = str(agent_id).strip() if agent_id else ""
        tag_val = str(tag).strip() if tag else ""
        role_val = str(role).strip() if role else ""
        since_val = float(since) if since is not None else None
        until_val = float(until) if until is not None else None
        off = max(0, int(offset))
        async with AsyncSession(self.engine) as session:
            stmt = select(AgentHeartbeatHistoryRecord)
            if tag_val:
                stmt = stmt.join(
                    AgentHeartbeatHistoryTagRecord,
                    AgentHeartbeatHistoryTagRecord.history_id
                    == AgentHeartbeatHistoryRecord.id,
                ).where(AgentHeartbeatHistoryTagRecord.tag == tag_val)
            stmt = stmt.order_by(AgentHeartbeatHistoryRecord.created_at.desc())
            if aid:
                stmt = stmt.where(AgentHeartbeatHistoryRecord.agent_id == aid)
            if role_val:
                stmt = stmt.where(AgentHeartbeatHistoryRecord.role == role_val)
            if since_val is not None:
                stmt = stmt.where(
                    AgentHeartbeatHistoryRecord.created_at >= float(since_val)
                )
            if until_val is not None:
                stmt = stmt.where(
                    AgentHeartbeatHistoryRecord.created_at <= float(until_val)
                )
            if tag_val:
                stmt = stmt.distinct()
            if off:
                stmt = stmt.offset(off)
            stmt = stmt.limit(lim)
            rows = (await session.exec(stmt)).all()
            return [r.model_dump() for r in rows]

    async def backfill_agent_heartbeat_history_tags(
        self,
        *,
        limit: int = 2000,
    ) -> int:
        lim = max(1, int(limit))
        async with AsyncSession(self.engine) as session:
            subq = select(AgentHeartbeatHistoryTagRecord.history_id)
            stmt = (
                select(AgentHeartbeatHistoryRecord)
                .where(~AgentHeartbeatHistoryRecord.id.in_(subq))
                .order_by(AgentHeartbeatHistoryRecord.created_at.desc())
                .limit(lim)
            )
            rows = (await session.exec(stmt)).all()
            inserted = 0
            for rec in rows:
                if rec.id is None:
                    continue
                payload = rec.payload or {}
                if not isinstance(payload, dict):
                    continue
                raw_tags = payload.get("tags")
                if not isinstance(raw_tags, list):
                    continue
                seen: set[str] = set()
                for t in raw_tags:
                    tag = str(t or "").strip()
                    if not tag or tag in seen:
                        continue
                    seen.add(tag)
                    session.add(
                        AgentHeartbeatHistoryTagRecord(
                            history_id=int(rec.id),
                            agent_id=str(rec.agent_id),
                            tag=tag,
                            created_at=float(rec.created_at or _now()),
                        )
                    )
                    inserted += 1
            if inserted:
                await session.commit()
            return inserted

    async def get_latest_agent_heartbeat_history_map(
        self, *, agent_ids: list[str] | None = None
    ) -> dict[str, float]:
        ids = [str(x).strip() for x in (agent_ids or []) if str(x).strip()]
        async with AsyncSession(self.engine) as session:
            stmt = (
                select(
                    AgentHeartbeatHistoryRecord.agent_id,
                    func.max(AgentHeartbeatHistoryRecord.created_at),
                )
                .group_by(AgentHeartbeatHistoryRecord.agent_id)
                .order_by(AgentHeartbeatHistoryRecord.agent_id.asc())
            )
            if ids:
                stmt = stmt.where(AgentHeartbeatHistoryRecord.agent_id.in_(ids))
            rows = (await session.exec(stmt)).all()
            out: dict[str, float] = {}
            for aid, ts in rows:
                try:
                    out[str(aid)] = float(ts or 0.0)
                except Exception:
                    continue
            return out

    async def enforce_agent_heartbeat_history_retention(
        self,
        *,
        max_rows: Optional[int],
        max_days: Optional[int],
        batch_size: int = 1000,
    ) -> Dict[str, Any]:
        max_rows_i = int(max_rows) if max_rows is not None else 0
        max_days_i = int(max_days) if max_days is not None else 0
        batch = max(1, min(10_000, int(batch_size)))
        deleted_by_days = 0
        deleted_by_rows = 0

        async with AsyncSession(self.engine) as session:
            if max_days_i > 0:
                cutoff = _now() - (max_days_i * 86400.0)
                try:
                    stmt_tags = delete(AgentHeartbeatHistoryTagRecord).where(
                        AgentHeartbeatHistoryTagRecord.history_id.in_(
                            select(AgentHeartbeatHistoryRecord.id).where(
                                AgentHeartbeatHistoryRecord.created_at < float(cutoff)
                            )
                        )
                    )
                    await session.exec(stmt_tags)  # type: ignore[arg-type]
                except Exception:
                    pass
                stmt = delete(AgentHeartbeatHistoryRecord).where(
                    AgentHeartbeatHistoryRecord.created_at < float(cutoff)
                )
                res = await session.exec(stmt)  # type: ignore[arg-type]
                deleted_by_days = int(getattr(res, "rowcount", 0) or 0)

            if max_rows_i > 0:
                res = await session.exec(
                    select(func.count()).select_from(AgentHeartbeatHistoryRecord)
                )
                total = int(res.one() or 0)
                excess = total - max_rows_i
                if excess > 0:
                    limit = min(excess, batch)
                    ids = (
                        await session.exec(
                            select(AgentHeartbeatHistoryRecord.id)
                            .order_by(AgentHeartbeatHistoryRecord.created_at.asc())
                            .limit(limit)
                        )
                    ).all()
                    ids_list = [int(x) for x in ids if x is not None]
                    if ids_list:
                        try:
                            stmt_tags = delete(AgentHeartbeatHistoryTagRecord).where(
                                AgentHeartbeatHistoryTagRecord.history_id.in_(ids_list)
                            )
                            await session.exec(stmt_tags)  # type: ignore[arg-type]
                        except Exception:
                            pass
                        stmt_del = delete(AgentHeartbeatHistoryRecord).where(
                            AgentHeartbeatHistoryRecord.id.in_(ids_list)
                        )
                        res = await session.exec(stmt_del)  # type: ignore[arg-type]
                        deleted_by_rows = int(getattr(res, "rowcount", 0) or 0)

            await session.commit()

        return {
            "ok": True,
            "deleted_by_days": deleted_by_days,
            "deleted_by_rows": deleted_by_rows,
            "max_days": max_days_i,
            "max_rows": max_rows_i,
        }

    # ---- Auth users + sessions ----

    async def list_auth_users(self) -> list[dict[str, Any]]:
        async with AsyncSession(self.engine) as session:
            rows = (
                await session.exec(select(AuthUserRecord).order_by(AuthUserRecord.username.asc()))
            ).all()
            return [r.model_dump() for r in rows]

    async def get_auth_user(self, username: str) -> dict[str, Any] | None:
        uname = str(username or "").strip()
        if not uname:
            return None
        async with AsyncSession(self.engine) as session:
            rec = await session.get(AuthUserRecord, uname)
            return rec.model_dump() if rec else None

    async def create_auth_user(
        self,
        *,
        username: str,
        password_hash: str,
        totp_secret: str,
        role: str = "user",
        disabled: bool = False,
        ip_allowlist: list[str] | None = None,
    ) -> None:
        uname = str(username or "").strip()
        if not uname:
            raise ValueError("username is required")
        now = _now()
        async with AsyncSession(self.engine) as session:
            existing = await session.get(AuthUserRecord, uname)
            if existing is not None:
                raise ValueError("User already exists")
            rec = AuthUserRecord(
                username=uname,
                password_hash=str(password_hash or ""),
                totp_secret=str(totp_secret or ""),
                role=str(role or "user"),
                disabled=bool(disabled),
                ip_allowlist=list(ip_allowlist or []),
                created_at=now,
                updated_at=now,
            )
            session.add(rec)
            await session.commit()

    async def ensure_auth_user(
        self,
        *,
        username: str,
        password_hash: str,
        totp_secret: str,
        role: str = "user",
        disabled: bool = False,
        ip_allowlist: list[str] | None = None,
    ) -> bool:
        """
        Ensure an auth user exists. Returns True if created.
        """
        uname = str(username or "").strip()
        if not uname:
            return False
        now = _now()
        async with AsyncSession(self.engine) as session:
            existing = await session.get(AuthUserRecord, uname)
            if existing is not None:
                return False
            rec = AuthUserRecord(
                username=uname,
                password_hash=str(password_hash or ""),
                totp_secret=str(totp_secret or ""),
                role=str(role or "user"),
                disabled=bool(disabled),
                ip_allowlist=list(ip_allowlist or []),
                created_at=now,
                updated_at=now,
            )
            session.add(rec)
            await session.commit()
            return True

    async def update_auth_user(
        self,
        *,
        username: str,
        password_hash: str | None = None,
        totp_secret: str | None = None,
        role: str | None = None,
        disabled: bool | None = None,
        ip_allowlist: list[str] | None = None,
    ) -> None:
        uname = str(username or "").strip()
        if not uname:
            raise ValueError("username is required")
        async with AsyncSession(self.engine) as session:
            rec = await session.get(AuthUserRecord, uname)
            if rec is None:
                raise ValueError("User not found")
            if password_hash is not None:
                rec.password_hash = str(password_hash or "")
            if totp_secret is not None:
                rec.totp_secret = str(totp_secret or "")
            if role is not None:
                rec.role = str(role or "user")
            if disabled is not None:
                rec.disabled = bool(disabled)
            if ip_allowlist is not None:
                rec.ip_allowlist = list(ip_allowlist)
            rec.updated_at = _now()
            await session.commit()

    async def delete_auth_user(self, username: str) -> None:
        uname = str(username or "").strip()
        if not uname:
            return
        async with AsyncSession(self.engine) as session:
            rec = await session.get(AuthUserRecord, uname)
            if rec is None:
                return
            await session.delete(rec)
            await session.commit()

    async def touch_auth_user_login(self, username: str) -> None:
        uname = str(username or "").strip()
        if not uname:
            return
        async with AsyncSession(self.engine) as session:
            rec = await session.get(AuthUserRecord, uname)
            if rec is None:
                return
            now = _now()
            rec.last_login_at = now
            rec.updated_at = now
            await session.commit()

    async def create_auth_session(
        self,
        *,
        jti: str,
        username: str,
        expires_at: float,
        ip: str | None = None,
        user_agent: str | None = None,
    ) -> None:
        sid = str(jti or "").strip()
        uname = str(username or "").strip()
        if not sid or not uname:
            return
        now = _now()
        async with AsyncSession(self.engine) as session:
            rec = AuthSessionRecord(
                jti=sid,
                username=uname,
                created_at=now,
                expires_at=float(expires_at),
                revoked_at=None,
                last_seen_at=now,
                ip=str(ip)[:64] if ip else None,
                user_agent=str(user_agent)[:256] if user_agent else None,
            )
            session.add(rec)
            await session.commit()

    async def get_auth_session(self, jti: str) -> dict[str, Any] | None:
        sid = str(jti or "").strip()
        if not sid:
            return None
        async with AsyncSession(self.engine) as session:
            rec = await session.get(AuthSessionRecord, sid)
            return rec.model_dump() if rec else None

    async def touch_auth_session(self, jti: str, *, min_interval_s: float = 60.0) -> None:
        sid = str(jti or "").strip()
        if not sid:
            return
        async with AsyncSession(self.engine) as session:
            rec = await session.get(AuthSessionRecord, sid)
            if rec is None:
                return
            now = _now()
            last = float(rec.last_seen_at or 0.0)
            if (now - last) < float(min_interval_s):
                return
            rec.last_seen_at = now
            await session.commit()

    async def revoke_auth_session(self, jti: str) -> None:
        sid = str(jti or "").strip()
        if not sid:
            return
        async with AsyncSession(self.engine) as session:
            rec = await session.get(AuthSessionRecord, sid)
            if rec is None:
                return
            rec.revoked_at = _now()
            await session.commit()

    async def revoke_auth_sessions_for_user(
        self, username: str, *, skip_jti: str | None = None
    ) -> int:
        uname = str(username or "").strip()
        if not uname:
            return 0
        skip = str(skip_jti or "").strip()
        async with AsyncSession(self.engine) as session:
            stmt = (
                select(AuthSessionRecord)
                .where(AuthSessionRecord.username == uname)
                .where(AuthSessionRecord.revoked_at.is_(None))
            )
            rows = (await session.exec(stmt)).all()
            now = _now()
            revoked = 0
            for rec in rows:
                if skip and str(rec.jti) == skip:
                    continue
                rec.revoked_at = now
                revoked += 1
            await session.commit()
            return revoked

    async def list_auth_sessions(
        self,
        *,
        username: str | None = None,
        active_only: bool = False,
        limit: int = 200,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        lim = max(1, int(limit))
        off = max(0, int(offset))
        uname = str(username or "").strip()
        now = _now()
        async with AsyncSession(self.engine) as session:
            stmt = select(AuthSessionRecord).order_by(AuthSessionRecord.created_at.desc())
            if uname:
                stmt = stmt.where(AuthSessionRecord.username == uname)
            if active_only:
                stmt = stmt.where(AuthSessionRecord.revoked_at.is_(None))
                stmt = stmt.where(AuthSessionRecord.expires_at > float(now))
            if off:
                stmt = stmt.offset(off)
            stmt = stmt.limit(lim)
            rows = (await session.exec(stmt)).all()
            return [r.model_dump() for r in rows]

    async def cleanup_auth_sessions(self, *, max_age_s: float = 0) -> int:
        now = _now()
        max_age = float(max_age_s or 0.0)
        deleted = 0
        async with AsyncSession(self.engine) as session:
            stmt = delete(AuthSessionRecord).where(
                AuthSessionRecord.expires_at < float(now)
            )
            if max_age > 0:
                stmt = stmt.where(AuthSessionRecord.created_at < float(now - max_age))
            res = await session.exec(stmt)  # type: ignore[arg-type]
            deleted = int(getattr(res, "rowcount", 0) or 0)
            await session.commit()
        return deleted

    async def get_auth_login_state(
        self,
        *,
        username: str,
        ip: str,
        window_s: float,
    ) -> dict[str, Any] | None:
        uname = str(username or "").strip()
        ip_s = str(ip or "").strip() or "unknown"
        if not uname:
            return None
        async with AsyncSession(self.engine) as session:
            rec = await session.get(AuthLoginAttemptRecord, (uname, ip_s))
            if rec is None:
                return None
            now = _now()
            if (now - float(rec.first_failed_at or 0.0)) > float(window_s):
                await session.delete(rec)
                await session.commit()
                return None
            return rec.model_dump()

    async def list_auth_login_attempts(
        self,
        *,
        username: str | None = None,
        ip: str | None = None,
        locked_only: bool = False,
        limit: int = 200,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        lim = max(1, int(limit))
        off = max(0, int(offset))
        uname = str(username or "").strip()
        ip_s = str(ip or "").strip()
        now = _now()
        async with AsyncSession(self.engine) as session:
            stmt = select(AuthLoginAttemptRecord).order_by(
                AuthLoginAttemptRecord.last_failed_at.desc()
            )
            if uname:
                stmt = stmt.where(AuthLoginAttemptRecord.username == uname)
            if ip_s:
                stmt = stmt.where(AuthLoginAttemptRecord.ip == ip_s)
            if locked_only:
                stmt = stmt.where(AuthLoginAttemptRecord.locked_until > float(now))
            if off:
                stmt = stmt.offset(off)
            stmt = stmt.limit(lim)
            rows = (await session.exec(stmt)).all()
            return [r.model_dump() for r in rows]

    async def record_auth_login_failure(
        self,
        *,
        username: str,
        ip: str,
        max_attempts: int,
        window_s: float,
        lockout_s: float,
    ) -> dict[str, Any]:
        uname = str(username or "").strip()
        ip_s = str(ip or "").strip() or "unknown"
        if not uname:
            return {"failed_count": 0, "locked_until": None}
        max_tries = max(1, int(max_attempts))
        win = max(10.0, float(window_s))
        lock_s = max(10.0, float(lockout_s))
        now = _now()
        async with AsyncSession(self.engine) as session:
            rec = await session.get(AuthLoginAttemptRecord, (uname, ip_s))
            if rec is None or (now - float(rec.first_failed_at or 0.0)) > win:
                rec = AuthLoginAttemptRecord(
                    username=uname,
                    ip=ip_s,
                    failed_count=1,
                    first_failed_at=now,
                    last_failed_at=now,
                    locked_until=None,
                )
                session.add(rec)
            else:
                rec.failed_count = int(rec.failed_count or 0) + 1
                rec.last_failed_at = now
            if int(rec.failed_count or 0) >= max_tries:
                rec.locked_until = now + lock_s
            await session.commit()
            return rec.model_dump()

    async def clear_auth_login_attempts(self, *, username: str, ip: str) -> None:
        uname = str(username or "").strip()
        ip_s = str(ip or "").strip() or "unknown"
        if not uname:
            return
        async with AsyncSession(self.engine) as session:
            rec = await session.get(AuthLoginAttemptRecord, (uname, ip_s))
            if rec is None:
                return
            await session.delete(rec)
            await session.commit()

    async def clear_auth_login_attempts_bulk(
        self,
        *,
        username: str | None = None,
        ip: str | None = None,
    ) -> int:
        uname = str(username or "").strip()
        ip_s = str(ip or "").strip()
        async with AsyncSession(self.engine) as session:
            stmt = delete(AuthLoginAttemptRecord)
            if uname:
                stmt = stmt.where(AuthLoginAttemptRecord.username == uname)
            if ip_s:
                stmt = stmt.where(AuthLoginAttemptRecord.ip == ip_s)
            res = await session.exec(stmt)  # type: ignore[arg-type]
            deleted = int(getattr(res, "rowcount", 0) or 0)
            await session.commit()
            return deleted

    async def cleanup_auth_login_attempts(self, *, older_than_s: float) -> int:
        now = _now()
        cutoff = now - max(60.0, float(older_than_s or 0.0))
        async with AsyncSession(self.engine) as session:
            stmt = delete(AuthLoginAttemptRecord).where(
                AuthLoginAttemptRecord.last_failed_at < float(cutoff)
            )
            res = await session.exec(stmt)  # type: ignore[arg-type]
            deleted = int(getattr(res, "rowcount", 0) or 0)
            await session.commit()
            return deleted

    # ---- Auth API keys ----

    async def create_auth_api_key(
        self,
        *,
        username: str,
        key_hash: str,
        label: str | None = None,
        prefix: str | None = None,
        expires_at: float | None = None,
    ) -> dict[str, Any]:
        uname = str(username or "").strip()
        if not uname:
            raise ValueError("username is required")
        now = _now()
        async with AsyncSession(self.engine) as session:
            rec = AuthApiKeyRecord(
                username=uname,
                label=str(label or "") or None,
                prefix=str(prefix or "") or None,
                key_hash=str(key_hash or ""),
                created_at=now,
                last_used_at=None,
                revoked_at=None,
                expires_at=float(expires_at) if expires_at else None,
            )
            session.add(rec)
            await session.commit()
            await session.refresh(rec)
            return rec.model_dump()

    async def list_auth_api_keys(
        self,
        *,
        username: str | None = None,
        active_only: bool = False,
        limit: int = 200,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        lim = max(1, int(limit))
        off = max(0, int(offset))
        uname = str(username or "").strip()
        now = _now()
        async with AsyncSession(self.engine) as session:
            stmt = select(AuthApiKeyRecord).order_by(AuthApiKeyRecord.created_at.desc())
            if uname:
                stmt = stmt.where(AuthApiKeyRecord.username == uname)
            if active_only:
                stmt = stmt.where(AuthApiKeyRecord.revoked_at.is_(None))
                stmt = stmt.where(
                    (AuthApiKeyRecord.expires_at.is_(None))
                    | (AuthApiKeyRecord.expires_at > float(now))
                )
            if off:
                stmt = stmt.offset(off)
            stmt = stmt.limit(lim)
            rows = (await session.exec(stmt)).all()
            return [r.model_dump() for r in rows]

    async def get_auth_api_key_by_hash(self, key_hash: str) -> dict[str, Any] | None:
        kh = str(key_hash or "").strip()
        if not kh:
            return None
        async with AsyncSession(self.engine) as session:
            stmt = select(AuthApiKeyRecord).where(AuthApiKeyRecord.key_hash == kh)
            rec = (await session.exec(stmt)).one_or_none()
            return rec.model_dump() if rec else None

    async def touch_auth_api_key(self, key_id: int) -> None:
        kid = int(key_id)
        async with AsyncSession(self.engine) as session:
            rec = await session.get(AuthApiKeyRecord, kid)
            if rec is None:
                return
            rec.last_used_at = _now()
            await session.commit()

    async def revoke_auth_api_key(self, key_id: int) -> None:
        kid = int(key_id)
        async with AsyncSession(self.engine) as session:
            rec = await session.get(AuthApiKeyRecord, kid)
            if rec is None:
                return
            rec.revoked_at = _now()
            await session.commit()

    async def revoke_auth_api_keys_for_user(self, username: str) -> int:
        uname = str(username or "").strip()
        if not uname:
            return 0
        async with AsyncSession(self.engine) as session:
            stmt = (
                select(AuthApiKeyRecord)
                .where(AuthApiKeyRecord.username == uname)
                .where(AuthApiKeyRecord.revoked_at.is_(None))
            )
            rows = (await session.exec(stmt)).all()
            now = _now()
            for rec in rows:
                rec.revoked_at = now
            await session.commit()
            return len(rows)

    async def cleanup_auth_api_keys(self, *, older_than_s: float = 0) -> int:
        now = _now()
        cutoff = None
        if older_than_s and older_than_s > 0:
            cutoff = now - float(older_than_s)
        async with AsyncSession(self.engine) as session:
            stmt = delete(AuthApiKeyRecord).where(
                or_(
                    AuthApiKeyRecord.revoked_at.is_not(None),
                    AuthApiKeyRecord.expires_at < float(now),
                )
            )
            if cutoff is not None:
                stmt = stmt.where(AuthApiKeyRecord.created_at < float(cutoff))
            res = await session.exec(stmt)  # type: ignore[arg-type]
            deleted = int(getattr(res, "rowcount", 0) or 0)
            await session.commit()
            return deleted

    # ---- Auth password resets ----

    async def create_auth_password_reset(
        self,
        *,
        username: str,
        token_hash: str,
        expires_at: float,
        created_by: str | None = None,
    ) -> dict[str, Any]:
        uname = str(username or "").strip()
        if not uname:
            raise ValueError("username is required")
        now = _now()
        async with AsyncSession(self.engine) as session:
            rec = AuthPasswordResetRecord(
                username=uname,
                token_hash=str(token_hash or ""),
                created_at=now,
                expires_at=float(expires_at),
                used_at=None,
                created_by=str(created_by or "") or None,
                used_ip=None,
            )
            session.add(rec)
            await session.commit()
            await session.refresh(rec)
            return rec.model_dump()

    async def get_auth_password_reset_by_hash(
        self, token_hash: str
    ) -> dict[str, Any] | None:
        th = str(token_hash or "").strip()
        if not th:
            return None
        async with AsyncSession(self.engine) as session:
            stmt = select(AuthPasswordResetRecord).where(
                AuthPasswordResetRecord.token_hash == th
            )
            rec = (await session.exec(stmt)).one_or_none()
            return rec.model_dump() if rec else None

    async def mark_auth_password_reset_used(
        self, reset_id: int, *, used_ip: str | None = None
    ) -> None:
        rid = int(reset_id)
        async with AsyncSession(self.engine) as session:
            rec = await session.get(AuthPasswordResetRecord, rid)
            if rec is None:
                return
            rec.used_at = _now()
            if used_ip is not None:
                rec.used_ip = str(used_ip or "") or None
            await session.commit()

    async def cleanup_auth_password_resets(self, *, older_than_s: float) -> int:
        now = _now()
        cutoff_s = float(older_than_s or 0.0)
        async with AsyncSession(self.engine) as session:
            stmt = delete(AuthPasswordResetRecord).where(
                AuthPasswordResetRecord.expires_at < float(now)
            )
            if cutoff_s > 0:
                stmt = stmt.where(
                    AuthPasswordResetRecord.created_at < float(now - cutoff_s)
                )
            res = await session.exec(stmt)  # type: ignore[arg-type]
            deleted = int(getattr(res, "rowcount", 0) or 0)
            await session.commit()
            return deleted

    # ---- Reconcile run history ----

    async def create_reconcile_run(
        self,
        *,
        source: str,
        options: Dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        now = _now()
        async with AsyncSession(self.engine) as session:
            rec = ReconcileRunRecord(
                agent_id=str(self.agent_id),
                started_at=now,
                finished_at=None,
                status="running",
                source=str(source or "manual"),
                error=None,
                cancel_requested=False,
                options=dict(options or {}),
                result={},
            )
            session.add(rec)
            await session.commit()
            await session.refresh(rec)
            return rec.model_dump()

    async def update_reconcile_run(
        self,
        *,
        run_id: int,
        status: str,
        error: str | None = None,
        finished_at: float | None = None,
        cancel_requested: bool | None = None,
        result: Dict[str, Any] | None = None,
    ) -> None:
        rid = int(run_id)
        async with AsyncSession(self.engine) as session:
            rec = await session.get(ReconcileRunRecord, rid)
            if rec is None:
                return
            rec.status = str(status or rec.status)
            if error is not None:
                rec.error = str(error)[:512] if error else None
            if finished_at is not None:
                rec.finished_at = float(finished_at)
            if cancel_requested is not None:
                rec.cancel_requested = bool(cancel_requested)
            if result is not None:
                rec.result = dict(result or {})
            await session.commit()

    async def list_reconcile_runs(
        self,
        *,
        limit: int = 200,
        offset: int = 0,
        status: str | None = None,
        source: str | None = None,
    ) -> list[dict[str, Any]]:
        lim = max(1, int(limit))
        off = max(0, int(offset))
        st = str(status or "").strip()
        src = str(source or "").strip()
        async with AsyncSession(self.engine) as session:
            stmt = select(ReconcileRunRecord).order_by(
                ReconcileRunRecord.started_at.desc()
            )
            if st:
                stmt = stmt.where(ReconcileRunRecord.status == st)
            if src:
                stmt = stmt.where(ReconcileRunRecord.source == src)
            if off:
                stmt = stmt.offset(off)
            stmt = stmt.limit(lim)
            rows = (await session.exec(stmt)).all()
            return [r.model_dump() for r in rows]

    async def mark_reconcile_cancel_requested(self, run_id: int) -> None:
        rid = int(run_id)
        async with AsyncSession(self.engine) as session:
            rec = await session.get(ReconcileRunRecord, rid)
            if rec is None:
                return
            rec.cancel_requested = True
            await session.commit()

    # ---- Orchestration presets ----

    async def list_orchestration_presets(
        self,
        *,
        limit: int = 200,
        offset: int = 0,
        scope: str | None = None,
    ) -> list[dict[str, Any]]:
        lim = max(1, int(limit))
        off = max(0, int(offset))
        sc = str(scope or "").strip()
        async with AsyncSession(self.engine) as session:
            stmt = select(OrchestrationPresetRecord).order_by(
                OrchestrationPresetRecord.updated_at.desc()
            )
            if sc:
                stmt = stmt.where(OrchestrationPresetRecord.scope == sc)
            if off:
                stmt = stmt.offset(off)
            stmt = stmt.limit(lim)
            rows = (await session.exec(stmt)).all()
            return [r.model_dump() for r in rows]

    async def upsert_orchestration_preset(
        self,
        *,
        name: str,
        scope: str,
        payload: Dict[str, Any],
        description: str | None = None,
        tags: list[str] | None = None,
        version: int | None = None,
    ) -> dict[str, Any]:
        nm = str(name or "").strip()
        sc = str(scope or "").strip() or "local"
        if not nm:
            raise ValueError("Preset name is required")
        now = _now()
        async with AsyncSession(self.engine) as session:
            stmt = select(OrchestrationPresetRecord).where(
                OrchestrationPresetRecord.name == nm,
                OrchestrationPresetRecord.scope == sc,
            )
            rec = (await session.exec(stmt)).one_or_none()
            if rec is None:
                ver = int(version) if version is not None else 1
                rec = OrchestrationPresetRecord(
                    name=nm,
                    scope=sc,
                    description=str(description or "") or None,
                    tags=list(tags or []),
                    version=max(1, ver),
                    created_at=now,
                    updated_at=now,
                    payload=dict(payload or {}),
                )
                session.add(rec)
            else:
                rec.updated_at = now
                rec.description = str(description or "") or None
                rec.payload = dict(payload or {})
                if tags is not None:
                    rec.tags = list(tags)
                if version is not None:
                    rec.version = max(1, int(version))
                else:
                    rec.version = max(1, int(rec.version or 1) + 1)
            await session.commit()
            return rec.model_dump()

    async def delete_orchestration_preset(self, preset_id: int) -> None:
        pid = int(preset_id)
        async with AsyncSession(self.engine) as session:
            rec = await session.get(OrchestrationPresetRecord, pid)
            if rec is None:
                return
            await session.delete(rec)
            await session.commit()

    # ---- DB leases / locks ----

    async def get_lease(self, key: str) -> dict[str, Any] | None:
        k = str(key).strip()
        if not k:
            return None
        async with AsyncSession(self.engine) as session:
            rec = await session.get(LeaseRecord, k)
            if rec is None:
                return None
            return {
                "key": rec.key,
                "owner_id": rec.owner_id,
                "expires_at": float(rec.expires_at),
                "updated_at": float(rec.updated_at),
            }

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

        for attempt in range(2):
            try:
                now = _now()
                async with AsyncSession(self.engine) as session:
                    async with session.begin():
                        stmt = (
                            select(LeaseRecord)
                            .where(LeaseRecord.key == k)
                            .with_for_update()
                        )
                        rec = (await session.exec(stmt)).one_or_none()
                        if rec is None:
                            session.add(
                                LeaseRecord(
                                    key=k,
                                    owner_id=owner,
                                    expires_at=now + ttl,
                                    updated_at=now,
                                )
                            )
                            return True

                        if (
                            str(rec.owner_id) == owner
                            or float(rec.expires_at or 0.0) < now
                        ):
                            rec.owner_id = owner
                            rec.expires_at = now + ttl
                            rec.updated_at = now
                            return True

                        return False
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
        now = _now()
        async with AsyncSession(self.engine) as session:
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
            await session.commit()

    async def list_scheduler_events(
        self,
        *,
        limit: int = 200,
        agent_id: str | None = None,
        since: float | None = None,
        until: float | None = None,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        lim = max(1, int(limit))
        aid = str(agent_id).strip() if agent_id else ""
        since_val = float(since) if since is not None else None
        until_val = float(until) if until is not None else None
        off = max(0, int(offset))
        async with AsyncSession(self.engine) as session:
            stmt = select(SchedulerEventRecord).order_by(
                SchedulerEventRecord.created_at.desc()
            )
            if aid:
                stmt = stmt.where(SchedulerEventRecord.agent_id == aid)
            if since_val is not None:
                stmt = stmt.where(SchedulerEventRecord.created_at >= float(since_val))
            if until_val is not None:
                stmt = stmt.where(SchedulerEventRecord.created_at <= float(until_val))
            if off:
                stmt = stmt.offset(off)
            stmt = stmt.limit(lim)
            rows = (await session.exec(stmt)).all()
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
        deleted_by_days = 0
        deleted_by_rows = 0

        async with AsyncSession(self.engine) as session:
            if max_days_i > 0:
                cutoff = _now() - (max_days_i * 86400.0)
                stmt = delete(SchedulerEventRecord).where(
                    SchedulerEventRecord.created_at < float(cutoff)
                )
                res = await session.exec(stmt)  # type: ignore[arg-type]
                deleted_by_days = int(getattr(res, "rowcount", 0) or 0)

            if max_rows_i > 0:
                res = await session.exec(
                    select(func.count()).select_from(SchedulerEventRecord)
                )
                total = int(res.one() or 0)
                excess = total - max_rows_i
                if excess > 0:
                    limit = min(excess, batch)
                    ids = (
                        await session.exec(
                            select(SchedulerEventRecord.id)
                            .order_by(SchedulerEventRecord.created_at.asc())
                            .limit(limit)
                        )
                    ).all()
                    ids_list = [int(x) for x in ids if x is not None]
                    if ids_list:
                        stmt_del = delete(SchedulerEventRecord).where(
                            SchedulerEventRecord.id.in_(ids_list)
                        )
                        res = await session.exec(stmt_del)  # type: ignore[arg-type]
                        deleted_by_rows = int(getattr(res, "rowcount", 0) or 0)

            await session.commit()

        return {
            "ok": True,
            "deleted_by_days": deleted_by_days,
            "deleted_by_rows": deleted_by_rows,
            "max_days": max_days_i,
            "max_rows": max_rows_i,
        }

    # ---- Audit log ----

    async def add_audit_log(
        self,
        *,
        action: str,
        actor: str,
        ok: bool = True,
        resource: str | None = None,
        error: str | None = None,
        ip: str | None = None,
        user_agent: str | None = None,
        request_id: str | None = None,
        payload: Dict[str, Any] | None = None,
        agent_id: str | None = None,
    ) -> None:
        act = str(action or "").strip()
        who = str(actor or "").strip()
        aid = str(agent_id or self.agent_id).strip()
        if not act or not who or not aid:
            return
        now = _now()
        async with AsyncSession(self.engine) as session:
            rec = AuditLogRecord(
                agent_id=aid,
                created_at=now,
                actor=who,
                action=act,
                resource=str(resource or "") or None,
                ok=bool(ok),
                error=str(error)[:512] if error else None,
                ip=str(ip)[:64] if ip else None,
                user_agent=str(user_agent)[:256] if user_agent else None,
                request_id=str(request_id)[:64] if request_id else None,
                payload=dict(payload or {}),
            )
            session.add(rec)
            await session.commit()

    async def list_audit_logs(
        self,
        *,
        limit: int = 200,
        agent_id: str | None = None,
        action: str | None = None,
        actor: str | None = None,
        ok: bool | None = None,
        resource: str | None = None,
        ip: str | None = None,
        error_contains: str | None = None,
        since: float | None = None,
        until: float | None = None,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        lim = max(1, int(limit))
        aid = str(agent_id).strip() if agent_id else ""
        act = str(action).strip() if action else ""
        who = str(actor).strip() if actor else ""
        res = str(resource).strip() if resource else ""
        ip_val = str(ip).strip() if ip else ""
        err = str(error_contains).strip() if error_contains else ""
        since_val = float(since) if since is not None else None
        until_val = float(until) if until is not None else None
        off = max(0, int(offset))
        async with AsyncSession(self.engine) as session:
            stmt = select(AuditLogRecord).order_by(AuditLogRecord.created_at.desc())
            if aid:
                stmt = stmt.where(AuditLogRecord.agent_id == aid)
            if act:
                if "*" in act or act.endswith("."):
                    pattern = act.replace("*", "%")
                    if act.endswith("."):
                        pattern = f"{pattern}%"
                    stmt = stmt.where(AuditLogRecord.action.like(pattern))
                else:
                    stmt = stmt.where(AuditLogRecord.action == act)
            if who:
                stmt = stmt.where(AuditLogRecord.actor == who)
            if ok is not None:
                stmt = stmt.where(AuditLogRecord.ok == bool(ok))
            if res:
                stmt = stmt.where(AuditLogRecord.resource.contains(res))
            if ip_val:
                stmt = stmt.where(AuditLogRecord.ip == ip_val)
            if err:
                stmt = stmt.where(AuditLogRecord.error.contains(err))
            if since_val is not None:
                stmt = stmt.where(AuditLogRecord.created_at >= float(since_val))
            if until_val is not None:
                stmt = stmt.where(AuditLogRecord.created_at <= float(until_val))
            if off:
                stmt = stmt.offset(off)
            stmt = stmt.limit(lim)
            rows = (await session.exec(stmt)).all()
            return [r.model_dump() for r in rows]

    async def audit_log_stats(self) -> dict[str, Any]:
        async with AsyncSession(self.engine) as session:
            stmt = select(
                func.count(),
                func.min(AuditLogRecord.created_at),
                func.max(AuditLogRecord.created_at),
            )
            res = await session.exec(stmt)
            row = res.one()
            count = int(row[0] or 0)
            oldest = float(row[1]) if row[1] is not None else None
            newest = float(row[2]) if row[2] is not None else None
            return {"count": count, "oldest": oldest, "newest": newest}

    async def enforce_audit_log_retention(
        self,
        *,
        max_rows: Optional[int],
        max_days: Optional[int],
        batch_size: int = 1000,
    ) -> Dict[str, Any]:
        max_rows_i = int(max_rows) if max_rows is not None else 0
        max_days_i = int(max_days) if max_days is not None else 0
        batch = max(1, min(10_000, int(batch_size)))
        deleted_by_days = 0
        deleted_by_rows = 0

        async with AsyncSession(self.engine) as session:
            if max_days_i > 0:
                cutoff = _now() - (max_days_i * 86400.0)
                stmt = delete(AuditLogRecord).where(
                    AuditLogRecord.created_at < float(cutoff)
                )
                res = await session.exec(stmt)  # type: ignore[arg-type]
                deleted_by_days = int(getattr(res, "rowcount", 0) or 0)

            if max_rows_i > 0:
                res = await session.exec(select(func.count()).select_from(AuditLogRecord))
                total = int(res.one() or 0)
                excess = total - max_rows_i
                if excess > 0:
                    limit = min(excess, batch)
                    ids = (
                        await session.exec(
                            select(AuditLogRecord.id)
                            .order_by(AuditLogRecord.created_at.asc())
                            .limit(limit)
                        )
                    ).all()
                    ids_list = [int(x) for x in ids if x is not None]
                    if ids_list:
                        stmt_del = delete(AuditLogRecord).where(
                            AuditLogRecord.id.in_(ids_list)
                        )
                        res = await session.exec(stmt_del)  # type: ignore[arg-type]
                        deleted_by_rows = int(getattr(res, "rowcount", 0) or 0)

            await session.commit()

        return {
            "ok": True,
            "deleted_by_days": deleted_by_days,
            "deleted_by_rows": deleted_by_rows,
            "max_days": max_days_i,
            "max_rows": max_rows_i,
        }

    # ---- Event log ----

    async def add_event_log(
        self,
        *,
        event_type: str,
        event: str | None = None,
        payload: Dict[str, Any] | None = None,
        created_at: float | None = None,
        agent_id: str | None = None,
    ) -> int | None:
        etype = str(event_type or "").strip()
        aid = str(agent_id or self.agent_id).strip()
        if not etype or not aid:
            return None
        evt = str(event or "").strip() or None
        now = float(created_at) if created_at is not None else _now()
        async with AsyncSession(self.engine) as session:
            rec = EventLogRecord(
                agent_id=aid,
                created_at=now,
                event_type=etype,
                event=evt,
                payload=dict(payload or {}),
            )
            session.add(rec)
            await session.commit()
            try:
                await session.refresh(rec)
            except Exception:
                pass
            return int(rec.id) if rec.id is not None else None

    async def list_event_logs(
        self,
        *,
        limit: int = 200,
        agent_id: str | None = None,
        event_type: str | None = None,
        event: str | None = None,
        since: float | None = None,
        until: float | None = None,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        lim = max(1, int(limit))
        aid = str(agent_id).strip() if agent_id else ""
        etype = str(event_type).strip() if event_type else ""
        evt = str(event).strip() if event else ""
        since_val = float(since) if since is not None else None
        until_val = float(until) if until is not None else None
        off = max(0, int(offset))
        async with AsyncSession(self.engine) as session:
            stmt = select(EventLogRecord).order_by(EventLogRecord.created_at.desc())
            if aid:
                stmt = stmt.where(EventLogRecord.agent_id == aid)
            if etype:
                stmt = stmt.where(EventLogRecord.event_type == etype)
            if evt:
                stmt = stmt.where(EventLogRecord.event == evt)
            if since_val is not None:
                stmt = stmt.where(EventLogRecord.created_at >= float(since_val))
            if until_val is not None:
                stmt = stmt.where(EventLogRecord.created_at <= float(until_val))
            if off:
                stmt = stmt.offset(off)
            stmt = stmt.limit(lim)
            rows = (await session.exec(stmt)).all()
            return [r.model_dump() for r in rows]

    async def list_event_logs_after_id(
        self,
        *,
        last_id: int,
        limit: int = 200,
        agent_id: str | None = None,
        event_types: list[str] | None = None,
        event_kinds: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        lid = max(0, int(last_id))
        lim = max(1, int(limit))
        aid = str(agent_id).strip() if agent_id else ""
        types = [str(x).strip() for x in (event_types or []) if str(x).strip()]
        kinds = [str(x).strip() for x in (event_kinds or []) if str(x).strip()]
        async with AsyncSession(self.engine) as session:
            stmt = select(EventLogRecord).where(EventLogRecord.id > lid)
            if aid:
                stmt = stmt.where(EventLogRecord.agent_id == aid)
            if types:
                stmt = stmt.where(EventLogRecord.event_type.in_(types))
            if kinds:
                stmt = stmt.where(EventLogRecord.event.in_(kinds))
            stmt = stmt.order_by(EventLogRecord.id.asc()).limit(lim)
            rows = (await session.exec(stmt)).all()
            return [r.model_dump() for r in rows]

    async def get_event_log_by_id(self, *, event_id: int) -> dict[str, Any] | None:
        eid = int(event_id)
        if eid <= 0:
            return None
        async with AsyncSession(self.engine) as session:
            rec = await session.get(EventLogRecord, eid)
            return rec.model_dump() if rec is not None else None

    async def list_event_logs_after_cursor(
        self,
        *,
        after_created_at: float,
        after_id: int,
        limit: int = 200,
        agent_id: str | None = None,
        event_types: list[str] | None = None,
        event_kinds: list[str] | None = None,
        since: float | None = None,
        until: float | None = None,
    ) -> list[dict[str, Any]]:
        lim = max(1, int(limit))
        aid = str(agent_id).strip() if agent_id else ""
        types = [str(x).strip() for x in (event_types or []) if str(x).strip()]
        kinds = [str(x).strip() for x in (event_kinds or []) if str(x).strip()]
        after_ts = float(after_created_at)
        after_id_val = max(0, int(after_id))
        since_val = float(since) if since is not None else None
        until_val = float(until) if until is not None else None
        async with AsyncSession(self.engine) as session:
            stmt = select(EventLogRecord).where(
                or_(
                    EventLogRecord.created_at > after_ts,
                    and_(
                        EventLogRecord.created_at == after_ts,
                        EventLogRecord.id > after_id_val,
                    ),
                )
            )
            if aid:
                stmt = stmt.where(EventLogRecord.agent_id == aid)
            if types:
                stmt = stmt.where(EventLogRecord.event_type.in_(types))
            if kinds:
                stmt = stmt.where(EventLogRecord.event.in_(kinds))
            if since_val is not None:
                stmt = stmt.where(EventLogRecord.created_at >= float(since_val))
            if until_val is not None:
                stmt = stmt.where(EventLogRecord.created_at <= float(until_val))
            stmt = stmt.order_by(EventLogRecord.created_at.asc(), EventLogRecord.id.asc())
            stmt = stmt.limit(lim)
            rows = (await session.exec(stmt)).all()
            return [r.model_dump() for r in rows]

    async def get_event_log_bounds(self) -> dict[str, int | None]:
        async with AsyncSession(self.engine) as session:
            stmt = sa_select(func.min(EventLogRecord.id), func.max(EventLogRecord.id))
            row = (await session.exec(stmt)).one()
            min_id = row[0]
            max_id = row[1]
            return {
                "min_id": int(min_id) if min_id is not None else None,
                "max_id": int(max_id) if max_id is not None else None,
            }

    async def _table_stats(self, model: Any, *, ts_column: Any | None = None) -> dict[str, Any]:
        async with AsyncSession(self.engine) as session:
            col = ts_column if ts_column is not None else model.created_at
            stmt = select(
                func.count(),
                func.min(col),
                func.max(col),
            )
            res = await session.exec(stmt)
            row = res.one()
            count = int(row[0] or 0)
            oldest = float(row[1]) if row[1] is not None else None
            newest = float(row[2]) if row[2] is not None else None
            return {"count": count, "oldest": oldest, "newest": newest}

    async def event_log_stats(self) -> dict[str, Any]:
        async with AsyncSession(self.engine) as session:
            stmt = select(
                func.count(),
                func.min(EventLogRecord.created_at),
                func.max(EventLogRecord.created_at),
            )
            res = await session.exec(stmt)
            row = res.one()
            count = int(row[0] or 0)
            oldest = float(row[1]) if row[1] is not None else None
            newest = float(row[2]) if row[2] is not None else None
            return {"count": count, "oldest": oldest, "newest": newest}

    async def scheduler_events_stats(self) -> dict[str, Any]:
        return await self._table_stats(SchedulerEventRecord)

    async def pack_ingests_stats(self) -> dict[str, Any]:
        return await self._table_stats(PackIngestRecord, ts_column=PackIngestRecord.updated_at)

    async def sequence_meta_stats(self) -> dict[str, Any]:
        return await self._table_stats(SequenceMetaRecord, ts_column=SequenceMetaRecord.updated_at)

    async def audio_analyses_stats(self) -> dict[str, Any]:
        return await self._table_stats(
            AudioAnalysisRecord, ts_column=AudioAnalysisRecord.updated_at
        )

    async def show_configs_stats(self) -> dict[str, Any]:
        return await self._table_stats(ShowConfigRecord, ts_column=ShowConfigRecord.updated_at)

    async def fseq_exports_stats(self) -> dict[str, Any]:
        return await self._table_stats(FseqExportRecord, ts_column=FseqExportRecord.updated_at)

    async def fpp_scripts_stats(self) -> dict[str, Any]:
        return await self._table_stats(FppScriptRecord, ts_column=FppScriptRecord.updated_at)

    async def orchestration_runs_stats(self) -> dict[str, Any]:
        ts = func.coalesce(
            OrchestrationRunRecord.started_at, OrchestrationRunRecord.created_at
        )
        return await self._table_stats(OrchestrationRunRecord, ts_column=ts)

    async def agent_history_stats(self) -> dict[str, Any]:
        return await self._table_stats(AgentHeartbeatHistoryRecord)

    async def enforce_event_log_retention(
        self,
        *,
        max_rows: Optional[int],
        max_days: Optional[int],
        batch_size: int = 1000,
    ) -> Dict[str, Any]:
        max_rows_i = int(max_rows) if max_rows is not None else 0
        max_days_i = int(max_days) if max_days is not None else 0
        batch = max(1, min(10_000, int(batch_size)))
        deleted_by_days = 0
        deleted_by_rows = 0

        async with AsyncSession(self.engine) as session:
            if max_days_i > 0:
                cutoff = _now() - (max_days_i * 86400.0)
                stmt = delete(EventLogRecord).where(
                    EventLogRecord.created_at < float(cutoff)
                )
                res = await session.exec(stmt)  # type: ignore[arg-type]
                deleted_by_days = int(getattr(res, "rowcount", 0) or 0)

            if max_rows_i > 0:
                res = await session.exec(select(func.count()).select_from(EventLogRecord))
                total = int(res.one() or 0)
                excess = total - max_rows_i
                if excess > 0:
                    limit = min(excess, batch)
                    ids = (
                        await session.exec(
                            select(EventLogRecord.id)
                            .order_by(EventLogRecord.created_at.asc())
                            .limit(limit)
                        )
                    ).all()
                    ids_list = [int(x) for x in ids if x is not None]
                    if ids_list:
                        stmt_del = delete(EventLogRecord).where(
                            EventLogRecord.id.in_(ids_list)
                        )
                        res = await session.exec(stmt_del)  # type: ignore[arg-type]
                        deleted_by_rows = int(getattr(res, "rowcount", 0) or 0)

            await session.commit()

        return {
            "ok": True,
            "deleted_by_days": deleted_by_days,
            "deleted_by_rows": deleted_by_rows,
            "max_days": max_days_i,
            "max_rows": max_rows_i,
        }

    # ---- Metrics history ----

    async def add_metrics_sample(
        self,
        *,
        created_at: float,
        jobs_count: int,
        scheduler_ok: bool,
        scheduler_running: bool,
        scheduler_in_window: bool,
        outbound_failures: int,
        outbound_retries: int,
        spool_dropped: int,
        spool_queued_events: int,
        spool_queued_bytes: int,
        agent_id: str | None = None,
    ) -> int | None:
        aid = str(agent_id or self.agent_id).strip()
        if not aid:
            return None
        async with AsyncSession(self.engine) as session:
            rec = MetricsSampleRecord(
                agent_id=aid,
                created_at=float(created_at),
                jobs_count=int(jobs_count),
                scheduler_ok=bool(scheduler_ok),
                scheduler_running=bool(scheduler_running),
                scheduler_in_window=bool(scheduler_in_window),
                outbound_failures=int(outbound_failures),
                outbound_retries=int(outbound_retries),
                spool_dropped=int(spool_dropped),
                spool_queued_events=int(spool_queued_events),
                spool_queued_bytes=int(spool_queued_bytes),
            )
            session.add(rec)
            await session.commit()
            try:
                await session.refresh(rec)
            except Exception:
                pass
            return int(rec.id) if rec.id is not None else None

    async def list_metrics_samples(
        self,
        *,
        limit: int = 200,
        agent_id: str | None = None,
        since: float | None = None,
        until: float | None = None,
        offset: int = 0,
        order: str = "desc",
    ) -> list[dict[str, Any]]:
        lim = max(1, int(limit))
        aid = str(agent_id or self.agent_id).strip()
        since_val = float(since) if since is not None else None
        until_val = float(until) if until is not None else None
        off = max(0, int(offset))
        async with AsyncSession(self.engine) as session:
            stmt = select(MetricsSampleRecord)
            if aid:
                stmt = stmt.where(MetricsSampleRecord.agent_id == aid)
            if since_val is not None:
                stmt = stmt.where(MetricsSampleRecord.created_at >= float(since_val))
            if until_val is not None:
                stmt = stmt.where(MetricsSampleRecord.created_at <= float(until_val))
            if order.strip().lower() == "asc":
                stmt = stmt.order_by(MetricsSampleRecord.created_at.asc())
            else:
                stmt = stmt.order_by(MetricsSampleRecord.created_at.desc())
            if off:
                stmt = stmt.offset(off)
            stmt = stmt.limit(lim)
            rows = (await session.exec(stmt)).all()
            return [r.model_dump() for r in rows]

    async def metrics_history_stats(
        self,
        *,
        agent_id: str | None = None,
    ) -> dict[str, Any]:
        aid = str(agent_id or self.agent_id).strip()
        async with AsyncSession(self.engine) as session:
            stmt = select(
                func.count(),
                func.min(MetricsSampleRecord.created_at),
                func.max(MetricsSampleRecord.created_at),
            )
            if aid:
                stmt = stmt.where(MetricsSampleRecord.agent_id == aid)
            res = await session.exec(stmt)
            row = res.one()
            count = int(row[0] or 0)
            oldest = float(row[1]) if row[1] is not None else None
            newest = float(row[2]) if row[2] is not None else None
            return {"count": count, "oldest": oldest, "newest": newest}

    async def enforce_metrics_history_retention(
        self,
        *,
        max_rows: Optional[int],
        max_days: Optional[int],
        agent_id: str | None = None,
        batch_size: int = 1000,
    ) -> Dict[str, Any]:
        max_rows_i = int(max_rows) if max_rows is not None else 0
        max_days_i = int(max_days) if max_days is not None else 0
        batch = max(1, min(10_000, int(batch_size)))
        deleted_by_days = 0
        deleted_by_rows = 0
        aid = str(agent_id or self.agent_id).strip()

        async with AsyncSession(self.engine) as session:
            if max_days_i > 0:
                cutoff = _now() - (max_days_i * 86400.0)
                stmt = delete(MetricsSampleRecord).where(
                    MetricsSampleRecord.created_at < float(cutoff)
                )
                if aid:
                    stmt = stmt.where(MetricsSampleRecord.agent_id == aid)
                res = await session.exec(stmt)  # type: ignore[arg-type]
                deleted_by_days = int(getattr(res, "rowcount", 0) or 0)

            if max_rows_i > 0:
                stmt_count = select(func.count()).select_from(MetricsSampleRecord)
                if aid:
                    stmt_count = stmt_count.where(MetricsSampleRecord.agent_id == aid)
                res = await session.exec(stmt_count)
                total = int(res.one() or 0)
                excess = total - max_rows_i
                if excess > 0:
                    limit = min(excess, batch)
                    ids_stmt = (
                        select(MetricsSampleRecord.id)
                        .order_by(MetricsSampleRecord.created_at.asc())
                        .limit(limit)
                    )
                    if aid:
                        ids_stmt = ids_stmt.where(MetricsSampleRecord.agent_id == aid)
                    ids = (await session.exec(ids_stmt)).all()
                    ids_list = [int(x) for x in ids if x is not None]
                    if ids_list:
                        stmt_del = delete(MetricsSampleRecord).where(
                            MetricsSampleRecord.id.in_(ids_list)
                        )
                        res = await session.exec(stmt_del)  # type: ignore[arg-type]
                        deleted_by_rows = int(getattr(res, "rowcount", 0) or 0)

            await session.commit()

        return {
            "ok": True,
            "deleted_by_days": deleted_by_days,
            "deleted_by_rows": deleted_by_rows,
            "max_days": max_days_i,
            "max_rows": max_rows_i,
        }

    # ---- Orchestration run history ----

    async def add_orchestration_run(
        self,
        *,
        run_id: str,
        scope: str,
        name: str | None,
        steps_total: int,
        loop: bool,
        include_self: bool,
        payload: Dict[str, Any] | None = None,
        agent_id: str | None = None,
    ) -> None:
        rid = str(run_id).strip()
        if not rid:
            return
        aid = str(agent_id or self.agent_id).strip()
        now = _now()
        async with AsyncSession(self.engine) as session:
            rec = OrchestrationRunRecord(
                run_id=rid,
                agent_id=aid,
                created_at=now,
                updated_at=now,
                started_at=now,
                name=str(name) if name else None,
                scope=str(scope or "local"),
                status="running",
                steps_total=max(0, int(steps_total)),
                loop=bool(loop),
                include_self=bool(include_self),
                duration_s=0.0,
                payload=dict(payload or {}),
            )
            session.add(rec)
            await session.commit()

    async def update_orchestration_run(
        self,
        *,
        run_id: str,
        status: str,
        error: str | None = None,
        finished_at: float | None = None,
        payload: Dict[str, Any] | None = None,
    ) -> None:
        rid = str(run_id).strip()
        if not rid:
            return
        now = _now()
        async with AsyncSession(self.engine) as session:
            rec = await session.get(OrchestrationRunRecord, rid)
            if rec is None:
                return
            rec.updated_at = now
            rec.status = str(status or rec.status)
            if finished_at is not None:
                rec.finished_at = float(finished_at)
                rec.duration_s = max(
                    0.0, float(rec.finished_at) - float(rec.started_at or 0.0)
                )
            if error:
                rec.error = str(error)[:512]
            if payload is not None:
                rec.payload = dict(payload)
            await session.commit()

    async def get_orchestration_run(self, *, run_id: str) -> dict[str, Any] | None:
        rid = str(run_id).strip()
        if not rid:
            return None
        async with AsyncSession(self.engine) as session:
            rec = await session.get(OrchestrationRunRecord, rid)
            return rec.model_dump() if rec else None

    async def list_orchestration_runs(
        self,
        *,
        limit: int = 200,
        agent_id: str | None = None,
        scope: str | None = None,
        status: str | None = None,
        since: float | None = None,
        until: float | None = None,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        lim = max(1, int(limit))
        aid = str(agent_id).strip() if agent_id else ""
        sc = str(scope).strip() if scope else ""
        st = str(status).strip() if status else ""
        since_val = float(since) if since is not None else None
        until_val = float(until) if until is not None else None
        off = max(0, int(offset))
        async with AsyncSession(self.engine) as session:
            stmt = select(OrchestrationRunRecord).order_by(
                OrchestrationRunRecord.started_at.desc()
            )
            if aid:
                stmt = stmt.where(OrchestrationRunRecord.agent_id == aid)
            if sc:
                stmt = stmt.where(OrchestrationRunRecord.scope == sc)
            if st:
                stmt = stmt.where(OrchestrationRunRecord.status == st)
            if since_val is not None:
                stmt = stmt.where(OrchestrationRunRecord.started_at >= float(since_val))
            if until_val is not None:
                stmt = stmt.where(OrchestrationRunRecord.started_at <= float(until_val))
            if off:
                stmt = stmt.offset(off)
            stmt = stmt.limit(lim)
            rows = (await session.exec(stmt)).all()
            return [r.model_dump() for r in rows]

    async def add_orchestration_step(
        self,
        *,
        run_id: str,
        step_index: int,
        iteration: int = 0,
        kind: str,
        status: str,
        ok: bool,
        started_at: float,
        finished_at: float | None = None,
        error: str | None = None,
        payload: Dict[str, Any] | None = None,
        agent_id: str | None = None,
    ) -> None:
        rid = str(run_id).strip()
        if not rid:
            return
        aid = str(agent_id or self.agent_id).strip()
        now = _now()
        start = float(started_at or now)
        end = float(finished_at) if finished_at is not None else None
        duration = max(0.0, (end - start) if end is not None else 0.0)
        async with AsyncSession(self.engine) as session:
            rec = OrchestrationStepRecord(
                run_id=rid,
                agent_id=aid,
                created_at=now,
                updated_at=now,
                started_at=start,
                finished_at=end,
                step_index=int(step_index),
                iteration=max(0, int(iteration)),
                kind=str(kind or ""),
                status=str(status or "completed"),
                ok=bool(ok),
                duration_s=float(duration),
                error=str(error)[:512] if error else None,
                payload=dict(payload or {}),
            )
            session.add(rec)
            await session.commit()

    async def list_orchestration_steps(
        self,
        *,
        run_id: str,
        limit: int = 500,
        offset: int = 0,
        status: str | None = None,
        ok: bool | None = None,
    ) -> list[dict[str, Any]]:
        rid = str(run_id).strip()
        if not rid:
            return []
        lim = max(1, int(limit))
        off = max(0, int(offset))
        st = str(status).strip() if status else ""
        async with AsyncSession(self.engine) as session:
            stmt = (
                select(OrchestrationStepRecord)
                .where(OrchestrationStepRecord.run_id == rid)
                .order_by(OrchestrationStepRecord.created_at.asc())
            )
            if st:
                stmt = stmt.where(OrchestrationStepRecord.status == st)
            if ok is not None:
                stmt = stmt.where(OrchestrationStepRecord.ok == bool(ok))
            if off:
                stmt = stmt.offset(off)
            stmt = stmt.limit(lim)
            rows = (await session.exec(stmt)).all()
            return [r.model_dump() for r in rows]

    async def add_orchestration_peer_result(
        self,
        *,
        run_id: str,
        step_index: int,
        iteration: int = 0,
        peer_id: str,
        action: str,
        status: str,
        ok: bool,
        started_at: float,
        finished_at: float | None = None,
        error: str | None = None,
        payload: Dict[str, Any] | None = None,
        agent_id: str | None = None,
    ) -> None:
        rid = str(run_id).strip()
        if not rid:
            return
        aid = str(agent_id or self.agent_id).strip()
        pid = str(peer_id or "").strip()
        if not pid:
            return
        now = _now()
        start = float(started_at or now)
        end = float(finished_at) if finished_at is not None else None
        duration = max(0.0, (end - start) if end is not None else 0.0)
        async with AsyncSession(self.engine) as session:
            rec = OrchestrationPeerResultRecord(
                run_id=rid,
                agent_id=aid,
                peer_id=pid,
                created_at=now,
                updated_at=now,
                started_at=start,
                finished_at=end,
                step_index=int(step_index),
                iteration=max(0, int(iteration)),
                action=str(action or ""),
                status=str(status or "completed"),
                ok=bool(ok),
                duration_s=float(duration),
                error=str(error)[:512] if error else None,
                payload=dict(payload or {}),
            )
            session.add(rec)
            await session.commit()

    async def list_orchestration_peer_results(
        self,
        *,
        run_id: str,
        limit: int = 2000,
        offset: int = 0,
        status: str | None = None,
        ok: bool | None = None,
    ) -> list[dict[str, Any]]:
        rid = str(run_id).strip()
        if not rid:
            return []
        lim = max(1, int(limit))
        off = max(0, int(offset))
        st = str(status).strip() if status else ""
        async with AsyncSession(self.engine) as session:
            stmt = (
                select(OrchestrationPeerResultRecord)
                .where(OrchestrationPeerResultRecord.run_id == rid)
                .order_by(OrchestrationPeerResultRecord.created_at.asc())
            )
            if st:
                stmt = stmt.where(OrchestrationPeerResultRecord.status == st)
            if ok is not None:
                stmt = stmt.where(OrchestrationPeerResultRecord.ok == bool(ok))
            if off:
                stmt = stmt.offset(off)
            stmt = stmt.limit(lim)
            rows = (await session.exec(stmt)).all()
            return [r.model_dump() for r in rows]

    async def enforce_orchestration_runs_retention(
        self,
        *,
        max_rows: Optional[int],
        max_days: Optional[int],
        batch_size: int = 1000,
    ) -> Dict[str, Any]:
        max_rows_i = int(max_rows) if max_rows is not None else 0
        max_days_i = int(max_days) if max_days is not None else 0
        batch = max(1, min(10_000, int(batch_size)))
        deleted_by_days = 0
        deleted_by_rows = 0

        async with AsyncSession(self.engine) as session:
            if max_days_i > 0:
                cutoff = _now() - (max_days_i * 86400.0)
                try:
                    stmt_steps = delete(OrchestrationStepRecord).where(
                        OrchestrationStepRecord.created_at < float(cutoff)
                    )
                    await session.exec(stmt_steps)  # type: ignore[arg-type]
                except Exception:
                    pass
                try:
                    stmt_peers = delete(OrchestrationPeerResultRecord).where(
                        OrchestrationPeerResultRecord.created_at < float(cutoff)
                    )
                    await session.exec(stmt_peers)  # type: ignore[arg-type]
                except Exception:
                    pass
                stmt = delete(OrchestrationRunRecord).where(
                    OrchestrationRunRecord.started_at < float(cutoff)
                )
                res = await session.exec(stmt)  # type: ignore[arg-type]
                deleted_by_days = int(getattr(res, "rowcount", 0) or 0)

            if max_rows_i > 0:
                res = await session.exec(
                    select(func.count()).select_from(OrchestrationRunRecord)
                )
                total = int(res.one() or 0)
                excess = total - max_rows_i
                if excess > 0:
                    limit = min(excess, batch)
                    ids = (
                        await session.exec(
                            select(OrchestrationRunRecord.run_id)
                            .order_by(OrchestrationRunRecord.started_at.asc())
                            .limit(limit)
                        )
                    ).all()
                    ids_list = [str(x) for x in ids if x]
                    if ids_list:
                        try:
                            stmt_del_steps = delete(OrchestrationStepRecord).where(
                                OrchestrationStepRecord.run_id.in_(ids_list)
                            )
                            await session.exec(stmt_del_steps)  # type: ignore[arg-type]
                        except Exception:
                            pass
                        try:
                            stmt_del_peers = delete(OrchestrationPeerResultRecord).where(
                                OrchestrationPeerResultRecord.run_id.in_(ids_list)
                            )
                            await session.exec(stmt_del_peers)  # type: ignore[arg-type]
                        except Exception:
                            pass
                        stmt_del = delete(OrchestrationRunRecord).where(
                            OrchestrationRunRecord.run_id.in_(ids_list)
                        )
                        res = await session.exec(stmt_del)  # type: ignore[arg-type]
                        deleted_by_rows = int(getattr(res, "rowcount", 0) or 0)

            await session.commit()

        return {
            "ok": True,
            "deleted_by_days": deleted_by_days,
            "deleted_by_rows": deleted_by_rows,
            "max_days": max_days_i,
            "max_rows": max_rows_i,
        }

    # ---- Jobs persistence ----

    async def list_jobs(self, *, limit: int) -> list[dict[str, Any]]:
        lim = max(1, int(limit))
        async with AsyncSession(self.engine) as session:
            stmt = (
                select(JobRecord)
                .where(JobRecord.agent_id == self.agent_id)
                .order_by(JobRecord.created_at.desc())
                .limit(lim)
            )
            rows = (await session.exec(stmt)).all()
            return [dict(r.payload or {}) for r in rows]

    async def job_stats(self, *, agent_id: str | None = None) -> dict[str, Any]:
        aid = str(agent_id or self.agent_id).strip()
        async with AsyncSession(self.engine) as session:
            stmt = select(
                func.count(),
                func.min(JobRecord.created_at),
                func.max(JobRecord.created_at),
            )
            if aid:
                stmt = stmt.where(JobRecord.agent_id == aid)
            res = await session.exec(stmt)
            row = res.one()
            count = int(row[0] or 0)
            oldest = float(row[1]) if row[1] is not None else None
            newest = float(row[2]) if row[2] is not None else None
            return {"count": count, "oldest": oldest, "newest": newest}

    async def get_job(self, job_id: str) -> dict[str, Any] | None:
        jid = str(job_id)
        async with AsyncSession(self.engine) as session:
            rec = await session.get(JobRecord, (self.agent_id, jid))
            return dict(rec.payload or {}) if rec else None

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
        async with AsyncSession(self.engine) as session:
            rec = await session.get(JobRecord, (self.agent_id, jid))
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
            await session.commit()

    async def mark_in_flight_failed(self, *, reason: str = "Server restarted") -> int:
        now = _now()
        updated = 0
        async with AsyncSession(self.engine) as session:
            stmt = select(JobRecord).where(
                JobRecord.agent_id == self.agent_id,
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

    # ---- KV persistence ----

    async def kv_get_json(self, key: str) -> dict[str, Any] | None:
        k = str(key)
        async with AsyncSession(self.engine) as session:
            rec = await session.get(KVRecord, (self.agent_id, k))
            return dict(rec.value or {}) if rec else None

    async def global_kv_get_json(self, key: str) -> dict[str, Any] | None:
        k = str(key)
        async with AsyncSession(self.engine) as session:
            rec = await session.get(GlobalKVRecord, k)
            return dict(rec.value or {}) if rec else None

    async def kv_set_json(self, key: str, value: dict[str, Any]) -> None:
        now = _now()
        k = str(key)
        async with AsyncSession(self.engine) as session:
            rec = await session.get(KVRecord, (self.agent_id, k))
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
            await session.commit()

    async def global_kv_set_json(self, key: str, value: dict[str, Any]) -> None:
        now = _now()
        k = str(key)
        async with AsyncSession(self.engine) as session:
            rec = await session.get(GlobalKVRecord, k)
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
            await session.commit()

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

        async with AsyncSession(self.engine) as session:
            if max_days_i > 0:
                cutoff = _now() - (max_days_i * 86400.0)
                stmt = delete(JobRecord).where(
                    JobRecord.agent_id == agent_id,
                    JobRecord.created_at < float(cutoff),
                )
                res = await session.exec(stmt)  # type: ignore[arg-type]
                deleted_by_days = int(getattr(res, "rowcount", 0) or 0)

            if max_rows_i > 0:
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
                        JobRecord.agent_id == agent_id,
                        JobRecord.id.in_(ids_list),
                    )
                    res = await session.exec(stmt_del)  # type: ignore[arg-type]
                    deleted_by_rows = int(getattr(res, "rowcount", 0) or 0)

            await session.commit()

        return {
            "ok": True,
            "deleted_by_days": deleted_by_days,
            "deleted_by_rows": deleted_by_rows,
        }

    # ---- Metadata retention ----

    async def enforce_pack_ingests_retention(
        self,
        *,
        max_rows: Optional[int],
        max_days: Optional[int],
    ) -> Dict[str, Any]:
        """
        Best-effort retention for PackIngestRecord (per agent_id).
        """
        agent_id = self.agent_id
        max_rows_i = int(max_rows) if max_rows is not None else 0
        max_days_i = int(max_days) if max_days is not None else 0
        deleted_by_days = 0
        deleted_by_rows = 0

        async with AsyncSession(self.engine) as session:
            if max_days_i > 0:
                cutoff = _now() - (max_days_i * 86400.0)
                stmt = delete(PackIngestRecord).where(
                    PackIngestRecord.agent_id == agent_id,
                    PackIngestRecord.updated_at < float(cutoff),
                )
                res = await session.exec(stmt)  # type: ignore[arg-type]
                deleted_by_days = int(getattr(res, "rowcount", 0) or 0)

            if max_rows_i > 0:
                stmt_ids = (
                    select(PackIngestRecord.dest_dir)
                    .where(PackIngestRecord.agent_id == agent_id)
                    .order_by(PackIngestRecord.updated_at.desc())
                    .offset(max_rows_i)
                )
                ids = (await session.exec(stmt_ids)).all()
                ids_list = [str(x) for x in ids if x]
                if ids_list:
                    stmt_del = delete(PackIngestRecord).where(
                        PackIngestRecord.agent_id == agent_id,
                        PackIngestRecord.dest_dir.in_(ids_list),
                    )
                    res = await session.exec(stmt_del)  # type: ignore[arg-type]
                    deleted_by_rows = int(getattr(res, "rowcount", 0) or 0)

            await session.commit()

        return {
            "ok": True,
            "deleted_by_days": deleted_by_days,
            "deleted_by_rows": deleted_by_rows,
        }

    async def enforce_sequence_meta_retention(
        self,
        *,
        max_rows: Optional[int],
        max_days: Optional[int],
    ) -> Dict[str, Any]:
        """
        Best-effort retention for SequenceMetaRecord (per agent_id).
        """
        agent_id = self.agent_id
        max_rows_i = int(max_rows) if max_rows is not None else 0
        max_days_i = int(max_days) if max_days is not None else 0
        deleted_by_days = 0
        deleted_by_rows = 0

        async with AsyncSession(self.engine) as session:
            if max_days_i > 0:
                cutoff = _now() - (max_days_i * 86400.0)
                stmt = delete(SequenceMetaRecord).where(
                    SequenceMetaRecord.agent_id == agent_id,
                    SequenceMetaRecord.updated_at < float(cutoff),
                )
                res = await session.exec(stmt)  # type: ignore[arg-type]
                deleted_by_days = int(getattr(res, "rowcount", 0) or 0)

            if max_rows_i > 0:
                stmt_ids = (
                    select(SequenceMetaRecord.file)
                    .where(SequenceMetaRecord.agent_id == agent_id)
                    .order_by(SequenceMetaRecord.updated_at.desc())
                    .offset(max_rows_i)
                )
                ids = (await session.exec(stmt_ids)).all()
                ids_list = [str(x) for x in ids if x]
                if ids_list:
                    stmt_del = delete(SequenceMetaRecord).where(
                        SequenceMetaRecord.agent_id == agent_id,
                        SequenceMetaRecord.file.in_(ids_list),
                    )
                    res = await session.exec(stmt_del)  # type: ignore[arg-type]
                    deleted_by_rows = int(getattr(res, "rowcount", 0) or 0)

            await session.commit()

        return {
            "ok": True,
            "deleted_by_days": deleted_by_days,
            "deleted_by_rows": deleted_by_rows,
        }

    async def enforce_audio_analyses_retention(
        self,
        *,
        max_rows: Optional[int],
        max_days: Optional[int],
    ) -> Dict[str, Any]:
        """
        Best-effort retention for AudioAnalysisRecord (per agent_id).
        """
        agent_id = self.agent_id
        max_rows_i = int(max_rows) if max_rows is not None else 0
        max_days_i = int(max_days) if max_days is not None else 0
        deleted_by_days = 0
        deleted_by_rows = 0

        async with AsyncSession(self.engine) as session:
            if max_days_i > 0:
                cutoff = _now() - (max_days_i * 86400.0)
                stmt = delete(AudioAnalysisRecord).where(
                    AudioAnalysisRecord.agent_id == agent_id,
                    AudioAnalysisRecord.created_at < float(cutoff),
                )
                res = await session.exec(stmt)  # type: ignore[arg-type]
                deleted_by_days = int(getattr(res, "rowcount", 0) or 0)

            if max_rows_i > 0:
                stmt_ids = (
                    select(AudioAnalysisRecord.id)
                    .where(AudioAnalysisRecord.agent_id == agent_id)
                    .order_by(AudioAnalysisRecord.created_at.desc())
                    .offset(max_rows_i)
                )
                ids = (await session.exec(stmt_ids)).all()
                ids_list = [str(x) for x in ids if x]
                if ids_list:
                    stmt_del = delete(AudioAnalysisRecord).where(
                        AudioAnalysisRecord.agent_id == agent_id,
                        AudioAnalysisRecord.id.in_(ids_list),
                    )
                    res = await session.exec(stmt_del)  # type: ignore[arg-type]
                    deleted_by_rows = int(getattr(res, "rowcount", 0) or 0)

            await session.commit()

        return {
            "ok": True,
            "deleted_by_days": deleted_by_days,
            "deleted_by_rows": deleted_by_rows,
        }

    async def enforce_show_configs_retention(
        self,
        *,
        max_rows: Optional[int],
        max_days: Optional[int],
    ) -> Dict[str, Any]:
        """
        Best-effort retention for ShowConfigRecord (per agent_id).
        """
        agent_id = self.agent_id
        max_rows_i = int(max_rows) if max_rows is not None else 0
        max_days_i = int(max_days) if max_days is not None else 0
        deleted_by_days = 0
        deleted_by_rows = 0

        async with AsyncSession(self.engine) as session:
            if max_days_i > 0:
                cutoff = _now() - (max_days_i * 86400.0)
                stmt = delete(ShowConfigRecord).where(
                    ShowConfigRecord.agent_id == agent_id,
                    ShowConfigRecord.updated_at < float(cutoff),
                )
                res = await session.exec(stmt)  # type: ignore[arg-type]
                deleted_by_days = int(getattr(res, "rowcount", 0) or 0)

            if max_rows_i > 0:
                stmt_ids = (
                    select(ShowConfigRecord.file)
                    .where(ShowConfigRecord.agent_id == agent_id)
                    .order_by(ShowConfigRecord.updated_at.desc())
                    .offset(max_rows_i)
                )
                ids = (await session.exec(stmt_ids)).all()
                ids_list = [str(x) for x in ids if x]
                if ids_list:
                    stmt_del = delete(ShowConfigRecord).where(
                        ShowConfigRecord.agent_id == agent_id,
                        ShowConfigRecord.file.in_(ids_list),
                    )
                    res = await session.exec(stmt_del)  # type: ignore[arg-type]
                    deleted_by_rows = int(getattr(res, "rowcount", 0) or 0)

            await session.commit()

        return {
            "ok": True,
            "deleted_by_days": deleted_by_days,
            "deleted_by_rows": deleted_by_rows,
        }

    async def enforce_fseq_exports_retention(
        self,
        *,
        max_rows: Optional[int],
        max_days: Optional[int],
    ) -> Dict[str, Any]:
        """
        Best-effort retention for FseqExportRecord (per agent_id).
        """
        agent_id = self.agent_id
        max_rows_i = int(max_rows) if max_rows is not None else 0
        max_days_i = int(max_days) if max_days is not None else 0
        deleted_by_days = 0
        deleted_by_rows = 0

        async with AsyncSession(self.engine) as session:
            if max_days_i > 0:
                cutoff = _now() - (max_days_i * 86400.0)
                stmt = delete(FseqExportRecord).where(
                    FseqExportRecord.agent_id == agent_id,
                    FseqExportRecord.updated_at < float(cutoff),
                )
                res = await session.exec(stmt)  # type: ignore[arg-type]
                deleted_by_days = int(getattr(res, "rowcount", 0) or 0)

            if max_rows_i > 0:
                stmt_ids = (
                    select(FseqExportRecord.file)
                    .where(FseqExportRecord.agent_id == agent_id)
                    .order_by(FseqExportRecord.updated_at.desc())
                    .offset(max_rows_i)
                )
                ids = (await session.exec(stmt_ids)).all()
                ids_list = [str(x) for x in ids if x]
                if ids_list:
                    stmt_del = delete(FseqExportRecord).where(
                        FseqExportRecord.agent_id == agent_id,
                        FseqExportRecord.file.in_(ids_list),
                    )
                    res = await session.exec(stmt_del)  # type: ignore[arg-type]
                    deleted_by_rows = int(getattr(res, "rowcount", 0) or 0)

            await session.commit()

        return {
            "ok": True,
            "deleted_by_days": deleted_by_days,
            "deleted_by_rows": deleted_by_rows,
        }

    async def enforce_fpp_scripts_retention(
        self,
        *,
        max_rows: Optional[int],
        max_days: Optional[int],
    ) -> Dict[str, Any]:
        """
        Best-effort retention for FppScriptRecord (per agent_id).
        """
        agent_id = self.agent_id
        max_rows_i = int(max_rows) if max_rows is not None else 0
        max_days_i = int(max_days) if max_days is not None else 0
        deleted_by_days = 0
        deleted_by_rows = 0

        async with AsyncSession(self.engine) as session:
            if max_days_i > 0:
                cutoff = _now() - (max_days_i * 86400.0)
                stmt = delete(FppScriptRecord).where(
                    FppScriptRecord.agent_id == agent_id,
                    FppScriptRecord.updated_at < float(cutoff),
                )
                res = await session.exec(stmt)  # type: ignore[arg-type]
                deleted_by_days = int(getattr(res, "rowcount", 0) or 0)

            if max_rows_i > 0:
                stmt_ids = (
                    select(FppScriptRecord.file)
                    .where(FppScriptRecord.agent_id == agent_id)
                    .order_by(FppScriptRecord.updated_at.desc())
                    .offset(max_rows_i)
                )
                ids = (await session.exec(stmt_ids)).all()
                ids_list = [str(x) for x in ids if x]
                if ids_list:
                    stmt_del = delete(FppScriptRecord).where(
                        FppScriptRecord.agent_id == agent_id,
                        FppScriptRecord.file.in_(ids_list),
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
        async with AsyncSession(self.engine) as session:
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
        async with AsyncSession(self.engine) as session:
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

    async def upsert_show_config(
        self,
        *,
        file: str,
        name: str,
        props_total: int,
        groups_total: int,
        coordinator_base_url: str | None,
        fpp_base_url: str | None,
        payload: Dict[str, Any] | None = None,
    ) -> None:
        now = _now()
        key = (self.agent_id, str(file))
        async with AsyncSession(self.engine) as session:
            rec = await session.get(ShowConfigRecord, key)
            if rec is None:
                rec = ShowConfigRecord(
                    agent_id=self.agent_id,
                    file=str(file),
                    created_at=now,
                    updated_at=now,
                    name=str(name or ""),
                    props_total=int(props_total),
                    groups_total=int(groups_total),
                    coordinator_base_url=(
                        str(coordinator_base_url) if coordinator_base_url else None
                    ),
                    fpp_base_url=str(fpp_base_url) if fpp_base_url else None,
                    payload=dict(payload or {}),
                )
                session.add(rec)
            else:
                rec.updated_at = now
                rec.name = str(name or "")
                rec.props_total = int(props_total)
                rec.groups_total = int(groups_total)
                rec.coordinator_base_url = (
                    str(coordinator_base_url) if coordinator_base_url else None
                )
                rec.fpp_base_url = str(fpp_base_url) if fpp_base_url else None
                rec.payload = dict(payload or {})
            await session.commit()

    async def upsert_fseq_export(
        self,
        *,
        file: str,
        source_sequence: str | None,
        bytes_written: int,
        frames: int | None,
        channels: int | None,
        step_ms: int | None,
        duration_s: float | None,
        payload: Dict[str, Any] | None = None,
    ) -> None:
        now = _now()
        key = (self.agent_id, str(file))
        async with AsyncSession(self.engine) as session:
            rec = await session.get(FseqExportRecord, key)
            if rec is None:
                rec = FseqExportRecord(
                    agent_id=self.agent_id,
                    file=str(file),
                    created_at=now,
                    updated_at=now,
                    source_sequence=str(source_sequence) if source_sequence else None,
                    bytes_written=int(bytes_written),
                    frames=int(frames) if frames is not None else None,
                    channels=int(channels) if channels is not None else None,
                    step_ms=int(step_ms) if step_ms is not None else None,
                    duration_s=float(duration_s) if duration_s is not None else None,
                    payload=dict(payload or {}),
                )
                session.add(rec)
            else:
                rec.updated_at = now
                rec.source_sequence = (
                    str(source_sequence) if source_sequence else rec.source_sequence
                )
                rec.bytes_written = int(bytes_written)
                rec.frames = int(frames) if frames is not None else rec.frames
                rec.channels = int(channels) if channels is not None else rec.channels
                rec.step_ms = int(step_ms) if step_ms is not None else rec.step_ms
                rec.duration_s = (
                    float(duration_s) if duration_s is not None else rec.duration_s
                )
                rec.payload = dict(payload or {})
            await session.commit()

    async def upsert_fpp_script(
        self,
        *,
        file: str,
        kind: str,
        bytes_written: int,
        payload: Dict[str, Any] | None = None,
    ) -> None:
        now = _now()
        key = (self.agent_id, str(file))
        async with AsyncSession(self.engine) as session:
            rec = await session.get(FppScriptRecord, key)
            if rec is None:
                rec = FppScriptRecord(
                    agent_id=self.agent_id,
                    file=str(file),
                    created_at=now,
                    updated_at=now,
                    kind=str(kind or ""),
                    bytes_written=int(bytes_written),
                    payload=dict(payload or {}),
                )
                session.add(rec)
            else:
                rec.updated_at = now
                rec.kind = str(kind or rec.kind or "")
                rec.bytes_written = int(bytes_written)
                rec.payload = dict(payload or {})
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
        async with AsyncSession(self.engine) as session:
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
        async with AsyncSession(self.engine) as session:
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

    # ---- SQL-backed metadata listing (UI) ----

    async def list_pack_ingests(self, *, limit: int = 200) -> list[dict[str, Any]]:
        lim = max(1, int(limit))
        async with AsyncSession(self.engine) as session:
            stmt = (
                select(PackIngestRecord)
                .where(PackIngestRecord.agent_id == self.agent_id)
                .order_by(PackIngestRecord.updated_at.desc())
                .limit(lim)
            )
            rows = (await session.exec(stmt)).all()
            return [r.model_dump() for r in rows]

    async def list_sequence_meta(self, *, limit: int = 500) -> list[dict[str, Any]]:
        lim = max(1, int(limit))
        async with AsyncSession(self.engine) as session:
            stmt = (
                select(SequenceMetaRecord)
                .where(SequenceMetaRecord.agent_id == self.agent_id)
                .order_by(SequenceMetaRecord.updated_at.desc())
                .limit(lim)
            )
            rows = (await session.exec(stmt)).all()
            return [r.model_dump() for r in rows]

    async def list_audio_analyses(self, *, limit: int = 200) -> list[dict[str, Any]]:
        lim = max(1, int(limit))
        async with AsyncSession(self.engine) as session:
            stmt = (
                select(AudioAnalysisRecord)
                .where(AudioAnalysisRecord.agent_id == self.agent_id)
                .order_by(AudioAnalysisRecord.created_at.desc())
                .limit(lim)
            )
            rows = (await session.exec(stmt)).all()
            return [r.model_dump() for r in rows]

    async def list_last_applied(self) -> list[dict[str, Any]]:
        async with AsyncSession(self.engine) as session:
            stmt = (
                select(LastAppliedRecord)
                .where(LastAppliedRecord.agent_id == self.agent_id)
                .order_by(LastAppliedRecord.updated_at.desc())
            )
            rows = (await session.exec(stmt)).all()
            return [r.model_dump() for r in rows]

    async def list_show_configs(self, *, limit: int = 200) -> list[dict[str, Any]]:
        lim = max(1, int(limit))
        async with AsyncSession(self.engine) as session:
            stmt = (
                select(ShowConfigRecord)
                .where(ShowConfigRecord.agent_id == self.agent_id)
                .order_by(ShowConfigRecord.updated_at.desc())
                .limit(lim)
            )
            rows = (await session.exec(stmt)).all()
            return [r.model_dump() for r in rows]

    async def list_fseq_exports(self, *, limit: int = 200) -> list[dict[str, Any]]:
        lim = max(1, int(limit))
        async with AsyncSession(self.engine) as session:
            stmt = (
                select(FseqExportRecord)
                .where(FseqExportRecord.agent_id == self.agent_id)
                .order_by(FseqExportRecord.updated_at.desc())
                .limit(lim)
            )
            rows = (await session.exec(stmt)).all()
            return [r.model_dump() for r in rows]

    async def list_fpp_scripts(self, *, limit: int = 200) -> list[dict[str, Any]]:
        lim = max(1, int(limit))
        async with AsyncSession(self.engine) as session:
            stmt = (
                select(FppScriptRecord)
                .where(FppScriptRecord.agent_id == self.agent_id)
                .order_by(FppScriptRecord.updated_at.desc())
                .limit(lim)
            )
            rows = (await session.exec(stmt)).all()
            return [r.model_dump() for r in rows]

    # ---- SQL-backed metadata deletes (on file removal) ----

    async def delete_sequence_meta(self, *, file: str) -> bool:
        async with AsyncSession(self.engine) as session:
            stmt = delete(SequenceMetaRecord).where(
                SequenceMetaRecord.agent_id == self.agent_id,
                SequenceMetaRecord.file == str(file),
            )
            res = await session.exec(stmt)  # type: ignore[arg-type]
            await session.commit()
            return bool(int(getattr(res, "rowcount", 0) or 0))

    async def delete_show_config(self, *, file: str) -> bool:
        async with AsyncSession(self.engine) as session:
            stmt = delete(ShowConfigRecord).where(
                ShowConfigRecord.agent_id == self.agent_id,
                ShowConfigRecord.file == str(file),
            )
            res = await session.exec(stmt)  # type: ignore[arg-type]
            await session.commit()
            return bool(int(getattr(res, "rowcount", 0) or 0))

    async def delete_fseq_export(self, *, file: str) -> bool:
        async with AsyncSession(self.engine) as session:
            stmt = delete(FseqExportRecord).where(
                FseqExportRecord.agent_id == self.agent_id,
                FseqExportRecord.file == str(file),
            )
            res = await session.exec(stmt)  # type: ignore[arg-type]
            await session.commit()
            return bool(int(getattr(res, "rowcount", 0) or 0))

    async def delete_fpp_script(self, *, file: str) -> bool:
        async with AsyncSession(self.engine) as session:
            stmt = delete(FppScriptRecord).where(
                FppScriptRecord.agent_id == self.agent_id,
                FppScriptRecord.file == str(file),
            )
            res = await session.exec(stmt)  # type: ignore[arg-type]
            await session.commit()
            return bool(int(getattr(res, "rowcount", 0) or 0))

    async def delete_audio_analysis_by_beats_path(self, *, beats_path: str) -> bool:
        async with AsyncSession(self.engine) as session:
            stmt = delete(AudioAnalysisRecord).where(
                AudioAnalysisRecord.agent_id == self.agent_id,
                AudioAnalysisRecord.beats_path == str(beats_path),
            )
            res = await session.exec(stmt)  # type: ignore[arg-type]
            await session.commit()
            return bool(int(getattr(res, "rowcount", 0) or 0))

    async def delete_pack_ingest(self, *, dest_dir: str) -> bool:
        async with AsyncSession(self.engine) as session:
            stmt = delete(PackIngestRecord).where(
                PackIngestRecord.agent_id == self.agent_id,
                PackIngestRecord.dest_dir == str(dest_dir),
            )
            res = await session.exec(stmt)  # type: ignore[arg-type]
            await session.commit()
            return bool(int(getattr(res, "rowcount", 0) or 0))
