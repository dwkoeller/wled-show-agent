from __future__ import annotations

from typing import Any, Dict

from fastapi import Depends, HTTPException
from sqlmodel import select

from services.auth_service import require_a2a_auth
from services.reconcile_service import reconcile_data_dir
from services.state import AppState, get_state
from sql_store import (
    AudioAnalysisRecord,
    LastAppliedRecord,
    PackIngestRecord,
    SequenceMetaRecord,
)


def _clamp_limit(limit: int, *, default: int = 200, max_limit: int = 2000) -> int:
    try:
        n = int(limit)
    except Exception:
        n = default
    return max(1, min(int(max_limit), n))


def _require_db(state: AppState):
    db = getattr(state, "db", None)
    if db is None:
        raise HTTPException(status_code=503, detail="DATABASE_URL is not configured")
    return db


async def meta_packs(
    limit: int = 200,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    db = _require_db(state)
    lim = _clamp_limit(limit)
    async with db.sessionmaker() as session:
        stmt = (
            select(PackIngestRecord)
            .where(PackIngestRecord.agent_id == db.agent_id)
            .order_by(PackIngestRecord.updated_at.desc())
            .limit(lim)
        )
        rows = (await session.exec(stmt)).all()
    return {"ok": True, "packs": [r.model_dump() for r in rows]}


async def meta_sequences(
    limit: int = 500,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    db = _require_db(state)
    lim = _clamp_limit(limit, default=500)
    async with db.sessionmaker() as session:
        stmt = (
            select(SequenceMetaRecord)
            .where(SequenceMetaRecord.agent_id == db.agent_id)
            .order_by(SequenceMetaRecord.updated_at.desc())
            .limit(lim)
        )
        rows = (await session.exec(stmt)).all()
    return {"ok": True, "sequences": [r.model_dump() for r in rows]}


async def meta_audio_analyses(
    limit: int = 200,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    db = _require_db(state)
    lim = _clamp_limit(limit)
    async with db.sessionmaker() as session:
        stmt = (
            select(AudioAnalysisRecord)
            .where(AudioAnalysisRecord.agent_id == db.agent_id)
            .order_by(AudioAnalysisRecord.created_at.desc())
            .limit(lim)
        )
        rows = (await session.exec(stmt)).all()
    return {"ok": True, "audio_analyses": [r.model_dump() for r in rows]}


async def meta_last_applied(
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    db = _require_db(state)
    async with db.sessionmaker() as session:
        stmt = (
            select(LastAppliedRecord)
            .where(LastAppliedRecord.agent_id == db.agent_id)
            .order_by(LastAppliedRecord.updated_at.desc())
        )
        rows = (await session.exec(stmt)).all()
    out: Dict[str, Any] = {}
    for r in rows:
        out[str(r.kind)] = r.model_dump()
    return {"ok": True, "last_applied": out}


async def meta_reconcile(
    packs: bool = True,
    sequences: bool = True,
    audio: bool = False,
    scan_limit: int = 5000,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    return await reconcile_data_dir(
        state,
        packs=bool(packs),
        sequences=bool(sequences),
        audio=bool(audio),
        scan_limit=int(scan_limit),
    )
