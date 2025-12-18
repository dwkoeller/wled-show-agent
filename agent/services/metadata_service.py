from __future__ import annotations

from typing import Any, Dict

from fastapi import Depends, HTTPException

from services.auth_service import require_a2a_auth
from services.reconcile_service import reconcile_data_dir
from services.state import AppState, get_state


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
    packs = await db.list_pack_ingests(limit=lim)
    return {"ok": True, "packs": packs}


async def meta_sequences(
    limit: int = 500,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    db = _require_db(state)
    lim = _clamp_limit(limit, default=500)
    sequences = await db.list_sequence_meta(limit=lim)
    return {"ok": True, "sequences": sequences}


async def meta_audio_analyses(
    limit: int = 200,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    db = _require_db(state)
    lim = _clamp_limit(limit)
    audio_analyses = await db.list_audio_analyses(limit=lim)
    return {"ok": True, "audio_analyses": audio_analyses}


async def meta_last_applied(
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    db = _require_db(state)
    rows = await db.list_last_applied()
    out: Dict[str, Any] = {}
    for r in rows:
        if isinstance(r, dict) and "kind" in r:
            out[str(r.get("kind"))] = dict(r)
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
