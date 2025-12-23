from __future__ import annotations

import asyncio
import time
from pathlib import Path
from typing import Any, Dict, Optional

from aiofiles import os as aio_os
from fastapi import Depends, HTTPException

from pack_io import read_json_async, write_json_async
from services.auth_service import require_a2a_auth
from services.state import AppState, get_state


async def persist_runtime_state(
    state: AppState, event: str, extra: Optional[Dict[str, Any]] = None
) -> None:
    """
    Best-effort runtime state snapshot under DATA_DIR so the UI can show "what was running"
    even after a restart.
    """
    try:
        path_s = str(getattr(state, "runtime_state_path", "") or "")
        if not path_s:
            return

        out: Dict[str, Any] = {
            "ok": True,
            "updated_at": time.time(),
            "event": str(event),
            "extra": dict(extra or {}),
        }

        try:
            ddp = getattr(state, "ddp", None)
            out["ddp"] = (await ddp.status()).__dict__ if ddp is not None else None
        except Exception:
            out["ddp"] = None

        try:
            seq = getattr(state, "sequences", None)
            out["sequence"] = (await seq.status()).__dict__ if seq is not None else None
        except Exception:
            out["sequence"] = None

        try:
            fleet = getattr(state, "fleet_sequences", None)
            if fleet is not None:
                out["fleet_sequence"] = (await fleet.status()).__dict__
        except Exception:
            pass

        # Persist to DB KV (if configured).
        db = getattr(state, "db", None)
        if db is not None:
            try:
                key = str(getattr(state, "kv_runtime_state_key", "runtime_state") or "")
                if key:
                    await db.kv_set_json(key, dict(out))
            except Exception:
                pass
        else:
            # Persist to filesystem only when DB is unavailable.
            p = Path(path_s)
            try:
                p.parent.mkdir(parents=True, exist_ok=True)
            except Exception:
                pass
            await write_json_async(str(p), out)
    except Exception:
        return


async def runtime_state(
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    try:
        db = getattr(state, "db", None)
        if db is not None:
            try:
                key = str(getattr(state, "kv_runtime_state_key", "runtime_state") or "")
                if key:
                    row = await db.kv_get_json(key)
                    if row:
                        return dict(row)
            except Exception:
                pass
            return {"ok": True, "exists": False}

        p = Path(str(getattr(state, "runtime_state_path", "") or ""))
        if not await aio_os.path.isfile(str(p)):
            return {"ok": True, "exists": False}
        return await read_json_async(str(p))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
