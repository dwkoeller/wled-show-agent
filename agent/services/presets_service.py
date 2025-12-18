from __future__ import annotations

import asyncio
import os
from typing import Any, Dict

from fastapi import Depends, HTTPException

from models.requests import ImportPresetsRequest
from services.auth_service import require_a2a_auth
from services.state import AppState, get_state


def _require_importer(state: AppState):
    imp = getattr(state, "importer", None)
    if imp is None:
        raise HTTPException(status_code=503, detail="Preset importer not initialized")
    return imp


async def presets_import(
    req: ImportPresetsRequest,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    try:
        imp = _require_importer(state)
        pack_path = os.path.join(state.settings.data_dir, "looks", req.pack_file)
        res = await asyncio.to_thread(
            imp.import_from_pack,
            pack_path=pack_path,
            start_id=req.start_id,
            limit=req.limit,
            name_prefix=req.name_prefix,
            include_brightness=req.include_brightness,
            save_bounds=req.save_bounds,
        )
        return {"ok": True, "result": res.__dict__}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
