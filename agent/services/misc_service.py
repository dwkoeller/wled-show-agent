from __future__ import annotations

import asyncio
import os
from typing import Any, Dict, List

from fastapi import Depends, HTTPException

from models.requests import GoCrazyRequest
from pack_io import read_jsonl
from services.auth_service import require_a2a_auth
from services.state import AppState, get_state


async def _available_ddp_patterns(state: AppState) -> List[str]:
    from patterns import PatternFactory

    ddp = getattr(state, "ddp", None)
    if ddp is None:
        return []
    info = await state.wled.device_info()
    layout = None
    try:
        from segment_layout import fetch_segment_layout

        layout = await asyncio.to_thread(
            fetch_segment_layout,
            state.wled_sync,
            segment_ids=list(state.segment_ids or []),
            refresh=False,
        )
    except Exception:
        layout = None

    factory = PatternFactory(
        led_count=int(info.led_count),
        geometry=ddp.geometry,
        segment_layout=layout,
    )
    return list(factory.available())


async def go_crazy(
    req: GoCrazyRequest,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    try:
        looks = getattr(state, "looks", None)
        seq = getattr(state, "sequences", None)
        imp = getattr(state, "importer", None)
        if looks is None or seq is None or imp is None:
            raise HTTPException(status_code=503, detail="Service not initialized")

        # 1) generate looks pack
        summary = await asyncio.to_thread(
            looks.generate_pack,
            total_looks=req.total_looks,
            themes=req.themes,
            brightness=min(state.settings.wled_max_bri, req.brightness),
            seed=req.seed,
            write_files=req.write_files,
            include_multi_segment=True,
        )

        results: Dict[str, Any] = {"looks_pack": summary.__dict__}

        # 2) generate sequences
        seq_files: List[str] = []
        if req.sequences > 0:
            pack_path = os.path.join(state.settings.data_dir, "looks", summary.file)
            looks_rows = await asyncio.to_thread(read_jsonl, pack_path)
            if len(looks_rows) > 2000:
                looks_rows = looks_rows[:2000]
            ddp_pats = await _available_ddp_patterns(state)
            for i in range(req.sequences):
                fname = await asyncio.to_thread(
                    seq.generate,
                    name=f"{i+1:02d}_Mix",
                    looks=looks_rows,
                    duration_s=req.sequence_duration_s,
                    step_s=req.step_s,
                    include_ddp=req.include_ddp,
                    renderable_only=False,
                    ddp_patterns=ddp_pats,
                    seed=req.seed + i,
                )
                seq_files.append(fname)
        results["sequences"] = seq_files

        # 3) optional preset import
        if req.import_presets:
            pack_path = os.path.join(state.settings.data_dir, "looks", summary.file)
            res = await asyncio.to_thread(
                imp.import_from_pack,
                pack_path=pack_path,
                start_id=req.import_start_id,
                limit=req.import_limit,
                name_prefix="AI",
                include_brightness=True,
                save_bounds=True,
            )
            results["preset_import"] = res.__dict__

        return {"ok": True, "result": results}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
