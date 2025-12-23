from __future__ import annotations

import time
from typing import Any, Dict

from fastapi import Depends, HTTPException, Request

from services.audit_logger import log_event
from services.auth_service import require_a2a_auth, require_admin
from services.precompute_service import schedule_precompute
from services.reconcile_service import get_reconcile_status, run_reconcile_with_status
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


_META_RETENTION_TABLES: Dict[str, Dict[str, str]] = {
    "pack_ingests": {
        "stats": "pack_ingests_stats",
        "enforce": "enforce_pack_ingests_retention",
        "max_rows": "pack_ingests_max_rows",
        "max_days": "pack_ingests_max_days",
        "maintenance": "pack_ingests_maintenance_interval_s",
        "last": "pack_ingests_retention_last",
    },
    "sequence_meta": {
        "stats": "sequence_meta_stats",
        "enforce": "enforce_sequence_meta_retention",
        "max_rows": "sequence_meta_max_rows",
        "max_days": "sequence_meta_max_days",
        "maintenance": "sequence_meta_maintenance_interval_s",
        "last": "sequence_meta_retention_last",
    },
    "audio_analyses": {
        "stats": "audio_analyses_stats",
        "enforce": "enforce_audio_analyses_retention",
        "max_rows": "audio_analyses_max_rows",
        "max_days": "audio_analyses_max_days",
        "maintenance": "audio_analyses_maintenance_interval_s",
        "last": "audio_analyses_retention_last",
    },
    "show_configs": {
        "stats": "show_configs_stats",
        "enforce": "enforce_show_configs_retention",
        "max_rows": "show_configs_max_rows",
        "max_days": "show_configs_max_days",
        "maintenance": "show_configs_maintenance_interval_s",
        "last": "show_configs_retention_last",
    },
    "fseq_exports": {
        "stats": "fseq_exports_stats",
        "enforce": "enforce_fseq_exports_retention",
        "max_rows": "fseq_exports_max_rows",
        "max_days": "fseq_exports_max_days",
        "maintenance": "fseq_exports_maintenance_interval_s",
        "last": "fseq_exports_retention_last",
    },
    "fpp_scripts": {
        "stats": "fpp_scripts_stats",
        "enforce": "enforce_fpp_scripts_retention",
        "max_rows": "fpp_scripts_max_rows",
        "max_days": "fpp_scripts_max_days",
        "maintenance": "fpp_scripts_maintenance_interval_s",
        "last": "fpp_scripts_retention_last",
    },
}


async def _emit_meta_event(
    state: AppState,
    *,
    event: str,
    kind: str,
    payload: Dict[str, Any] | None = None,
    event_type: str = "meta",
) -> None:
    try:
        from services.events_service import emit_event

        data: Dict[str, Any] = {"event": str(event or ""), "kind": str(kind or "")}
        if payload:
            data.update(payload)
        await emit_event(state, event_type=event_type, data=data)
    except Exception:
        return


async def meta_packs(
    request: Request,
    limit: int = 200,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    db = _require_db(state)
    try:
        lim = _clamp_limit(limit)
        packs = await db.list_pack_ingests(limit=lim)
        await log_event(
            state,
            action="packs.list",
            ok=True,
            payload={"count": len(packs)},
            request=request,
            emit=False,
        )
        await _emit_meta_event(
            state,
            event="list",
            kind="packs",
            payload={"count": len(packs), "limit": lim},
            event_type="packs",
        )
        return {"ok": True, "packs": packs}
    except Exception as e:
        await log_event(
            state,
            action="packs.list",
            ok=False,
            error=str(e),
            request=request,
            emit=False,
        )
        raise


async def meta_sequences(
    request: Request,
    limit: int = 500,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    db = _require_db(state)
    try:
        lim = _clamp_limit(limit, default=500)
        sequences = await db.list_sequence_meta(limit=lim)
        await log_event(
            state,
            action="meta.sequences",
            ok=True,
            payload={"count": len(sequences)},
            request=request,
            emit=False,
        )
        await _emit_meta_event(
            state,
            event="list",
            kind="sequences",
            payload={"count": len(sequences), "limit": lim},
        )
        return {"ok": True, "sequences": sequences}
    except Exception as e:
        await log_event(
            state,
            action="meta.sequences",
            ok=False,
            error=str(e),
            request=request,
            emit=False,
        )
        raise


async def meta_audio_analyses(
    request: Request,
    limit: int = 200,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    db = _require_db(state)
    try:
        lim = _clamp_limit(limit)
        audio_analyses = await db.list_audio_analyses(limit=lim)
        await log_event(
            state,
            action="meta.audio_analyses",
            ok=True,
            payload={"count": len(audio_analyses)},
            request=request,
            emit=False,
        )
        await _emit_meta_event(
            state,
            event="list",
            kind="audio_analyses",
            payload={"count": len(audio_analyses), "limit": lim},
        )
        return {"ok": True, "audio_analyses": audio_analyses}
    except Exception as e:
        await log_event(
            state,
            action="meta.audio_analyses",
            ok=False,
            error=str(e),
            request=request,
            emit=False,
        )
        raise


async def meta_show_configs(
    request: Request,
    limit: int = 200,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    db = _require_db(state)
    try:
        lim = _clamp_limit(limit)
        show_configs = await db.list_show_configs(limit=lim)
        await log_event(
            state,
            action="meta.show_configs",
            ok=True,
            payload={"count": len(show_configs)},
            request=request,
            emit=False,
        )
        await _emit_meta_event(
            state,
            event="list",
            kind="show_configs",
            payload={"count": len(show_configs), "limit": lim},
        )
        return {"ok": True, "show_configs": show_configs}
    except Exception as e:
        await log_event(
            state,
            action="meta.show_configs",
            ok=False,
            error=str(e),
            request=request,
            emit=False,
        )
        raise


async def meta_fseq_exports(
    request: Request,
    limit: int = 200,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    db = _require_db(state)
    try:
        lim = _clamp_limit(limit)
        fseq_exports = await db.list_fseq_exports(limit=lim)
        await log_event(
            state,
            action="meta.fseq_exports",
            ok=True,
            payload={"count": len(fseq_exports)},
            request=request,
            emit=False,
        )
        await _emit_meta_event(
            state,
            event="list",
            kind="fseq_exports",
            payload={"count": len(fseq_exports), "limit": lim},
        )
        return {"ok": True, "fseq_exports": fseq_exports}
    except Exception as e:
        await log_event(
            state,
            action="meta.fseq_exports",
            ok=False,
            error=str(e),
            request=request,
            emit=False,
        )
        raise


async def meta_fpp_scripts(
    request: Request,
    limit: int = 200,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    db = _require_db(state)
    try:
        lim = _clamp_limit(limit)
        fpp_scripts = await db.list_fpp_scripts(limit=lim)
        await log_event(
            state,
            action="meta.fpp_scripts",
            ok=True,
            payload={"count": len(fpp_scripts)},
            request=request,
            emit=False,
        )
        await _emit_meta_event(
            state,
            event="list",
            kind="fpp_scripts",
            payload={"count": len(fpp_scripts), "limit": lim},
        )
        return {"ok": True, "fpp_scripts": fpp_scripts}
    except Exception as e:
        await log_event(
            state,
            action="meta.fpp_scripts",
            ok=False,
            error=str(e),
            request=request,
            emit=False,
        )
        raise


async def meta_last_applied(
    request: Request,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    db = _require_db(state)
    try:
        rows = await db.list_last_applied()
        out: Dict[str, Any] = {}
        for r in rows:
            if isinstance(r, dict) and "kind" in r:
                out[str(r.get("kind"))] = dict(r)
        await log_event(
            state,
            action="meta.last_applied",
            ok=True,
            payload={"count": len(out)},
            request=request,
            emit=False,
        )
        await _emit_meta_event(
            state,
            event="status",
            kind="last_applied",
            payload={"count": len(out)},
        )
        return {"ok": True, "last_applied": out}
    except Exception as e:
        await log_event(
            state,
            action="meta.last_applied",
            ok=False,
            error=str(e),
            request=request,
            emit=False,
        )
        raise


async def meta_reconcile(
    request: Request,
    packs: bool = True,
    sequences: bool = True,
    audio: bool = False,
    show_configs: bool = True,
    fseq_exports: bool = True,
    fpp_scripts: bool = True,
    scan_limit: int = 5000,
    precompute_previews: bool | None = None,
    precompute_waveforms: bool | None = None,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    try:
        task = getattr(state, "reconcile_task", None)
        if task is not None and not getattr(task, "done", lambda: True)():
            raise HTTPException(status_code=409, detail="Reconcile already running")
        res = await run_reconcile_with_status(
            state,
            mode="manual",
            packs=bool(packs),
            sequences=bool(sequences),
            audio=bool(audio),
            show_configs=bool(show_configs),
            fseq_exports=bool(fseq_exports),
            fpp_scripts=bool(fpp_scripts),
            scan_limit=int(scan_limit),
            precompute_previews=bool(
                precompute_previews
                if precompute_previews is not None
                else state.settings.precompute_previews_on_reconcile
            ),
            precompute_waveforms=bool(
                precompute_waveforms
                if precompute_waveforms is not None
                else state.settings.precompute_waveforms_on_reconcile
            ),
        )
        if res.get("cancelled"):
            await log_event(
                state,
                action="meta.reconcile",
                ok=False,
                error="cancelled",
                request=request,
            )
            raise HTTPException(status_code=409, detail="Reconcile cancelled")
        await log_event(
            state,
            action="meta.reconcile",
            ok=True,
            payload={
                "packs": bool(packs),
                "sequences": bool(sequences),
                "audio": bool(audio),
                "scan_limit": int(scan_limit),
                "precompute_previews": bool(
                    precompute_previews
                    if precompute_previews is not None
                    else state.settings.precompute_previews_on_reconcile
                ),
                "precompute_waveforms": bool(
                    precompute_waveforms
                    if precompute_waveforms is not None
                    else state.settings.precompute_waveforms_on_reconcile
                ),
            },
            request=request,
        )
        return res
    except Exception as e:
        await log_event(
            state,
            action="meta.reconcile",
            ok=False,
            error=str(e),
            request=request,
        )
        raise


async def meta_precompute(
    request: Request,
    previews: bool = True,
    waveforms: bool = True,
    formats: str | None = None,
    scan_limit: int | None = None,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    try:
        fmt_list: list[str] | None = None
        if formats:
            fmt_list = [
                f.strip().lower()
                for f in str(formats).split(",")
                if f.strip()
            ]
        job = await schedule_precompute(
            state,
            reason="manual",
            sequences=None if previews else [],
            audio_files=None if waveforms else [],
            scan_limit=int(scan_limit) if scan_limit is not None else None,
            formats=fmt_list,
        )
        if job is None:
            res = {"ok": True, "skipped": True, "reason": "No matching files"}
        else:
            res = {"ok": True, "job": job}
        await log_event(
            state,
            action="meta.precompute",
            ok=True,
            payload={
                "previews": bool(previews),
                "waveforms": bool(waveforms),
                "formats": fmt_list,
                "scan_limit": int(scan_limit) if scan_limit is not None else None,
                "skipped": bool(res.get("skipped")),
            },
            request=request,
        )
        return res
    except Exception as e:
        await log_event(
            state,
            action="meta.precompute",
            ok=False,
            error=str(e),
            request=request,
        )
        raise HTTPException(status_code=500, detail=str(e))


async def meta_reconcile_status(
    request: Request,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    db = getattr(state, "db", None)
    if db is None:
        await log_event(
            state,
            action="meta.reconcile.status",
            ok=False,
            error="Database not initialized",
            request=request,
            emit=False,
        )
        raise HTTPException(status_code=503, detail="Database not initialized")
    try:
        row = await get_reconcile_status(state)
        await log_event(
            state,
            action="meta.reconcile.status",
            ok=True,
            request=request,
            emit=False,
        )
        if not row:
            await _emit_meta_event(
                state,
                event="status",
                kind="reconcile",
                payload={"exists": False},
            )
            return {"ok": True, "exists": False}
        await _emit_meta_event(
            state,
            event="status",
            kind="reconcile",
            payload={"exists": True, "status": row},
        )
        return {"ok": True, "exists": True, "status": row}
    except Exception as e:
        await log_event(
            state,
            action="meta.reconcile.status",
            ok=False,
            error=str(e),
            request=request,
            emit=False,
        )
        raise


async def meta_reconcile_history(
    request: Request,
    limit: int = 200,
    offset: int = 0,
    status: str | None = None,
    source: str | None = None,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    db = getattr(state, "db", None)
    if db is None:
        raise HTTPException(status_code=503, detail="Database not initialized")
    try:
        rows = await db.list_reconcile_runs(
            limit=int(limit), offset=int(offset), status=status, source=source
        )
        next_offset = int(offset) + len(rows) if len(rows) >= int(limit) else None
        await log_event(
            state,
            action="meta.reconcile.history",
            ok=True,
            payload={"count": len(rows)},
            request=request,
            emit=False,
        )
        await _emit_meta_event(
            state,
            event="list",
            kind="reconcile_history",
            payload={
                "count": len(rows),
                "limit": int(limit),
                "offset": int(offset),
                "next_offset": next_offset,
            },
        )
        return {
            "ok": True,
            "runs": rows,
            "count": len(rows),
            "limit": int(limit),
            "offset": int(offset),
            "next_offset": next_offset,
        }
    except Exception as e:
        await log_event(
            state,
            action="meta.reconcile.history",
            ok=False,
            error=str(e),
            request=request,
            emit=False,
        )
        raise


async def meta_reconcile_cancel(
    request: Request,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    db = getattr(state, "db", None)
    if db is None:
        raise HTTPException(status_code=503, detail="Database not initialized")
    cancel_event = getattr(state, "reconcile_cancel_event", None)
    if cancel_event is not None:
        cancel_event.set()
    run_id = getattr(state, "reconcile_run_id", None)
    if run_id is not None:
        try:
            await db.mark_reconcile_cancel_requested(int(run_id))
        except Exception:
            pass
    try:
        status = await get_reconcile_status(state)
        if status:
            status["cancel_requested"] = True
            await db.kv_set_json("meta_reconcile_status", dict(status))
    except Exception:
        pass
    await log_event(
        state,
        action="meta.reconcile.cancel",
        ok=True,
        payload={"run_id": run_id},
        request=request,
    )
    return {"ok": True, "run_id": run_id}


async def meta_retention_status(
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    db = _require_db(state)
    try:
        tables: Dict[str, Dict[str, Any]] = {}
        now = time.time()
        for key, cfg in _META_RETENTION_TABLES.items():
            stats_fn = getattr(db, cfg["stats"])
            stats = await stats_fn()
            max_rows = int(getattr(state.settings, cfg["max_rows"], 0) or 0)
            max_days = int(getattr(state.settings, cfg["max_days"], 0) or 0)
            oldest = stats.get("oldest")
            oldest_age_s = max(0.0, now - float(oldest)) if oldest else None
            excess_rows = (
                max(0, int(stats.get("count", 0)) - max_rows) if max_rows else 0
            )
            excess_age_s = (
                max(0.0, float(oldest_age_s) - (max_days * 86400.0))
                if max_days and oldest_age_s is not None
                else 0.0
            )
            drift = bool(excess_rows > 0 or excess_age_s > 0)
            tables[key] = {
                "stats": stats,
                "settings": {
                    "max_rows": max_rows,
                    "max_days": max_days,
                    "maintenance_interval_s": int(
                        getattr(state.settings, cfg["maintenance"], 0) or 0
                    ),
                },
                "drift": {
                    "excess_rows": int(excess_rows),
                    "excess_age_s": float(excess_age_s),
                    "oldest_age_s": float(oldest_age_s)
                    if oldest_age_s is not None
                    else None,
                    "drift": drift,
                },
                "last_retention": getattr(state, cfg["last"], None),
            }
        return {"ok": True, "tables": tables}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def meta_retention_cleanup(
    table: str,
    max_rows: int | None = None,
    max_days: int | None = None,
    _: Dict[str, Any] = Depends(require_admin),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    db = _require_db(state)
    key = str(table or "").strip().lower()
    if not key or key not in _META_RETENTION_TABLES:
        raise HTTPException(status_code=400, detail="Unknown metadata table")
    cfg = _META_RETENTION_TABLES[key]
    try:
        cfg_max_rows = int(getattr(state.settings, cfg["max_rows"], 0) or 0)
        cfg_max_days = int(getattr(state.settings, cfg["max_days"], 0) or 0)
        use_max_rows = max_rows if max_rows is not None else (cfg_max_rows or None)
        use_max_days = max_days if max_days is not None else (cfg_max_days or None)
        enforce_fn = getattr(db, cfg["enforce"])
        result = await enforce_fn(max_rows=use_max_rows, max_days=use_max_days)
        setattr(
            state,
            cfg["last"],
            {"at": time.time(), "result": result},
        )
        return {"ok": True, "table": key, "result": result}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
