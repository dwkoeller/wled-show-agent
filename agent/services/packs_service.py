from __future__ import annotations

import os
import uuid
from pathlib import Path
from typing import Any, Dict

import aiofiles
from aiofiles import os as aio_os
from fastapi import Depends, HTTPException, Request

from config.constants import APP_VERSION, SERVICE_NAME
from services.audit_logger import log_event
from services.auth_service import require_a2a_auth
from services.files_service import (
    _collect_rel_files,
    _delete_metadata_for_paths,
    _maybe_delete_metadata,
    _maybe_upsert_metadata,
)
from services.precompute_service import schedule_precompute
from services.state import AppState, get_state
from utils.blocking import run_cpu_blocking_state
from utils.packs_extract import extract_pack


def _resolve_data_path(state: AppState, rel_path: str) -> Path:
    base = Path(state.settings.data_dir).resolve()
    p = (base / (rel_path or "")).resolve()
    if p == base:
        return p
    if base not in p.parents:
        raise HTTPException(status_code=400, detail="Path must be within DATA_DIR.")
    return p


async def packs_ingest(
    request: Request,
    dest_dir: str,
    overwrite: bool = False,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    """
    Upload a zip file and unpack it into a dedicated folder under DATA_DIR.

    - The zip is streamed to disk first (no full buffering).
    - Extraction is staged, then atomically renamed into place for rollback safety.
    """
    dest_rel = str(dest_dir or "").strip().strip("/")
    if not dest_rel:
        raise HTTPException(status_code=400, detail="dest_dir is required")

    root = Path(state.settings.data_dir).resolve()
    final_dir = _resolve_data_path(state, dest_rel)
    if final_dir == root:
        raise HTTPException(
            status_code=400, detail="dest_dir must not be DATA_DIR root"
        )

    parent = final_dir.parent

    try:
        await aio_os.makedirs(str(parent), exist_ok=True)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create parent dir: {e}")

    dest_exists = await aio_os.path.exists(str(final_dir))
    dest_is_dir = False
    if dest_exists:
        dest_is_dir = await aio_os.path.isdir(str(final_dir))
        if not bool(overwrite):
            raise HTTPException(
                status_code=409,
                detail="Destination already exists (set overwrite=true)",
            )
        if not dest_is_dir:
            raise HTTPException(status_code=400, detail="dest_dir must be a directory")

    old_rel_files: list[str] = []
    if bool(overwrite) and dest_exists and dest_is_dir:
        try:
            old_rel_files = await _collect_rel_files(state, final_dir)
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    ingest_id = uuid.uuid4().hex
    staging_dir = parent / f".ingest-{final_dir.name}-{ingest_id}"
    tmp_zip = parent / f".ingest-{final_dir.name}-{ingest_id}.zip"

    total_bytes = 0
    max_files = max(1, int(os.environ.get("PACK_MAX_FILES", "4000") or "4000"))
    max_unpacked_mb = float(os.environ.get("PACK_MAX_UNPACKED_MB", "500") or "500")
    max_unpacked_bytes = int(max(1.0, max_unpacked_mb) * 1024 * 1024)
    manifest_path = f"{dest_rel.rstrip('/')}/manifest.json"

    try:
        async with aiofiles.open(tmp_zip, "wb") as f:
            async for chunk in request.stream():
                if not chunk:
                    continue
                total_bytes += len(chunk)
                await f.write(chunk)

        try:
            result = await run_cpu_blocking_state(
                state,
                extract_pack,
                tmp_zip=str(tmp_zip),
                staging_dir=str(staging_dir),
                final_dir=str(final_dir),
                dest_rel=dest_rel,
                overwrite=bool(overwrite),
                total_bytes=int(total_bytes),
                max_files=int(max_files),
                max_unpacked_bytes=int(max_unpacked_bytes),
                ingest_id=ingest_id,
                service_name=str(SERVICE_NAME),
                service_version=str(APP_VERSION),
            )
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

        if state.db is not None:
            try:
                await state.db.upsert_pack_ingest(
                    dest_dir=dest_rel,
                    source_name=None,
                    manifest_path=manifest_path,
                    uploaded_bytes=total_bytes,
                    unpacked_bytes=int(result["unpacked_bytes"]),
                    file_count=len(result["extracted"]),
                )
            except Exception:
                pass

        if result.get("extracted"):
            for rel_file in list(result["extracted"]):
                rel_path = f"{dest_rel.rstrip('/')}/{rel_file}"
                abs_path = _resolve_data_path(state, rel_path)
                meta_result = await _maybe_upsert_metadata(
                    state, rel_path=rel_path, abs_path=abs_path
                )
                if meta_result is False:
                    await _maybe_delete_metadata(state, rel_path)

        if old_rel_files:
            await _delete_metadata_for_paths(state, old_rel_files)

        precompute_job = None
        if result.get("extracted"):
            seq_files: list[str] = []
            audio_files: list[str] = []
            dest_prefix = dest_rel.rstrip("/")
            for rel_file in list(result.get("extracted") or []):
                rel_path = f"{dest_prefix}/{rel_file}".strip("/")
                if rel_path.startswith("sequences/") and rel_path.endswith(".json"):
                    rel_seq = rel_path[len("sequences/") :]
                    if rel_seq:
                        seq_files.append(rel_seq)
                ext = Path(rel_path).suffix.lower()
                if rel_path.startswith("music/") and ext in {
                    ".wav",
                    ".mp3",
                    ".aac",
                    ".m4a",
                    ".flac",
                    ".ogg",
                }:
                    audio_files.append(rel_path)
            if seq_files or audio_files:
                if state.settings.precompute_previews_on_ingest or state.settings.precompute_waveforms_on_ingest:
                    try:
                        precompute_job = await schedule_precompute(
                            state,
                            reason="pack_ingest",
                            sequences=seq_files if state.settings.precompute_previews_on_ingest else [],
                            audio_files=audio_files if state.settings.precompute_waveforms_on_ingest else [],
                            scan_limit=int(state.settings.db_reconcile_scan_limit or 5000),
                        )
                    except Exception:
                        precompute_job = None

        res = {
            "ok": True,
            "dest_dir": dest_rel,
            "uploaded_bytes": total_bytes,
            "unpacked_bytes": int(result["unpacked_bytes"]),
            "files": list(result["extracted"]),
            "manifest": manifest_path,
            "precompute_job": precompute_job,
        }
        await log_event(
            state,
            action="packs.ingest",
            ok=True,
            resource=str(dest_rel),
            payload={
                "uploaded_bytes": int(total_bytes),
                "unpacked_bytes": int(result["unpacked_bytes"]),
                "files": len(result.get("extracted") or []),
                "overwrite": bool(overwrite),
            },
            request=request,
        )
        return res
    except HTTPException as e:
        await log_event(
            state,
            action="packs.ingest",
            ok=False,
            resource=str(dest_dir),
            error=str(getattr(e, "detail", e)),
            payload={"overwrite": bool(overwrite)},
            request=request,
        )
        raise
    except Exception as e:
        await log_event(
            state,
            action="packs.ingest",
            ok=False,
            resource=str(dest_dir),
            error=str(e),
            payload={"overwrite": bool(overwrite)},
            request=request,
        )
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        try:
            await aio_os.unlink(str(tmp_zip))
        except Exception:
            pass
