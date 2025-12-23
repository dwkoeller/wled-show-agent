from __future__ import annotations

import shutil
import uuid
from pathlib import Path
from typing import Any, Dict

import aiofiles
from aiofiles import os as aio_os
from fastapi import Depends, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse

from pack_io import read_json_async
from services.audit_logger import log_event
from services.auth_service import require_a2a_auth
from services.state import AppState, get_state
from show_config import ShowConfig
from utils.blocking import run_cpu_blocking_state


def _resolve_data_path(state: AppState, rel_path: str) -> Path:
    base = Path(state.settings.data_dir).resolve()
    p = (base / (rel_path or "")).resolve()
    if p == base:
        return p
    if base not in p.parents:
        raise HTTPException(status_code=400, detail="Path must be within DATA_DIR.")
    return p


def _rel_path(root: Path, path: Path) -> str:
    try:
        return str(path.resolve().relative_to(root))
    except Exception:
        return str(path)


def _normalize_rel_path(rel_path: str) -> str:
    return str(rel_path or "").replace("\\", "/").lstrip("/")


async def _emit_files_event(
    state: AppState, *, event: str, payload: Dict[str, Any] | None = None
) -> None:
    try:
        from services.events_service import emit_event

        data: Dict[str, Any] = {"event": str(event or "")}
        if payload:
            data.update(payload)
        await emit_event(state, event_type="files", data=data)
    except Exception:
        return


async def _emit_meta_event(
    state: AppState,
    *,
    event: str,
    kind: str,
    path: str | None = None,
    payload: Dict[str, Any] | None = None,
) -> None:
    try:
        from services.events_service import emit_event

        data: Dict[str, Any] = {"event": str(event or ""), "kind": str(kind or "")}
        if path:
            data["path"] = str(path)
        if payload:
            data.update(payload)
        await emit_event(state, event_type="meta", data=data)
    except Exception:
        return


def _scan_rel_files(*, root_dir: str, base_dir: str) -> list[str]:
    root = Path(root_dir).resolve()
    base = Path(base_dir).resolve()
    if not base.exists() or not base.is_dir():
        return []
    out: list[str] = []
    for p in base.rglob("*"):
        if not p.is_file():
            continue
        try:
            rp = p.resolve()
        except Exception:
            continue
        if root not in rp.parents and rp != root:
            continue
        try:
            out.append(str(rp.relative_to(root)))
        except Exception:
            out.append(str(rp))
    return out


def _scan_files(
    *, root_dir: str, base_dir: str, pattern: str, recursive: bool, limit: int
) -> list[str]:
    root = Path(root_dir).resolve()
    base = Path(base_dir).resolve()
    if not base.exists():
        return []
    if not base.is_dir():
        raise ValueError("dir must be a directory under DATA_DIR")

    it = base.rglob(pattern) if bool(recursive) else base.glob(pattern)
    out: list[str] = []
    for p in it:
        if len(out) >= int(limit):
            break
        try:
            rp = p.resolve()
        except Exception:
            continue
        if root not in rp.parents and rp != root:
            continue
        if rp.is_file():
            try:
                out.append(str(rp.relative_to(root)))
            except Exception:
                out.append(str(rp))
    out.sort()
    return out


def _dir_has_entries(path_s: str) -> bool:
    p = Path(path_s)
    try:
        next(p.iterdir())
        return True
    except StopIteration:
        return False


def _delete_dir_sync(path_s: str, recursive: bool) -> None:
    p = Path(path_s)
    if recursive:
        shutil.rmtree(p)
    else:
        p.rmdir()


def _validate_upload_allowlist(state: AppState, rel_path: str) -> None:
    if not bool(getattr(state.settings, "files_upload_allowlist_only", False)):
        return
    rel_norm = _normalize_rel_path(rel_path)
    parts = rel_norm.split("/", 1)
    if len(parts) < 2:
        raise HTTPException(
            status_code=400,
            detail="path must include a top-level dir (audio/, music/, xlights/, sequences/)",
        )
    top_dir = parts[0]
    allow = _upload_allowlist()
    if top_dir not in allow:
        raise HTTPException(
            status_code=400,
            detail=f"dir '{top_dir}' is not allowed for raw upload",
        )
    ext = Path(parts[1]).suffix.lower()
    if not ext or ext not in allow[top_dir]:
        raise HTTPException(
            status_code=400,
            detail=f"File type '{ext or '<none>'}' is not allowed for dir '{top_dir}'",
        )


async def _collect_rel_files(state: AppState, base: Path) -> list[str]:
    root = Path(state.settings.data_dir).resolve()
    return await run_cpu_blocking_state(
        state, _scan_rel_files, root_dir=str(root), base_dir=str(base)
    )


async def _delete_metadata_for_paths(state: AppState, rel_paths: list[str]) -> None:
    for rel_path in rel_paths:
        await _maybe_delete_metadata(state, rel_path)


def _show_config_payload(cfg: ShowConfig) -> Dict[str, Any]:
    props_by_kind: Dict[str, int] = {}
    for prop in cfg.props:
        k = str(getattr(prop, "kind", "") or "").strip().lower() or "unknown"
        props_by_kind[k] = props_by_kind.get(k, 0) + 1
    return {
        "subnet": cfg.subnet,
        "channels_per_universe": cfg.channels_per_universe,
        "props_by_kind": props_by_kind,
    }


async def _read_fseq_header(
    path: Path,
) -> tuple[int | None, int | None, int | None]:
    try:
        async with aiofiles.open(path, "rb") as f:
            hdr = await f.read(20)
    except Exception:
        return None, None, None
    if len(hdr) < 19:
        return None, None, None
    if hdr[0:4] != b"PSEQ":
        return None, None, None
    try:
        channels = int.from_bytes(hdr[10:14], "little", signed=False)
        frames = int.from_bytes(hdr[14:18], "little", signed=False)
        step_ms = int(hdr[18])
        return frames, channels, step_ms
    except Exception:
        return None, None, None


async def _maybe_delete_metadata(state: AppState, rel_path: str) -> None:
    db = getattr(state, "db", None)
    if db is None:
        return

    rel_norm = _normalize_rel_path(rel_path)
    if not rel_norm:
        return

    try:
        if rel_norm.startswith("sequences/"):
            seq_file = rel_norm[len("sequences/") :]
            await db.delete_sequence_meta(file=seq_file)
            await _emit_meta_event(
                state,
                event="deleted",
                kind="sequence",
                path=rel_norm,
                payload={"file": seq_file},
            )
        if rel_norm.startswith("show/"):
            show_file = rel_norm[len("show/") :]
            await db.delete_show_config(file=show_file)
            await _emit_meta_event(
                state,
                event="deleted",
                kind="show_config",
                path=rel_norm,
                payload={"file": show_file},
            )
        if rel_norm.startswith("fseq/"):
            fseq_file = rel_norm[len("fseq/") :]
            await db.delete_fseq_export(file=fseq_file)
            await _emit_meta_event(
                state,
                event="deleted",
                kind="fseq_export",
                path=rel_norm,
                payload={"file": fseq_file},
            )
        if rel_norm.startswith("fpp/scripts/"):
            script_file = rel_norm[len("fpp/scripts/") :]
            await db.delete_fpp_script(file=script_file)
            await _emit_meta_event(
                state,
                event="deleted",
                kind="fpp_script",
                path=rel_norm,
                payload={"file": script_file},
            )
        if rel_norm.endswith("manifest.json") and "/" in rel_norm:
            dest_dir = rel_norm.rsplit("/", 1)[0]
            await db.delete_pack_ingest(dest_dir=dest_dir)
            await _emit_meta_event(
                state,
                event="deleted",
                kind="pack_ingest",
                path=rel_norm,
                payload={"dest_dir": dest_dir},
            )
        await db.delete_audio_analysis_by_beats_path(beats_path=rel_norm)
        if rel_norm.startswith("audio/") and rel_norm.endswith(".json"):
            await _emit_meta_event(
                state,
                event="deleted",
                kind="audio_analysis",
                path=rel_norm,
            )
    except Exception:
        return


async def _maybe_upsert_metadata(
    state: AppState, *, rel_path: str, abs_path: Path
) -> bool | None:
    db = getattr(state, "db", None)
    if db is None:
        return None

    rel_norm = _normalize_rel_path(rel_path)
    if not rel_norm:
        return None

    if rel_norm.startswith("sequences/") and rel_norm.endswith(".json"):
        try:
            seq = await read_json_async(str(abs_path))
            if not isinstance(seq, dict):
                raise ValueError("sequence JSON must be an object")
            steps = list(seq.get("steps", []))
            steps_total = len([s for s in steps if isinstance(s, dict)])
            duration_s = 0.0
            for s in steps:
                if not isinstance(s, dict):
                    continue
                try:
                    duration_s += float(s.get("duration_s") or 0.0)
                except Exception:
                    continue
        except Exception:
            return False

        try:
            await db.upsert_sequence_meta(
                file=rel_norm[len("sequences/") :],
                duration_s=duration_s,
                steps_total=steps_total,
            )
            await _emit_meta_event(
                state,
                event="updated",
                kind="sequence",
                path=rel_norm,
                payload={"file": rel_norm[len("sequences/") :]},
            )
            return True
        except Exception:
            return False

    if rel_norm.startswith("show/") and rel_norm.endswith(".json"):
        try:
            raw = await read_json_async(str(abs_path))
            cfg = ShowConfig.model_validate(raw)
            payload = _show_config_payload(cfg)
        except Exception:
            return False

        try:
            await db.upsert_show_config(
                file=rel_norm[len("show/") :],
                name=str(cfg.name or ""),
                props_total=len(cfg.props),
                groups_total=len(cfg.groups or {}),
                coordinator_base_url=str(cfg.coordinator.base_url or "").strip() or None,
                fpp_base_url=str(cfg.fpp.base_url or "").strip() or None,
                payload=payload,
            )
            await _emit_meta_event(
                state,
                event="updated",
                kind="show_config",
                path=rel_norm,
                payload={"file": rel_norm[len("show/") :]},
            )
            return True
        except Exception:
            return False

    if rel_norm.startswith("fseq/") and rel_norm.endswith(".fseq"):
        try:
            frames, channels, step_ms = await _read_fseq_header(abs_path)
            try:
                stat_res = await aio_os.stat(str(abs_path))
                bytes_written = int(stat_res.st_size)
            except Exception:
                bytes_written = 0
            duration_s = (
                (float(frames) * float(step_ms) / 1000.0)
                if frames is not None and step_ms is not None
                else None
            )
        except Exception:
            return False

        try:
            await db.upsert_fseq_export(
                file=rel_norm[len("fseq/") :],
                source_sequence=None,
                bytes_written=int(bytes_written),
                frames=frames,
                channels=channels,
                step_ms=step_ms,
                duration_s=duration_s,
                payload={},
            )
            await _emit_meta_event(
                state,
                event="updated",
                kind="fseq_export",
                path=rel_norm,
                payload={"file": rel_norm[len("fseq/") :]},
            )
            return True
        except Exception:
            return False

    if rel_norm.startswith("fpp/scripts/") and rel_norm.endswith(".sh"):
        try:
            async with aiofiles.open(abs_path, "r", encoding="utf-8", errors="ignore") as f:
                raw = await f.read()
        except Exception:
            raw = ""
        kind = "custom"
        if "/v1/fleet/sequences/start" in raw:
            kind = "fleet_sequence_start"
        elif "/v1/fleet/stop_all" in raw:
            kind = "fleet_stop_all"
        try:
            stat_res = await aio_os.stat(str(abs_path))
            bytes_written = int(stat_res.st_size)
        except Exception:
            bytes_written = 0

        try:
            await db.upsert_fpp_script(
                file=rel_norm[len("fpp/scripts/") :],
                kind=kind,
                bytes_written=int(bytes_written),
                payload={},
            )
            await _emit_meta_event(
                state,
                event="updated",
                kind="fpp_script",
                path=rel_norm,
                payload={"file": rel_norm[len("fpp/scripts/") :]},
            )
            return True
        except Exception:
            return False

    if rel_norm.endswith("manifest.json") and "/" in rel_norm:
        try:
            manifest = await read_json_async(str(abs_path))
            if not isinstance(manifest, dict):
                raise ValueError("manifest JSON must be an object")
        except Exception:
            return False

        dest_dir = str(
            manifest.get("dest_dir") or rel_norm.rsplit("/", 1)[0]
        ).strip()
        dest_dir = _normalize_rel_path(dest_dir)
        uploaded_bytes = int(manifest.get("uploaded_bytes") or 0)
        unpacked_bytes = int(manifest.get("unpacked_bytes") or 0)
        files = manifest.get("files")
        if isinstance(files, list):
            file_count = len(files)
        else:
            file_count = int(manifest.get("file_count") or 0)
        source_name = (
            str(manifest.get("source_name") or "").strip() or None
        )

        try:
            await db.upsert_pack_ingest(
                dest_dir=dest_dir,
                source_name=source_name,
                manifest_path=rel_norm,
                uploaded_bytes=uploaded_bytes,
                unpacked_bytes=unpacked_bytes,
                file_count=file_count,
            )
            await _emit_meta_event(
                state,
                event="updated",
                kind="pack_ingest",
                path=rel_norm,
                payload={"dest_dir": dest_dir},
            )
            return True
        except Exception:
            return False

    if rel_norm.startswith("audio/") and rel_norm.endswith(".json"):
        try:
            raw = await read_json_async(str(abs_path))
            if not isinstance(raw, dict):
                raise ValueError("audio analysis JSON must be an object")
            beats = raw.get("beats_s")
            if beats is None:
                beats = raw.get("beats_ms")
            beat_count = None
            if isinstance(beats, list):
                beat_count = len(beats)
            bpm = raw.get("bpm")
            if bpm is None and isinstance(raw.get("bpm_timeline"), list):
                tl = raw.get("bpm_timeline") or []
                if tl and isinstance(tl[0], dict):
                    bpm = tl[0].get("bpm")
            bpm_val = float(bpm) if bpm is not None else None
        except Exception:
            return False

        try:
            await db.delete_audio_analysis_by_beats_path(beats_path=rel_norm)
            await db.add_audio_analysis(
                analysis_id=uuid.uuid4().hex,
                source_path=None,
                beats_path=rel_norm,
                prefer_ffmpeg=False,
                bpm=bpm_val,
                beat_count=beat_count,
                error=None,
            )
            await _emit_meta_event(
                state,
                event="updated",
                kind="audio_analysis",
                path=rel_norm,
            )
            return True
        except Exception:
            return False

    return None


async def files_list(
    dir: str = "",
    glob: str = "*",
    recursive: bool = False,
    limit: int = 500,
    request: Request | None = None,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    base = _resolve_data_path(state, dir)
    root = Path(state.settings.data_dir).resolve()
    lim = max(1, min(5000, int(limit)))
    pattern = (glob or "*").strip() or "*"

    try:
        files = await run_cpu_blocking_state(
            state,
            _scan_files,
            root_dir=str(root),
            base_dir=str(base),
            pattern=pattern,
            recursive=bool(recursive),
            limit=lim,
        )
        await log_event(
            state,
            action="files.list",
            ok=True,
            resource=str(dir or ""),
            payload={
                "glob": pattern,
                "recursive": bool(recursive),
                "limit": lim,
                "count": len(files),
            },
            request=request,
            emit=False,
        )
        await _emit_files_event(
            state,
            event="list",
            payload={
                "dir": str(dir or ""),
                "glob": pattern,
                "recursive": bool(recursive),
                "limit": lim,
                "count": len(files),
            },
        )
        return {"ok": True, "files": files}
    except ValueError as e:
        await log_event(
            state,
            action="files.list",
            ok=False,
            resource=str(dir or ""),
            error=str(e),
            payload={"glob": pattern, "recursive": bool(recursive), "limit": lim},
            request=request,
            emit=False,
        )
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException as e:
        await log_event(
            state,
            action="files.list",
            ok=False,
            resource=str(dir or ""),
            error=str(getattr(e, "detail", e)),
            payload={"glob": pattern, "recursive": bool(recursive), "limit": lim},
            request=request,
            emit=False,
        )
        raise
    except Exception as e:
        await log_event(
            state,
            action="files.list",
            ok=False,
            resource=str(dir or ""),
            error=str(e),
            payload={"glob": pattern, "recursive": bool(recursive), "limit": lim},
            request=request,
            emit=False,
        )
        raise HTTPException(status_code=500, detail=str(e))


async def files_download(
    path: str,
    request: Request | None = None,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> FileResponse:
    try:
        p = _resolve_data_path(state, path)
        if not await aio_os.path.isfile(str(p)):
            raise HTTPException(status_code=404, detail="File not found")
        await log_event(
            state,
            action="files.download",
            ok=True,
            resource=str(path or ""),
            payload={"filename": p.name},
            request=request,
        )
        return FileResponse(path=str(p), filename=p.name)
    except HTTPException as e:
        await log_event(
            state,
            action="files.download",
            ok=False,
            resource=str(path or ""),
            error=str(getattr(e, "detail", e)),
            request=request,
        )
        raise
    except Exception as e:
        await log_event(
            state,
            action="files.download",
            ok=False,
            resource=str(path or ""),
            error=str(e),
            request=request,
        )
        raise


async def files_upload(
    path: str,
    request: Request,
    overwrite: bool = False,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    """
    Upload a file to DATA_DIR.

    This endpoint accepts raw bytes (e.g. Content-Type: application/octet-stream) to avoid
    requiring multipart parsing dependencies.
    """
    try:
        root = Path(state.settings.data_dir).resolve()
        dest = _resolve_data_path(state, path)
        try:
            rel_for_allowlist = str(dest.resolve().relative_to(root))
        except Exception:
            rel_for_allowlist = str(path or "")
        _validate_upload_allowlist(state, rel_for_allowlist)
        if dest == root or await aio_os.path.isdir(str(dest)):
            raise HTTPException(
                status_code=400, detail="path must be a file under DATA_DIR"
            )

        try:
            await aio_os.makedirs(str(dest.parent), exist_ok=True)
        except Exception as e:
            raise HTTPException(
                status_code=500, detail=f"Failed to create parent dir: {e}"
            )

        if await aio_os.path.exists(str(dest)) and not overwrite:
            raise HTTPException(
                status_code=409, detail="File already exists (set overwrite=true)"
            )

        tmp = dest.with_name(f".{dest.name}.uploading-{uuid.uuid4().hex}")
        total = 0
        try:
            async with aiofiles.open(tmp, "wb") as f:
                async for chunk in request.stream():
                    if not chunk:
                        continue
                    total += len(chunk)
                    await f.write(chunk)
            await aio_os.replace(str(tmp), str(dest))
        finally:
            try:
                await aio_os.unlink(str(tmp))
            except Exception:
                pass

        try:
            rel = str(dest.resolve().relative_to(root))
        except Exception:
            rel = str(dest)
        meta_result = await _maybe_upsert_metadata(state, rel_path=rel, abs_path=dest)
        if meta_result is False:
            await _maybe_delete_metadata(state, rel)

        await log_event(
            state,
            action="files.upload",
            ok=True,
            resource=str(rel),
            payload={"bytes": int(total), "overwrite": bool(overwrite)},
            request=request,
        )
        return {"ok": True, "path": rel, "bytes": total}
    except HTTPException as e:
        await log_event(
            state,
            action="files.upload",
            ok=False,
            resource=str(path or ""),
            error=str(getattr(e, "detail", e)),
            payload={"overwrite": bool(overwrite)},
            request=request,
        )
        raise
    except Exception as e:
        await log_event(
            state,
            action="files.upload",
            ok=False,
            resource=str(path or ""),
            error=str(e),
            payload={"overwrite": bool(overwrite)},
            request=request,
        )
        raise


def _upload_allowlist() -> Dict[str, set[str]]:
    audio_exts = {".wav", ".mp3", ".ogg", ".flac", ".m4a", ".aac"}
    return {
        "audio": set(audio_exts),
        "music": set(audio_exts),  # common alias for "audio" in many setups
        "xlights": {".xsq"},
        "sequences": {".json"},
    }


async def files_upload_multipart(
    file: UploadFile = File(...),
    dir: str = Form(...),
    filename: str | None = Form(default=None),
    overwrite: bool = Form(default=False),
    request: Request | None = None,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    """
    Upload a file to DATA_DIR with strict allowlisting.

    Allowed:
    - audio/music: .wav/.mp3/.ogg/.flac/.m4a/.aac
    - xlights: .xsq
    - sequences: .json
    """
    try:
        allow = _upload_allowlist()
        d = str(dir or "").strip().strip("/")
        if d not in allow:
            raise HTTPException(status_code=400, detail=f"dir '{d}' is not allowed")

        name = str(filename or "").strip()
        if not name:
            name = str(getattr(file, "filename", "") or "").strip()
        # Strip any path components.
        name = Path(name).name
        if not name or name in (".", ".."):
            raise HTTPException(status_code=400, detail="filename is required")

        ext = Path(name).suffix.lower()
        if ext not in allow[d]:
            raise HTTPException(
                status_code=400,
                detail=f"File type '{ext or '<none>'}' is not allowed for dir '{d}'",
            )

        rel = f"{d}/{name}"
        root = Path(state.settings.data_dir).resolve()
        dest = _resolve_data_path(state, rel)
        if dest == root or await aio_os.path.isdir(str(dest)):
            raise HTTPException(
                status_code=400, detail="path must be a file under DATA_DIR"
            )

        try:
            await aio_os.makedirs(str(dest.parent), exist_ok=True)
        except Exception as e:
            raise HTTPException(
                status_code=500, detail=f"Failed to create parent dir: {e}"
            )

        if await aio_os.path.exists(str(dest)) and not overwrite:
            raise HTTPException(
                status_code=409, detail="File already exists (set overwrite=true)"
            )

        tmp = dest.with_name(f".{dest.name}.uploading-{uuid.uuid4().hex}")
        total = 0
        try:
            async with aiofiles.open(tmp, "wb") as f:
                while True:
                    chunk = await file.read(1024 * 1024)
                    if not chunk:
                        break
                    total += len(chunk)
                    await f.write(chunk)
            await aio_os.replace(str(tmp), str(dest))
        finally:
            try:
                await file.close()
            except Exception:
                pass
            try:
                await aio_os.unlink(str(tmp))
            except Exception:
                pass

        try:
            rel_out = str(dest.resolve().relative_to(root))
        except Exception:
            rel_out = str(dest)
        meta_result = await _maybe_upsert_metadata(
            state, rel_path=rel_out, abs_path=dest
        )
        if meta_result is False:
            await _maybe_delete_metadata(state, rel_out)

        await log_event(
            state,
            action="files.upload",
            ok=True,
            resource=str(rel_out),
            payload={"bytes": int(total), "overwrite": bool(overwrite)},
            request=request,
        )
        return {"ok": True, "path": rel_out, "bytes": total}
    except HTTPException as e:
        await log_event(
            state,
            action="files.upload",
            ok=False,
            resource=str(dir or ""),
            error=str(getattr(e, "detail", e)),
            payload={"overwrite": bool(overwrite)},
            request=request,
        )
        raise
    except Exception as e:
        await log_event(
            state,
            action="files.upload",
            ok=False,
            resource=str(dir or ""),
            error=str(e),
            payload={"overwrite": bool(overwrite)},
            request=request,
        )
        raise


async def files_delete(
    path: str,
    request: Request | None = None,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    try:
        p = _resolve_data_path(state, path)
        if not await aio_os.path.exists(str(p)):
            await log_event(
                state,
                action="files.delete",
                ok=True,
                resource=str(path or ""),
                payload={"deleted": False},
                request=request,
            )
            return {"ok": True, "deleted": False}
        if not await aio_os.path.isfile(str(p)):
            raise HTTPException(
                status_code=400, detail="path must be a file under DATA_DIR"
            )
        root = Path(state.settings.data_dir).resolve()
        rel = _rel_path(root, p)
        try:
            await aio_os.unlink(str(p))
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to delete file: {e}")

        await _maybe_delete_metadata(state, rel)
        await log_event(
            state,
            action="files.delete",
            ok=True,
            resource=str(rel),
            payload={"deleted": True},
            request=request,
        )
        return {"ok": True, "deleted": True, "path": rel}
    except HTTPException as e:
        await log_event(
            state,
            action="files.delete",
            ok=False,
            resource=str(path or ""),
            error=str(getattr(e, "detail", e)),
            request=request,
        )
        raise
    except Exception as e:
        await log_event(
            state,
            action="files.delete",
            ok=False,
            resource=str(path or ""),
            error=str(e),
            request=request,
        )
        raise


async def files_delete_dir(
    dir: str,
    recursive: bool = True,
    request: Request | None = None,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    try:
        p = _resolve_data_path(state, dir)
        root = Path(state.settings.data_dir).resolve()
        if p == root:
            raise HTTPException(status_code=400, detail="dir must not be DATA_DIR root")
        if not await aio_os.path.exists(str(p)):
            await log_event(
                state,
                action="files.delete_dir",
                ok=True,
                resource=str(dir or ""),
                payload={"deleted": False, "recursive": bool(recursive)},
                request=request,
            )
            return {"ok": True, "deleted": False}
        if not await aio_os.path.isdir(str(p)):
            raise HTTPException(
                status_code=400, detail="dir must be a directory under DATA_DIR"
            )

        try:
            rel = _rel_path(root, p)
        except Exception:
            rel = str(p)

        if not bool(recursive):
            try:
                if await run_cpu_blocking_state(
                    state, _dir_has_entries, str(p)
                ):
                    raise HTTPException(
                        status_code=400, detail="dir is not empty (set recursive=true)"
                    )
            except HTTPException:
                raise
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))

        try:
            rel_files = await _collect_rel_files(state, p)
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

        await _delete_metadata_for_paths(state, rel_files)
        if state.db is not None:
            try:
                await state.db.delete_pack_ingest(dest_dir=_normalize_rel_path(rel))
            except Exception:
                pass

        try:
            await run_cpu_blocking_state(
                state, _delete_dir_sync, str(p), bool(recursive)
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

        await log_event(
            state,
            action="files.delete_dir",
            ok=True,
            resource=str(rel),
            payload={"deleted": True, "files": len(rel_files), "recursive": bool(recursive)},
            request=request,
        )
        return {
            "ok": True,
            "deleted": True,
            "path": rel,
            "files": len(rel_files),
        }
    except HTTPException as e:
        await log_event(
            state,
            action="files.delete_dir",
            ok=False,
            resource=str(dir or ""),
            error=str(getattr(e, "detail", e)),
            payload={"recursive": bool(recursive)},
            request=request,
        )
        raise
    except Exception as e:
        await log_event(
            state,
            action="files.delete_dir",
            ok=False,
            resource=str(dir or ""),
            error=str(e),
            payload={"recursive": bool(recursive)},
            request=request,
        )
        raise
