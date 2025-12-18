from __future__ import annotations

import os
import uuid
from pathlib import Path
from typing import Any, Dict

from fastapi import Depends, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse

from services.auth_service import require_a2a_auth
from services.state import AppState, get_state


def _resolve_data_path(state: AppState, rel_path: str) -> Path:
    base = Path(state.settings.data_dir).resolve()
    p = (base / (rel_path or "")).resolve()
    if p == base:
        return p
    if base not in p.parents:
        raise HTTPException(status_code=400, detail="Path must be within DATA_DIR.")
    return p


def files_list(
    dir: str = "",
    glob: str = "*",
    recursive: bool = False,
    limit: int = 500,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    try:
        base = _resolve_data_path(state, dir)
        if not base.exists():
            return {"ok": True, "files": []}
        if not base.is_dir():
            raise HTTPException(
                status_code=400, detail="dir must be a directory under DATA_DIR"
            )

        pattern = (glob or "*").strip() or "*"
        it = base.rglob(pattern) if bool(recursive) else base.glob(pattern)

        out: list[str] = []
        root = Path(state.settings.data_dir).resolve()
        lim = max(1, min(5000, int(limit)))
        for p in it:
            if len(out) >= lim:
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
        return {"ok": True, "files": out}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def files_download(
    path: str,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> FileResponse:
    p = _resolve_data_path(state, path)
    if not p.is_file():
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(path=str(p), filename=p.name)


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
    root = Path(state.settings.data_dir).resolve()
    dest = _resolve_data_path(state, path)
    if dest == root or dest.is_dir():
        raise HTTPException(
            status_code=400, detail="path must be a file under DATA_DIR"
        )

    try:
        dest.parent.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create parent dir: {e}")

    if dest.exists() and not overwrite:
        raise HTTPException(
            status_code=409, detail="File already exists (set overwrite=true)"
        )

    tmp = dest.with_name(f".{dest.name}.uploading-{uuid.uuid4().hex}")
    total = 0
    try:
        with open(tmp, "wb") as f:
            async for chunk in request.stream():
                if not chunk:
                    continue
                total += len(chunk)
                f.write(chunk)
        os.replace(tmp, dest)
    finally:
        try:
            if tmp.exists():
                tmp.unlink()
        except Exception:
            pass

    try:
        rel = str(dest.resolve().relative_to(root))
    except Exception:
        rel = str(dest)
    return {"ok": True, "path": rel, "bytes": total}


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
    if dest == root or dest.is_dir():
        raise HTTPException(
            status_code=400, detail="path must be a file under DATA_DIR"
        )

    try:
        dest.parent.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create parent dir: {e}")

    if dest.exists() and not overwrite:
        raise HTTPException(
            status_code=409, detail="File already exists (set overwrite=true)"
        )

    tmp = dest.with_name(f".{dest.name}.uploading-{uuid.uuid4().hex}")
    total = 0
    try:
        with open(tmp, "wb") as f:
            while True:
                chunk = await file.read(1024 * 1024)
                if not chunk:
                    break
                total += len(chunk)
                f.write(chunk)
        os.replace(tmp, dest)
    finally:
        try:
            await file.close()
        except Exception:
            pass
        try:
            if tmp.exists():
                tmp.unlink()
        except Exception:
            pass

    try:
        rel_out = str(dest.resolve().relative_to(root))
    except Exception:
        rel_out = str(dest)
    return {"ok": True, "path": rel_out, "bytes": total}


def files_delete(
    path: str,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    p = _resolve_data_path(state, path)
    if not p.exists():
        return {"ok": True, "deleted": False}
    if not p.is_file():
        raise HTTPException(
            status_code=400, detail="path must be a file under DATA_DIR"
        )
    try:
        p.unlink()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete file: {e}")
    return {"ok": True, "deleted": True}
