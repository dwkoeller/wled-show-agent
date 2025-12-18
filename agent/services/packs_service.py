from __future__ import annotations

import os
import shutil
import time
import uuid
import zipfile
from pathlib import Path, PurePosixPath
from typing import Any, Dict

from fastapi import Depends, HTTPException, Request

from config.constants import APP_VERSION, SERVICE_NAME
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


def _zipinfo_is_symlink(info: zipfile.ZipInfo) -> bool:
    # Only works for Unix-style zips; safe default is "not a symlink".
    mode = (int(getattr(info, "external_attr", 0)) >> 16) & 0o170000
    return mode == 0o120000


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
        parent.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create parent dir: {e}")

    if final_dir.exists():
        if not bool(overwrite):
            raise HTTPException(
                status_code=409,
                detail="Destination already exists (set overwrite=true)",
            )
        if not final_dir.is_dir():
            raise HTTPException(status_code=400, detail="dest_dir must be a directory")

    ingest_id = uuid.uuid4().hex
    staging_dir = parent / f".ingest-{final_dir.name}-{ingest_id}"
    tmp_zip = parent / f".ingest-{final_dir.name}-{ingest_id}.zip"

    total_bytes = 0
    try:
        with open(tmp_zip, "wb") as f:
            async for chunk in request.stream():
                if not chunk:
                    continue
                total_bytes += len(chunk)
                f.write(chunk)

        try:
            staging_dir.mkdir(parents=True, exist_ok=False)
        except Exception as e:
            raise HTTPException(
                status_code=500, detail=f"Failed to create staging dir: {e}"
            )

        max_files = max(1, int(os.environ.get("PACK_MAX_FILES", "4000") or "4000"))
        max_unpacked_mb = float(os.environ.get("PACK_MAX_UNPACKED_MB", "500") or "500")
        max_unpacked_bytes = int(max(1.0, max_unpacked_mb) * 1024 * 1024)

        extracted: list[str] = []
        unpacked_bytes = 0

        with zipfile.ZipFile(tmp_zip) as zf:
            infos = zf.infolist()
            if len(infos) > max_files:
                raise HTTPException(
                    status_code=400, detail="Zip contains too many entries"
                )

            file_infos: list[tuple[zipfile.ZipInfo, PurePosixPath]] = []
            for info in infos:
                name = str(getattr(info, "filename", "") or "")
                if not name:
                    continue
                # Normalize Windows zip paths.
                name = name.replace("\\", "/")
                if name.endswith("/"):
                    continue
                if getattr(info, "is_dir", None) and info.is_dir():
                    continue
                if _zipinfo_is_symlink(info):
                    raise HTTPException(
                        status_code=400, detail="Zip contains a symlink entry"
                    )

                p = PurePosixPath(name)
                if p.is_absolute() or ".." in p.parts:
                    raise HTTPException(
                        status_code=400, detail="Zip contains unsafe paths"
                    )
                if not p.parts:
                    continue
                # Reject Windows drive-letter like "C:".
                if ":" in p.parts[0]:
                    raise HTTPException(
                        status_code=400, detail="Zip contains unsafe paths"
                    )

                unpacked_bytes += int(getattr(info, "file_size", 0) or 0)
                if unpacked_bytes > max_unpacked_bytes:
                    raise HTTPException(status_code=400, detail="Zip unpacks too large")
                file_infos.append((info, p))

            for info, rel_posix in file_infos:
                out_path = staging_dir.joinpath(*rel_posix.parts)
                try:
                    rp = out_path.resolve()
                except Exception:
                    raise HTTPException(
                        status_code=400, detail="Zip contains invalid paths"
                    )
                if staging_dir.resolve() not in rp.parents:
                    raise HTTPException(
                        status_code=400, detail="Zip contains unsafe paths"
                    )

                out_path.parent.mkdir(parents=True, exist_ok=True)
                with zf.open(info, "r") as src, open(out_path, "wb") as dst:
                    shutil.copyfileobj(src, dst, length=1024 * 1024)
                extracted.append(rel_posix.as_posix())

        manifest_path = f"{dest_rel.rstrip('/')}/manifest.json"
        manifest = {
            "ok": True,
            "service": SERVICE_NAME,
            "version": APP_VERSION,
            "ingest_id": ingest_id,
            "dest_dir": dest_rel,
            "uploaded_bytes": total_bytes,
            "unpacked_bytes": unpacked_bytes,
            "files": sorted(extracted),
            "created_at": time.time(),
        }
        # Use the existing JSON writer (atomic) by importing lazily.
        from pack_io import write_json

        write_json(str(staging_dir / "manifest.json"), manifest)

        if final_dir.exists() and bool(overwrite):
            shutil.rmtree(final_dir)
        os.replace(str(staging_dir), str(final_dir))

        if state.db is not None:
            try:
                await state.db.upsert_pack_ingest(
                    dest_dir=dest_rel,
                    source_name=None,
                    manifest_path=manifest_path,
                    uploaded_bytes=total_bytes,
                    unpacked_bytes=unpacked_bytes,
                    file_count=len(extracted),
                )
            except Exception:
                pass

        return {
            "ok": True,
            "dest_dir": dest_rel,
            "uploaded_bytes": total_bytes,
            "unpacked_bytes": unpacked_bytes,
            "files": sorted(extracted),
            "manifest": manifest_path,
        }
    finally:
        try:
            if tmp_zip.exists():
                tmp_zip.unlink()
        except Exception:
            pass
        try:
            if staging_dir.exists():
                shutil.rmtree(staging_dir)
        except Exception:
            pass
