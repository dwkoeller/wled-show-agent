from __future__ import annotations

import os
import shutil
import time
import zipfile
from pathlib import Path, PurePosixPath
from typing import Any, Dict

from pack_io import write_json


def _zipinfo_is_symlink(info: zipfile.ZipInfo) -> bool:
    # Only works for Unix-style zips; safe default is "not a symlink".
    mode = (int(getattr(info, "external_attr", 0)) >> 16) & 0o170000
    return mode == 0o120000


def extract_pack(
    *,
    tmp_zip: str,
    staging_dir: str,
    final_dir: str,
    dest_rel: str,
    overwrite: bool,
    total_bytes: int,
    max_files: int,
    max_unpacked_bytes: int,
    ingest_id: str,
    service_name: str,
    service_version: str,
) -> Dict[str, Any]:
    extracted: list[str] = []
    unpacked_bytes = 0
    staging = Path(staging_dir)
    final_path = Path(final_dir)
    tmp_zip_path = Path(tmp_zip)

    try:
        staging.mkdir(parents=True, exist_ok=False)
    except Exception as e:
        raise RuntimeError(f"Failed to create staging dir: {e}")

    try:
        with zipfile.ZipFile(tmp_zip_path) as zf:
            infos = zf.infolist()
            if len(infos) > max_files:
                raise ValueError("Zip contains too many entries")

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
                    raise ValueError("Zip contains a symlink entry")

                p = PurePosixPath(name)
                if p.is_absolute() or ".." in p.parts:
                    raise ValueError("Zip contains unsafe paths")
                if not p.parts:
                    continue
                # Reject Windows drive-letter like "C:".
                if ":" in p.parts[0]:
                    raise ValueError("Zip contains unsafe paths")

                unpacked_bytes += int(getattr(info, "file_size", 0) or 0)
                if unpacked_bytes > max_unpacked_bytes:
                    raise ValueError("Zip unpacks too large")
                file_infos.append((info, p))

            for info, rel_posix in file_infos:
                out_path = staging.joinpath(*rel_posix.parts)
                try:
                    rp = out_path.resolve()
                except Exception:
                    raise ValueError("Zip contains invalid paths")
                if staging.resolve() not in rp.parents:
                    raise ValueError("Zip contains unsafe paths")

                out_path.parent.mkdir(parents=True, exist_ok=True)
                with zf.open(info, "r") as src, open(out_path, "wb") as dst:
                    shutil.copyfileobj(src, dst, length=1024 * 1024)
                extracted.append(rel_posix.as_posix())

        manifest = {
            "ok": True,
            "service": str(service_name),
            "version": str(service_version),
            "ingest_id": str(ingest_id),
            "dest_dir": str(dest_rel),
            "uploaded_bytes": int(total_bytes),
            "unpacked_bytes": int(unpacked_bytes),
            "files": sorted(extracted),
            "created_at": time.time(),
        }
        write_json(str(staging / "manifest.json"), manifest)

        if final_path.exists() and bool(overwrite):
            shutil.rmtree(final_path)
        os.replace(str(staging), str(final_path))

        return {
            "manifest": manifest,
            "extracted": sorted(extracted),
            "unpacked_bytes": int(unpacked_bytes),
        }
    finally:
        if staging.exists():
            shutil.rmtree(staging, ignore_errors=True)
