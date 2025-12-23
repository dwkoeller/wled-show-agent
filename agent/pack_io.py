from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import aiofiles

def ensure_dir(path: str) -> str:
    Path(path).mkdir(parents=True, exist_ok=True)
    return path


def nowstamp() -> str:
    # Safe filename timestamp (UTC-ish)
    import datetime

    return (
        datetime.datetime.utcnow().replace(microsecond=0).isoformat().replace(":", "-")
        + "Z"
    )


def write_jsonl(path: str, rows: Iterable[Dict[str, Any]]) -> str:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
    return str(p)


def read_jsonl(path: str, *, limit: Optional[int] = None) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            out.append(json.loads(line))
            if limit is not None and len(out) >= limit:
                break
    return out


def write_json(path: str, obj: Any) -> str:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    return str(p)


def read_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


async def write_jsonl_async(path: str, rows: Iterable[Dict[str, Any]]) -> str:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    async with aiofiles.open(p, "w", encoding="utf-8") as f:
        for row in rows:
            await f.write(json.dumps(row, ensure_ascii=False) + "\n")
    return str(p)


async def read_jsonl_async(
    path: str, *, limit: Optional[int] = None
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    async with aiofiles.open(path, "r", encoding="utf-8") as f:
        async for line in f:
            line = line.strip()
            if not line:
                continue
            out.append(json.loads(line))
            if limit is not None and len(out) >= limit:
                break
    return out


async def write_json_async(path: str, obj: Any) -> str:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    async with aiofiles.open(p, "w", encoding="utf-8") as f:
        await f.write(json.dumps(obj, ensure_ascii=False, indent=2))
    return str(p)


async def read_json_async(path: str) -> Any:
    async with aiofiles.open(path, "r", encoding="utf-8") as f:
        return json.loads(await f.read())
