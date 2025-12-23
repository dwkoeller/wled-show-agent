from __future__ import annotations

import asyncio
import csv
import io
import json
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict

import aiofiles
from aiofiles import os as aio_os
from fastapi import Depends, HTTPException, Request
from fastapi.responses import Response, StreamingResponse

from services.auth_service import require_a2a_auth, require_admin
from services.state import AppState, get_state


@dataclass(frozen=True)
class EventMessage:
    id: int | None
    type: str
    data: Dict[str, Any]
    ts: float


@dataclass
class EventSubscriber:
    id: int
    connected_at: float
    last_event_id: int | None
    last_seen_at: float | None
    missed: int = 0
    dropped: int = 0

    def as_dict(self, *, queue_max: int, queue_size: int) -> Dict[str, Any]:
        return {
            "id": int(self.id),
            "connected_at": float(self.connected_at),
            "last_event_id": self.last_event_id,
            "last_seen_at": self.last_seen_at,
            "missed": int(self.missed),
            "dropped": int(self.dropped),
            "queue_max": int(queue_max),
            "queue_size": int(queue_size),
        }


class EventBus:
    def __init__(self, *, max_queue: int = 200, max_history: int = 1000) -> None:
        self._max_queue = max(10, int(max_queue))
        self._max_history = max(50, int(max_history))
        self._history: deque[EventMessage] = deque(maxlen=self._max_history)
        self._next_id = 1
        self._next_sub_id = 1
        self._subs: dict[asyncio.Queue[EventMessage], EventSubscriber] = {}
        self._lock = asyncio.Lock()

    async def subscribe(
        self, *, last_event_id: int | None = None
    ) -> tuple[asyncio.Queue[EventMessage], list[EventMessage], int]:
        q: asyncio.Queue[EventMessage] = asyncio.Queue(maxsize=self._max_queue)
        history: list[EventMessage] = []
        async with self._lock:
            sub = EventSubscriber(
                id=self._next_sub_id,
                connected_at=time.time(),
                last_event_id=last_event_id if last_event_id and last_event_id > 0 else None,
                last_seen_at=None,
            )
            self._next_sub_id += 1
            self._subs[q] = sub
            if last_event_id is not None and last_event_id >= 0:
                history = [
                    msg
                    for msg in self._history
                    if msg.id is not None and msg.id > last_event_id
                ]
        return q, history, sub.id

    async def unsubscribe(self, q: asyncio.Queue[EventMessage]) -> None:
        async with self._lock:
            self._subs.pop(q, None)

    async def note_missed(self, q: asyncio.Queue[EventMessage], *, count: int) -> None:
        if count <= 0:
            return
        async with self._lock:
            sub = self._subs.get(q)
            if sub is None:
                return
            sub.missed += int(count)

    async def note_seen(
        self, q: asyncio.Queue[EventMessage], *, event_id: int | None = None
    ) -> None:
        async with self._lock:
            sub = self._subs.get(q)
            if sub is None:
                return
            sub.last_seen_at = time.time()
            if event_id is not None and event_id > 0:
                sub.last_event_id = int(event_id)

    async def publish(self, message: EventMessage) -> EventMessage:
        async with self._lock:
            if message.id is None or message.id <= 0:
                msg = EventMessage(
                    id=self._next_id,
                    type=message.type,
                    data=message.data,
                    ts=message.ts,
                )
                self._next_id += 1
            else:
                msg = message
                if msg.id >= self._next_id:
                    self._next_id = msg.id + 1
            self._history.append(msg)
            subs = list(self._subs.items())
        if not subs:
            return msg
        for q, sub in subs:
            if q.full():
                try:
                    _ = q.get_nowait()
                except Exception:
                    pass
                try:
                    sub.dropped += 1
                    sub.missed += 1
                except Exception:
                    pass
            try:
                q.put_nowait(msg)
            except Exception:
                pass
        return msg

    async def stats(self, *, include_clients: bool = False) -> Dict[str, Any]:
        async with self._lock:
            payload: Dict[str, Any] = {
                "subscribers": len(self._subs),
                "history": len(self._history),
                "max_history": self._max_history,
                "missed_total": sum(int(s.missed) for s in self._subs.values()),
                "dropped_total": sum(int(s.dropped) for s in self._subs.values()),
            }
            if include_clients:
                payload["clients"] = [
                    s.as_dict(queue_max=self._max_queue, queue_size=q.qsize())
                    for q, s in self._subs.items()
                ]
            return payload


_SPOOL_LOCK = asyncio.Lock()
_SPOOL_STATS: Dict[str, int] = {
    "queued_bytes": 0,
    "queued_events": 0,
    "dropped": 0,
    "rotated": 0,
}
_SPOOL_STATS_AT = 0.0
_SPOOL_STATS_TTL_S = 5.0
_SPOOL_STATS_LOADED = False
_SPOOL_STATS_WRITE_AT = 0.0
_SPOOL_STATS_WRITE_MIN_INTERVAL_S = 1.0


def _event_type_from_action(action: str) -> str:
    raw = str(action or "").strip().lower()
    if not raw:
        return "event"
    if "orchestration" in raw:
        return "orchestration"
    if raw.startswith("fleet"):
        return "fleet"
    if raw.startswith("auth"):
        return "auth"
    if raw.startswith("files"):
        return "files"
    if raw.startswith("scheduler"):
        return "scheduler"
    if raw.startswith("fpp"):
        return "fpp"
    if raw.startswith("mqtt"):
        return "mqtt"
    if raw.startswith("packs"):
        return "packs"
    if raw.startswith("audio"):
        return "audio"
    if raw.startswith("fseq"):
        return "fseq"
    if raw.startswith("backup"):
        return "backup"
    if raw.startswith("looks"):
        return "looks"
    if raw.startswith("sequences"):
        return "sequences"
    if raw.startswith("ddp"):
        return "ddp"
    if raw.startswith("meta"):
        return "meta"
    if raw.startswith("audit"):
        return "audit"
    return raw.split(".", 1)[0]


async def emit_event(
    state: AppState,
    *,
    event_type: str,
    data: Dict[str, Any] | None = None,
) -> None:
    bus: EventBus | None = getattr(state, "events", None)
    if bus is None:
        return
    msg = EventMessage(
        id=None,
        type=str(event_type or "event"),
        data=dict(data or {}),
        ts=time.time(),
    )
    msg = await _persist_event(state, msg)
    await bus.publish(msg)


async def emit_event_for_action(
    state: AppState,
    *,
    action: str,
    data: Dict[str, Any] | None = None,
) -> None:
    event_type = _event_type_from_action(action)
    await emit_event(state, event_type=event_type, data=data)


async def events_stream(
    request: Request,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
    heartbeat_s: float = 15.0,
) -> StreamingResponse:
    bus: EventBus | None = getattr(state, "events", None)
    if bus is None:
        bus = EventBus()
        setattr(state, "events", bus)

    last_event_id = _parse_last_event_id(request)
    allowed_types = _parse_event_types(request)
    allowed_kinds = _parse_event_kinds(request)

    def _allowed(msg: EventMessage) -> bool:
        if msg.type in ("ready", "tick"):
            return True
        if allowed_types is not None and msg.type.lower() not in allowed_types:
            return False
        if allowed_kinds is None:
            return True
        kind = _event_kind_from_data(msg.data)
        if not kind:
            return False
        return kind.lower() in allowed_kinds

    async def _gen():  # type: ignore[no-untyped-def]
        q, bus_history, client_id = await bus.subscribe(last_event_id=last_event_id)
        last_sent_id = int(last_event_id or 0)
        try:
            missed_on_connect = 0
            if (
                last_event_id is not None
                and allowed_types is None
                and allowed_kinds is None
            ):
                db = getattr(state, "db", None)
                if db is not None:
                    try:
                        bounds = await db.get_event_log_bounds()
                        min_id = bounds.get("min_id")
                        if min_id is not None and int(last_event_id) < int(min_id):
                            missed_on_connect = int(min_id) - int(last_event_id) - 1
                    except Exception:
                        missed_on_connect = 0
            if missed_on_connect > 0:
                await bus.note_missed(q, count=missed_on_connect)

            ready = EventMessage(
                id=None,
                type="ready",
                data={
                    "agent_id": getattr(state.settings, "agent_id", None),
                    "client_id": client_id,
                    "missed_on_connect": missed_on_connect,
                },
                ts=time.time(),
            )
            yield _format_event(ready)
            db_history = await _history_from_db(
                state,
                last_event_id=last_event_id,
                allowed_types=allowed_types,
                allowed_kinds=allowed_kinds,
            )
            history = _merge_history(
                last_event_id,
                db_history=db_history,
                bus_history=bus_history,
            )
            for msg in history:
                if _allowed(msg):
                    await bus.note_seen(q, event_id=msg.id)
                    yield _format_event(msg)
                    if msg.id is not None and msg.id > last_sent_id:
                        last_sent_id = msg.id
            while True:
                if await request.is_disconnected():
                    break
                try:
                    msg = await asyncio.wait_for(q.get(), timeout=float(heartbeat_s))
                    if msg.id is not None and msg.id <= last_sent_id:
                        continue
                    if _allowed(msg):
                        await bus.note_seen(q, event_id=msg.id)
                        yield _format_event(msg)
                        if msg.id is not None and msg.id > last_sent_id:
                            last_sent_id = msg.id
                except asyncio.TimeoutError:
                    heartbeat = EventMessage(
                        id=None,
                        type="tick",
                        data={},
                        ts=time.time(),
                    )
                    yield _format_event(heartbeat)
        finally:
            await bus.unsubscribe(q)

    headers = {
        "Cache-Control": "no-cache",
        "X-Accel-Buffering": "no",
    }
    return StreamingResponse(
        _gen(),
        media_type="text/event-stream",
        headers=headers,
    )


async def events_history(
    limit: int = 200,
    event_type: str | None = None,
    event: str | None = None,
    agent_id: str | None = None,
    since: float | None = None,
    until: float | None = None,
    offset: int = 0,
    after_id: int | None = None,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    db = getattr(state, "db", None)
    if db is None:
        raise HTTPException(status_code=503, detail="Database not initialized")
    try:
        lim = _clamp_limit(limit)
        after_val = _parse_after_id(after_id)
        if after_val is not None:
            types = _parse_csv_filter(event_type)
            kinds = _parse_csv_filter(event)
            cursor = await _resolve_event_cursor(db, after_val)
            if cursor is None:
                raise HTTPException(status_code=400, detail="after_id not found")
            after_ts, after_row_id = cursor
            rows = await db.list_event_logs_after_cursor(
                after_created_at=after_ts,
                after_id=after_row_id,
                limit=lim,
                agent_id=agent_id,
                event_types=types,
                event_kinds=kinds,
                since=since,
                until=until,
            )
            count = len(rows)
            next_after_id = rows[-1]["id"] if rows else None
            return {
                "ok": True,
                "events": rows,
                "count": count,
                "limit": lim,
                "offset": 0,
                "next_offset": None,
                "after_id": after_val,
                "next_after_id": next_after_id,
            }

        off = max(0, int(offset))
        rows = await db.list_event_logs(
            limit=lim,
            agent_id=agent_id,
            event_type=event_type,
            event=event,
            since=since,
            until=until,
            offset=off,
        )
        count = len(rows)
        next_offset = off + count if count >= lim else None
        return {
            "ok": True,
            "events": rows,
            "count": count,
            "limit": lim,
            "offset": off,
            "next_offset": next_offset,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def events_stats(
    include_clients: bool = False,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    bus: EventBus | None = getattr(state, "events", None)
    if bus is None:
        bus = EventBus()
        setattr(state, "events", bus)
    try:
        bus_stats = await bus.stats(include_clients=bool(include_clients))
    except Exception:
        bus_stats = {}
    spool_stats = await get_spool_stats(state)
    return {"ok": True, "bus": bus_stats, "spool": spool_stats}


async def events_retention_status(
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    db = getattr(state, "db", None)
    if db is None:
        raise HTTPException(status_code=503, detail="Database not initialized")
    try:
        stats = await db.event_log_stats()
        now = time.time()
        max_rows = int(getattr(state.settings, "events_history_max_rows", 0) or 0)
        max_days = int(getattr(state.settings, "events_history_max_days", 0) or 0)
        oldest = stats.get("oldest")
        oldest_age_s = max(0.0, now - float(oldest)) if oldest else None
        excess_rows = max(0, int(stats.get("count", 0)) - max_rows) if max_rows else 0
        excess_age_s = (
            max(0.0, float(oldest_age_s) - (max_days * 86400.0))
            if max_days and oldest_age_s is not None
            else 0.0
        )
        drift = bool(excess_rows > 0 or excess_age_s > 0)
        return {
            "ok": True,
            "stats": stats,
            "settings": {
                "max_rows": max_rows,
                "max_days": max_days,
                "maintenance_interval_s": int(
                    getattr(state.settings, "events_history_maintenance_interval_s", 0)
                    or 0
                ),
            },
            "drift": {
                "excess_rows": int(excess_rows),
                "excess_age_s": float(excess_age_s),
                "oldest_age_s": float(oldest_age_s) if oldest_age_s is not None else None,
                "drift": drift,
            },
            "last_retention": getattr(state, "event_log_retention_last", None),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def events_retention_cleanup(
    max_rows: int | None = None,
    max_days: int | None = None,
    _: Dict[str, Any] = Depends(require_admin),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    db = getattr(state, "db", None)
    if db is None:
        raise HTTPException(status_code=503, detail="Database not initialized")
    try:
        cfg_max_rows = int(getattr(state.settings, "events_history_max_rows", 0) or 0)
        cfg_max_days = int(getattr(state.settings, "events_history_max_days", 0) or 0)
        use_max_rows = max_rows if max_rows is not None else (cfg_max_rows or None)
        use_max_days = max_days if max_days is not None else (cfg_max_days or None)
        result = await db.enforce_event_log_retention(
            max_rows=use_max_rows,
            max_days=use_max_days,
        )
        state.event_log_retention_last = {
            "at": time.time(),
            "result": result,
        }
        return {"ok": True, "result": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def events_history_export(
    limit: int = 2000,
    event_type: str | None = None,
    event: str | None = None,
    agent_id: str | None = None,
    since: float | None = None,
    until: float | None = None,
    offset: int = 0,
    after_id: int | None = None,
    format: str = "csv",
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Response:
    db = getattr(state, "db", None)
    if db is None:
        raise HTTPException(status_code=503, detail="Database not initialized")
    try:
        lim = _clamp_limit(limit, default=2000, max_limit=20000)
        fmt = str(format or "csv").strip().lower()
        off = max(0, int(offset))
        after_val = _parse_after_id(after_id)
        batch_size = min(1000, lim)
        cursor = None
        if after_val is not None:
            cursor = await _resolve_event_cursor(db, after_val)
            if cursor is None:
                raise HTTPException(status_code=400, detail="after_id not found")

        async def _iter_rows():  # type: ignore[no-untyped-def]
            remaining = lim
            current_offset = off
            current_after = after_val
            current_after_ts = cursor[0] if cursor else None
            types = _parse_csv_filter(event_type) if after_val is not None else None
            kinds = _parse_csv_filter(event) if after_val is not None else None
            while remaining > 0:
                page_limit = min(batch_size, remaining)
                if current_after is not None and current_after_ts is not None:
                    rows = await db.list_event_logs_after_cursor(
                        after_created_at=float(current_after_ts),
                        after_id=int(current_after),
                        limit=page_limit,
                        agent_id=agent_id,
                        event_types=types,
                        event_kinds=kinds,
                        since=since,
                        until=until,
                    )
                else:
                    rows = await db.list_event_logs(
                        limit=page_limit,
                        agent_id=agent_id,
                        event_type=event_type,
                        event=event,
                        since=since,
                        until=until,
                        offset=current_offset,
                    )
                if not rows:
                    break
                for row in rows:
                    yield row
                fetched = len(rows)
                remaining -= fetched
                if current_after is not None:
                    last_id = rows[-1].get("id") if rows else None
                    last_ts = rows[-1].get("created_at") if rows else None
                    if last_id is not None:
                        try:
                            current_after = int(last_id)
                        except Exception:
                            current_after = current_after
                    if last_ts is not None:
                        try:
                            current_after_ts = float(last_ts)
                        except Exception:
                            current_after_ts = current_after_ts
                    if fetched < page_limit:
                        break
                else:
                    current_offset += fetched
                if fetched < page_limit:
                    break

        if fmt == "json":
            async def _gen_json():  # type: ignore[no-untyped-def]
                yield '{"ok":true,"events":['
                first = True
                async for row in _iter_rows():
                    chunk = json.dumps(row, separators=(",", ":"))
                    if first:
                        first = False
                        yield chunk
                    else:
                        yield "," + chunk
                yield "]}"

            return StreamingResponse(
                _gen_json(),
                media_type="application/json",
                headers={
                    "Content-Disposition": "attachment; filename=event_history.json"
                },
            )

        if fmt in ("ndjson", "jsonl"):
            async def _gen_ndjson():  # type: ignore[no-untyped-def]
                async for row in _iter_rows():
                    yield json.dumps(row, separators=(",", ":")) + "\n"

            return StreamingResponse(
                _gen_ndjson(),
                media_type="application/x-ndjson",
                headers={
                    "Content-Disposition": "attachment; filename=event_history.jsonl"
                },
            )

        async def _gen_csv():  # type: ignore[no-untyped-def]
            buf = io.StringIO()
            writer = csv.writer(buf)
            writer.writerow(
                [
                    "id",
                    "agent_id",
                    "created_at",
                    "event_type",
                    "event",
                    "payload",
                ]
            )
            yield buf.getvalue()
            buf.seek(0)
            buf.truncate(0)
            async for row in _iter_rows():
                payload = row.get("payload") or {}
                writer.writerow(
                    [
                        row.get("id"),
                        row.get("agent_id"),
                        row.get("created_at"),
                        row.get("event_type"),
                        row.get("event"),
                        json.dumps(payload, separators=(",", ":")),
                    ]
                )
                yield buf.getvalue()
                buf.seek(0)
                buf.truncate(0)

        return StreamingResponse(
            _gen_csv(),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=event_history.csv"},
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def _format_event(msg: EventMessage) -> str:
    payload = {
        "id": msg.id,
        "type": msg.type,
        "data": msg.data,
        "ts": msg.ts,
    }
    lines = []
    if msg.id is not None:
        lines.append(f"id: {msg.id}")
    lines.append(f"data: {json.dumps(payload, separators=(',', ':'))}")
    return "\n".join(lines) + "\n\n"


def _event_kind_from_data(data: Dict[str, Any]) -> str | None:
    raw = data.get("event")
    if raw is None:
        return None
    val = str(raw).strip()
    return val or None


def _should_persist_event(msg: EventMessage) -> bool:
    if msg.type in ("tick", "ready"):
        return False
    return True


async def _persist_event(state: AppState, msg: EventMessage) -> EventMessage:
    if not _should_persist_event(msg):
        return msg
    db = getattr(state, "db", None)
    if db is None:
        return msg
    event_kind = _event_kind_from_data(msg.data)
    try:
        event_id = await db.add_event_log(
            event_type=str(msg.type or "event"),
            event=event_kind,
            payload=dict(msg.data or {}),
            created_at=float(msg.ts),
        )
    except Exception:
        await _spool_event(state, msg)
        return msg
    if event_id is None:
        await _spool_event(state, msg)
        return msg
    return EventMessage(id=int(event_id), type=msg.type, data=msg.data, ts=msg.ts)


def _spool_stats_path(path: str) -> str:
    return f"{path}.stats.json"


def _spool_rotate_path(path: str) -> str:
    return f"{path}.1"


def _spool_paths(path: str) -> list[str]:
    return [_spool_rotate_path(path), path]


async def _load_spool_stats(state: AppState) -> None:
    global _SPOOL_STATS_LOADED
    if _SPOOL_STATS_LOADED:
        return
    settings = getattr(state, "settings", None)
    if settings is None:
        _SPOOL_STATS_LOADED = True
        return
    path = str(getattr(settings, "events_spool_path", "") or "").strip()
    if not path:
        _SPOOL_STATS_LOADED = True
        return
    stats_path = _spool_stats_path(path)
    try:
        async with aiofiles.open(stats_path, "r", encoding="utf-8") as f:
            raw = await f.read()
        data = json.loads(raw)
        if isinstance(data, dict):
            for key in ("queued_bytes", "queued_events", "dropped", "rotated"):
                try:
                    _SPOOL_STATS[key] = max(0, int(data.get(key, 0)))
                except Exception:
                    continue
    except FileNotFoundError:
        pass
    except Exception:
        pass
    _SPOOL_STATS_LOADED = True


async def _write_spool_stats(state: AppState, *, force: bool = False) -> None:
    global _SPOOL_STATS_WRITE_AT
    settings = getattr(state, "settings", None)
    if settings is None:
        return
    path = str(getattr(settings, "events_spool_path", "") or "").strip()
    if not path:
        return
    now = time.time()
    if not force and now - _SPOOL_STATS_WRITE_AT < _SPOOL_STATS_WRITE_MIN_INTERVAL_S:
        return
    stats_path = _spool_stats_path(path)
    try:
        await aio_os.makedirs(str(Path(stats_path).parent), exist_ok=True)
    except Exception:
        return
    payload = dict(_SPOOL_STATS)
    payload["updated_at"] = now
    try:
        async with aiofiles.open(stats_path, "w", encoding="utf-8") as f:
            await f.write(json.dumps(payload, separators=(",", ":")))
        _SPOOL_STATS_WRITE_AT = now
    except Exception:
        return


async def _drop_spool_file(state: AppState, path: str) -> None:
    try:
        exists = await aio_os.path.isfile(path)
    except Exception:
        exists = False
    if not exists:
        return
    try:
        size = await aio_os.path.getsize(path)
    except Exception:
        size = 0
    events = 0
    try:
        async with aiofiles.open(path, "r", encoding="utf-8") as f:
            async for line in f:
                if line.strip():
                    events += 1
    except Exception:
        events = 0
    try:
        await aio_os.remove(path)
    except Exception:
        pass
    _SPOOL_STATS["queued_bytes"] = max(
        0, int(_SPOOL_STATS.get("queued_bytes", 0)) - int(size or 0)
    )
    _SPOOL_STATS["queued_events"] = max(
        0, int(_SPOOL_STATS.get("queued_events", 0)) - int(events or 0)
    )
    _SPOOL_STATS["dropped"] = int(_SPOOL_STATS.get("dropped", 0)) + int(events or 0)
    await _write_spool_stats(state)


async def _rotate_spool_file(state: AppState, path: str) -> None:
    rotated = _spool_rotate_path(path)
    try:
        if await aio_os.path.isfile(rotated):
            await _drop_spool_file(state, rotated)
    except Exception:
        pass
    try:
        exists = await aio_os.path.isfile(path)
    except Exception:
        exists = False
    if not exists:
        return
    try:
        await aio_os.replace(path, rotated)
    except Exception:
        try:
            await aio_os.rename(path, rotated)
        except Exception:
            return
    _SPOOL_STATS["rotated"] = int(_SPOOL_STATS.get("rotated", 0)) + 1
    await _write_spool_stats(state)


async def _append_spool_line(
    state: AppState, *, path: str, line: str, max_bytes: int
) -> None:
    global _SPOOL_STATS_AT
    try:
        line_bytes = len(line.encode("utf-8"))
    except Exception:
        return
    if line_bytes <= 0:
        return
    if line_bytes > max_bytes:
        _SPOOL_STATS["dropped"] = int(_SPOOL_STATS.get("dropped", 0)) + 1
        _SPOOL_STATS_AT = time.time()
        await _write_spool_stats(state)
        return
    try:
        size = await aio_os.path.getsize(path)
    except FileNotFoundError:
        size = 0
    except Exception:
        size = 0
    if size + line_bytes > max_bytes:
        await _rotate_spool_file(state, path)
        try:
            size = await aio_os.path.getsize(path)
        except FileNotFoundError:
            size = 0
        except Exception:
            size = 0
    if size + line_bytes > max_bytes:
        _SPOOL_STATS["dropped"] = int(_SPOOL_STATS.get("dropped", 0)) + 1
        _SPOOL_STATS_AT = time.time()
        await _write_spool_stats(state)
        return
    try:
        async with aiofiles.open(path, "a", encoding="utf-8") as f:
            await f.write(line)
        _SPOOL_STATS["queued_bytes"] = int(_SPOOL_STATS.get("queued_bytes", 0)) + line_bytes
        _SPOOL_STATS["queued_events"] = int(_SPOOL_STATS.get("queued_events", 0)) + 1
        _SPOOL_STATS_AT = time.time()
        await _write_spool_stats(state)
    except Exception:
        _SPOOL_STATS["dropped"] = int(_SPOOL_STATS.get("dropped", 0)) + 1
        _SPOOL_STATS_AT = time.time()
        await _write_spool_stats(state)
        return


async def _spool_event(state: AppState, msg: EventMessage) -> None:
    settings = getattr(state, "settings", None)
    if settings is None:
        return
    path = str(getattr(settings, "events_spool_path", "") or "").strip()
    if not path:
        return
    max_mb = int(getattr(settings, "events_spool_max_mb", 0) or 0)
    if max_mb <= 0:
        return
    max_bytes = max_mb * 1024 * 1024
    payload = msg.data if isinstance(msg.data, dict) else {}
    record = {
        "event_type": str(msg.type or "event"),
        "event": _event_kind_from_data(payload),
        "payload": dict(payload),
        "created_at": float(msg.ts),
    }
    line = json.dumps(record, separators=(",", ":"), ensure_ascii=True) + "\n"
    async with _SPOOL_LOCK:
        await _load_spool_stats(state)
        await _refresh_spool_stats(state)
        try:
            await aio_os.makedirs(str(Path(path).parent), exist_ok=True)
        except Exception:
            return
        await _append_spool_line(state, path=path, line=line, max_bytes=max_bytes)


async def flush_event_spool(state: AppState) -> int:
    global _SPOOL_STATS_AT
    settings = getattr(state, "settings", None)
    if settings is None:
        return 0
    path = str(getattr(settings, "events_spool_path", "") or "").strip()
    if not path:
        return 0
    max_mb = int(getattr(settings, "events_spool_max_mb", 0) or 0)
    if max_mb <= 0:
        return 0
    db = getattr(state, "db", None)
    if db is None:
        return 0
    async with _SPOOL_LOCK:
        await _load_spool_stats(state)
        await _refresh_spool_stats(state)
        spool_paths = _spool_paths(path)
        lines: list[str] = []
        for p in spool_paths:
            try:
                if not await aio_os.path.isfile(p):
                    continue
                async with aiofiles.open(p, "r", encoding="utf-8") as f:
                    lines.extend(await f.readlines())
            except Exception:
                continue
        if not lines:
            _SPOOL_STATS["queued_bytes"] = 0
            _SPOOL_STATS["queued_events"] = 0
            _SPOOL_STATS_AT = time.time()
            await _write_spool_stats(state, force=True)
            return 0
        remaining: list[str] = []
        inserted = 0
        invalid = 0
        for raw in lines:
            line = raw.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except Exception:
                invalid += 1
                continue
            if not isinstance(payload, dict):
                invalid += 1
                continue
            try:
                event_type = str(payload.get("event_type") or "event")
                event_kind = payload.get("event")
                created_at = float(payload.get("created_at") or time.time())
                data = payload.get("payload")
                if not isinstance(data, dict):
                    data = {}
            except Exception:
                invalid += 1
                continue
            try:
                event_id = await db.add_event_log(
                    event_type=event_type,
                    event=event_kind,
                    payload=dict(data),
                    created_at=created_at,
                )
            except Exception:
                remaining.append(line + "\n")
                continue
            if event_id is None:
                remaining.append(line + "\n")
            else:
                inserted += 1
        try:
            for p in spool_paths:
                try:
                    if await aio_os.path.isfile(p):
                        await aio_os.remove(p)
                except Exception:
                    pass
            _SPOOL_STATS["queued_bytes"] = 0
            _SPOOL_STATS["queued_events"] = 0
            if invalid:
                _SPOOL_STATS["dropped"] = int(_SPOOL_STATS.get("dropped", 0)) + invalid
            if remaining:
                for line in remaining:
                    await _append_spool_line(
                        state, path=path, line=line, max_bytes=max_mb * 1024 * 1024
                    )
            _SPOOL_STATS_AT = time.time()
            await _write_spool_stats(state, force=True)
        except Exception:
            pass
        return inserted


async def get_spool_stats(state: AppState) -> Dict[str, int]:
    async with _SPOOL_LOCK:
        await _refresh_spool_stats(state)
        return dict(_SPOOL_STATS)


async def _refresh_spool_stats(state: AppState) -> None:
    global _SPOOL_STATS_AT
    now = time.time()
    if now - _SPOOL_STATS_AT < _SPOOL_STATS_TTL_S and _SPOOL_STATS_AT > 0:
        return
    await _load_spool_stats(state)
    settings = getattr(state, "settings", None)
    if settings is None:
        _SPOOL_STATS["queued_bytes"] = 0
        _SPOOL_STATS["queued_events"] = 0
        _SPOOL_STATS_AT = now
        await _write_spool_stats(state)
        return
    path = str(getattr(settings, "events_spool_path", "") or "").strip()
    max_mb = int(getattr(settings, "events_spool_max_mb", 0) or 0)
    if not path or max_mb <= 0:
        _SPOOL_STATS["queued_bytes"] = 0
        _SPOOL_STATS["queued_events"] = 0
        _SPOOL_STATS_AT = now
        await _write_spool_stats(state)
        return
    total_bytes = 0
    total_events = 0
    for p in _spool_paths(path):
        try:
            if not await aio_os.path.isfile(p):
                continue
        except Exception:
            continue
        try:
            size = await aio_os.path.getsize(p)
        except Exception:
            size = 0
        total_bytes += int(size or 0)
        try:
            async with aiofiles.open(p, "r", encoding="utf-8") as f:
                async for line in f:
                    if line.strip():
                        total_events += 1
        except Exception:
            continue
    _SPOOL_STATS["queued_bytes"] = int(total_bytes)
    _SPOOL_STATS["queued_events"] = int(total_events)
    _SPOOL_STATS_AT = now
    await _write_spool_stats(state)


def _parse_last_event_id(request: Request) -> int | None:
    raw = request.headers.get("last-event-id") or request.query_params.get(
        "last_event_id"
    )
    if not raw:
        return None
    try:
        val = int(str(raw).strip())
    except Exception:
        return None
    if val < 0:
        return None
    return val


def _parse_event_types(request: Request) -> set[str] | None:
    try:
        raw_values = request.query_params.getlist("types")
    except Exception:
        raw_values = []
    if not raw_values:
        return None
    out: set[str] = set()
    for raw in raw_values:
        for part in str(raw or "").split(","):
            p = part.strip().lower()
            if not p:
                continue
            if p in ("*", "all"):
                return None
            out.add(p)
    return out or None


def _parse_event_kinds(request: Request) -> set[str] | None:
    raw_values: list[str] = []
    try:
        raw_values.extend(request.query_params.getlist("event"))
    except Exception:
        pass
    try:
        raw_values.extend(request.query_params.getlist("events"))
    except Exception:
        pass
    if not raw_values:
        return None
    out: set[str] = set()
    for raw in raw_values:
        for part in str(raw or "").split(","):
            p = part.strip().lower()
            if not p:
                continue
            if p in ("*", "all"):
                return None
            out.add(p)
    return out or None


def _parse_csv_filter(raw: str | None) -> list[str] | None:
    if raw is None:
        return None
    out: list[str] = []
    for part in str(raw or "").split(","):
        p = part.strip()
        if not p:
            continue
        if p in ("*", "all"):
            return None
        out.append(p)
    return out or None


def _parse_after_id(raw: int | None) -> int | None:
    if raw is None:
        return None
    try:
        val = int(raw)
    except Exception:
        return None
    if val < 0:
        return None
    return val


async def _resolve_event_cursor(
    db: Any, after_id: int
) -> tuple[float, int] | None:
    try:
        row = await db.get_event_log_by_id(event_id=int(after_id))
    except Exception:
        return None
    if not row:
        return None
    try:
        created_at = float(row.get("created_at") or 0.0)
        row_id = int(row.get("id") or after_id)
    except Exception:
        return None
    return created_at, row_id


def _clamp_limit(limit: int, *, default: int = 200, max_limit: int = 2000) -> int:
    try:
        n = int(limit)
    except Exception:
        n = default
    return max(1, min(int(max_limit), n))


async def _history_from_db(
    state: AppState,
    *,
    last_event_id: int | None,
    allowed_types: set[str] | None,
    allowed_kinds: set[str] | None = None,
    limit: int = 500,
) -> list[EventMessage]:
    if last_event_id is None:
        return []
    db = getattr(state, "db", None)
    if db is None:
        return []
    try:
        types = sorted(list(allowed_types)) if allowed_types else None
        kinds = sorted(list(allowed_kinds)) if allowed_kinds else None
        cursor = await _resolve_event_cursor(db, int(last_event_id))
        if cursor is not None:
            rows = await db.list_event_logs_after_cursor(
                after_created_at=cursor[0],
                after_id=cursor[1],
                limit=_clamp_limit(limit, default=500, max_limit=2000),
                agent_id=str(getattr(state.settings, "agent_id", "")) or None,
                event_types=types,
                event_kinds=kinds,
            )
        else:
            rows = await db.list_event_logs_after_id(
                last_id=int(last_event_id),
                limit=_clamp_limit(limit, default=500, max_limit=2000),
                agent_id=str(getattr(state.settings, "agent_id", "")) or None,
                event_types=types,
                event_kinds=kinds,
            )
    except Exception:
        return []

    out: list[EventMessage] = []
    for row in rows:
        try:
            event_id = row.get("id")
            event_type = str(row.get("event_type") or "event")
            payload = row.get("payload") or {}
            created_at = float(row.get("created_at") or 0.0)
            if not isinstance(payload, dict):
                payload = {}
            if event_id is None:
                continue
            out.append(
                EventMessage(
                    id=int(event_id),
                    type=event_type,
                    data=dict(payload),
                    ts=created_at,
                )
            )
        except Exception:
            continue
    return out


def _merge_history(
    last_event_id: int | None,
    *,
    db_history: list[EventMessage],
    bus_history: list[EventMessage],
) -> list[EventMessage]:
    seen: set[int] = set()
    out: list[EventMessage] = []
    last_id = int(last_event_id or 0)
    for msg in db_history + bus_history:
        if msg.id is not None:
            if msg.id <= last_id:
                continue
            if msg.id in seen:
                continue
            seen.add(msg.id)
        out.append(msg)
    out.sort(
        key=lambda m: (
            1 if m.id is None else 0,
            int(m.id or 0),
            float(m.ts or 0.0),
        )
    )
    return out
