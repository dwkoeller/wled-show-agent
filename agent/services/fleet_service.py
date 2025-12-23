from __future__ import annotations

import asyncio
import csv
import io
import json
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from fastapi import Depends, HTTPException, Request
from fastapi.responses import PlainTextResponse, Response

from models.requests import (
    FleetApplyRandomLookRequest,
    FleetCrossfadeRequest,
    FleetInvokeRequest,
    FleetResolveRequest,
    FleetOverrideRequest,
    FleetStopAllRequest,
)
from services import a2a_service
from services.audit_logger import log_event
from services.auth_service import require_a2a_auth, require_admin
from services.runtime_state_service import persist_runtime_state
from services.state import AppState, get_state
from utils.outbound_http import request_with_retry, retry_policy_from_settings


def _peer_headers(state: AppState) -> Dict[str, str]:
    key = state.settings.a2a_api_key
    return {"X-A2A-Key": str(key)} if key else {}


_FLEET_HEALTH_LOCK = asyncio.Lock()
_FLEET_HEALTH_CACHE: Dict[str, Any] = {}


async def _peer_get_json(
    *,
    state: AppState,
    peer: Any,
    path: str,
    timeout_s: float,
) -> Dict[str, Any]:
    base_url = str(getattr(peer, "base_url", "") or "").rstrip("/")
    url = base_url + path
    client = state.peer_http
    if client is None:
        return {"ok": False, "error": "peer_http is not initialized"}
    try:
        resp = await request_with_retry(
            client=client,
            method="GET",
            url=url,
            target_kind="peer",
            target=str(getattr(peer, "name", "") or base_url),
            timeout_s=float(timeout_s),
            retry=retry_policy_from_settings(state.settings),
            headers=_peer_headers(state),
        )
    except Exception as e:
        return {"ok": False, "error": str(e)}

    try:
        body = resp.json()
    except Exception:
        body = {"ok": False, "error": resp.text[:300]}
    if (
        resp.status_code >= 400
        and isinstance(body, dict)
        and body.get("ok") is not False
    ):
        body = {"ok": False, "error": f"HTTP {resp.status_code}: {resp.text[:300]}"}
    return (
        body
        if isinstance(body, dict)
        else {"ok": False, "error": "Non-object response"}
    )


async def _peer_post_json(
    *,
    state: AppState,
    peer: Any,
    path: str,
    payload: Dict[str, Any],
    timeout_s: float,
) -> Dict[str, Any]:
    base_url = str(getattr(peer, "base_url", "") or "").rstrip("/")
    url = base_url + path
    client = state.peer_http
    if client is None:
        return {"ok": False, "error": "peer_http is not initialized"}
    try:
        resp = await request_with_retry(
            client=client,
            method="POST",
            url=url,
            target_kind="peer",
            target=str(getattr(peer, "name", "") or base_url),
            timeout_s=float(timeout_s),
            retry=retry_policy_from_settings(state.settings),
            headers=_peer_headers(state),
            json_body=payload,
        )
    except Exception as e:
        return {"ok": False, "error": str(e)}

    try:
        body = resp.json()
    except Exception:
        body = {"ok": False, "error": resp.text[:300]}
    if (
        resp.status_code >= 400
        and isinstance(body, dict)
        and body.get("ok") is not False
    ):
        body = {"ok": False, "error": f"HTTP {resp.status_code}: {resp.text[:300]}"}
    return (
        body
        if isinstance(body, dict)
        else {"ok": False, "error": "Non-object response"}
    )


async def _peer_supported_actions(
    *, state: AppState, peer: Any, timeout_s: float
) -> set[str]:
    card = await _peer_get_json(
        state=state, peer=peer, path="/v1/a2a/card", timeout_s=timeout_s
    )
    if not isinstance(card, dict) or card.get("ok") is not True:
        return set()
    agent = card.get("agent") or {}
    caps = agent.get("capabilities") or []
    actions: set[str] = set()
    if isinstance(caps, list):
        for c in caps:
            if isinstance(c, dict) and "action" in c:
                actions.add(str(c.get("action")))
            elif isinstance(c, str):
                actions.add(c)
    return actions


@dataclass(frozen=True)
class _DiscoveredPeer:
    name: str
    base_url: str


async def _load_agent_overrides(state: AppState) -> dict[str, dict[str, Any]]:
    db = getattr(state, "db", None)
    if db is None:
        return {}
    try:
        rows = await db.list_agent_overrides(limit=2000)
    except Exception:
        return {}
    out: dict[str, dict[str, Any]] = {}
    for row in rows or []:
        try:
            aid = str(row.get("agent_id") or "").strip()
            if not aid:
                continue
            out[aid] = dict(row)
        except Exception:
            continue
    return out


def _override_role(raw: dict[str, Any] | None) -> str | None:
    if not raw:
        return None
    val = str(raw.get("role") or "").strip()
    return val or None


def _override_tags(raw: dict[str, Any] | None) -> list[str] | None:
    if raw is None:
        return None
    tags = raw.get("tags")
    if isinstance(tags, list):
        return [str(t).strip() for t in tags if str(t).strip()]
    return []


async def _select_peers(state: AppState, targets: Optional[List[str]]) -> List[Any]:
    peers = state.peers or {}
    if not targets:
        return list(peers.values())
    selected: Dict[str, Any] = {}
    seen_urls: set[str] = set()

    raw_targets = [str(t).strip() for t in targets if str(t).strip()]
    if not raw_targets:
        return []

    db_discovery = bool(getattr(state.settings, "fleet_db_discovery_enabled", True))
    overrides_map: dict[str, dict[str, Any]] = {}
    if db_discovery:
        overrides_map = await _load_agent_overrides(state)

    # Targets support:
    # - configured peer names: "roofline1"
    # - explicit agent_id discovery (SQL heartbeat): "star_wled"
    # - role selector: "role:roofline"
    # - tag selector: "tag:outside"
    # - wildcard selector: "*" or "all" (all online discovered agents)
    explicit: List[str] = []
    roles: set[str] = set()
    tags: set[str] = set()
    include_all_discovered = False

    for t in raw_targets:
        if t in ("*", "all"):
            include_all_discovered = True
            continue
        if t.startswith("role:"):
            v = t[len("role:") :].strip()
            if v:
                roles.add(v)
            continue
        if t.startswith("tag:"):
            v = t[len("tag:") :].strip()
            if v:
                tags.add(v)
            continue
        explicit.append(t)

    missing: List[str] = []
    if include_all_discovered and not db_discovery:
        # Without DB discovery, treat "*" as "all configured peers" (equivalent to omitting targets).
        for aid, p in peers.items():
            try:
                u = str(getattr(p, "base_url", "") or "").rstrip("/")
                if u:
                    seen_urls.add(u)
            except Exception:
                pass
            selected[str(getattr(p, "name", aid) or aid)] = p

    for aid in explicit:
        if aid in peers:
            p = peers[aid]
            try:
                u = str(getattr(p, "base_url", "") or "").rstrip("/")
                if u:
                    seen_urls.add(u)
            except Exception:
                pass
            selected[str(getattr(p, "name", aid) or aid)] = p
        else:
            missing.append(str(aid))

    db = getattr(state, "db", None)
    if db is not None and db_discovery and (include_all_discovered or roles or tags):
        stale_after_s = float(getattr(state.settings, "fleet_stale_after_s", 30.0))
        now = time.time()
        try:
            rows = await db.list_agent_heartbeats(limit=2000)
        except Exception:
            rows = []

        for rec in rows:
            if not isinstance(rec, dict):
                continue
            aid = str(rec.get("agent_id") or "").strip()
            if not aid or aid == str(state.settings.agent_id):
                continue

            updated_at = float(rec.get("updated_at") or 0.0)
            age_s = max(0.0, now - updated_at) if updated_at else None
            online = bool(age_s is not None and age_s <= stale_after_s)
            if not online:
                continue

            role = str(rec.get("role") or "").strip()
            payload = rec.get("payload") or {}
            if not isinstance(payload, dict):
                payload = {}
            base_url = str(payload.get("base_url") or "").strip().rstrip("/")
            if not base_url:
                continue
            if not (base_url.startswith("http://") or base_url.startswith("https://")):
                continue
            if base_url in seen_urls:
                continue
            raw_tags = payload.get("tags")
            rec_tags: set[str] = set()
            if isinstance(raw_tags, list):
                rec_tags = {
                    str(x).strip() for x in raw_tags if x is not None and str(x).strip()
                }
            override = overrides_map.get(aid)
            role_override = _override_role(override)
            tags_override = _override_tags(override)
            if role_override is not None:
                role = role_override
            if tags_override is not None:
                rec_tags = {t for t in tags_override if t}

            matches = include_all_discovered
            if (not matches) and roles and role in roles:
                matches = True
            if (not matches) and tags and (rec_tags & tags):
                matches = True
            if not matches:
                continue

            if aid not in selected:
                seen_urls.add(base_url)
                selected[aid] = _DiscoveredPeer(name=aid, base_url=base_url)

    # If explicit targets include agent_ids not in A2A_PEERS, try DB discovery via heartbeats.
    if db is not None and db_discovery:
        for aid in missing:
            if aid in selected:
                continue
            try:
                hb = await db.get_agent_heartbeat(agent_id=str(aid))
            except Exception:
                hb = None
            if not hb:
                continue
            payload = hb.get("payload") or {}
            if not isinstance(payload, dict):
                continue
            base_url = str(payload.get("base_url") or "").strip().rstrip("/")
            if not base_url:
                continue
            if not (base_url.startswith("http://") or base_url.startswith("https://")):
                continue
            if base_url in seen_urls:
                continue
            seen_urls.add(base_url)
            selected[aid] = _DiscoveredPeer(name=str(aid), base_url=base_url)

    return list(selected.values())


async def fleet_peers(
    request: Request,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    try:
        peers = state.peers or {}
        s = state.settings
        payload = {
            "ok": True,
            "self": {"id": s.agent_id, "name": s.agent_name, "role": s.agent_role},
            "peers": [
                {"name": getattr(p, "name", ""), "base_url": getattr(p, "base_url", "")}
                for p in peers.values()
            ],
        }
        await log_event(
            state,
            action="fleet.peers",
            ok=True,
            payload={"count": len(payload.get("peers") or [])},
            request=request,
        )
        return payload
    except Exception as e:
        await log_event(
            state, action="fleet.peers", ok=False, error=str(e), request=request
        )
        raise HTTPException(status_code=500, detail=str(e))


async def fleet_status(
    request: Request,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
    stale_after_s: float | None = None,
    limit: int = 200,
    include_payload: bool = False,
) -> Dict[str, Any]:
    """
    Fleet status derived from SQL heartbeats (no fanout).
    """
    db = getattr(state, "db", None)
    if db is None:
        await log_event(
            state,
            action="fleet.status",
            ok=False,
            error="Database not initialized",
            request=request,
        )
        return {"ok": False, "error": "Database not initialized"}

    try:
        now = time.time()
        overrides_map = await _load_agent_overrides(state)
        stale = max(
            1.0,
            (
                float(stale_after_s)
                if stale_after_s is not None
                else float(getattr(state.settings, "fleet_stale_after_s", 30.0))
            ),
        )

        rows = await db.list_agent_heartbeats(limit=max(1, int(limit)))
        by_id: Dict[str, Dict[str, Any]] = {str(r.get("agent_id")): dict(r) for r in rows}

        peers = state.peers or {}
        configured_ids = {str(state.settings.agent_id)} | {str(k) for k in peers.keys()}

        def _format(
            aid: str, rec: Dict[str, Any] | None, *, configured: bool
        ) -> Dict[str, Any]:
            if not rec:
                override = overrides_map.get(aid)
                role_override = _override_role(override)
                tags_override = _override_tags(override)
                return {
                    "agent_id": aid,
                    "configured": configured,
                    "online": False,
                    "age_s": None,
                    "updated_at": None,
                    "started_at": None,
                    "name": None,
                    "role": None,
                    "controller_kind": None,
                    "version": None,
                    "base_url": None,
                    "tags": None,
                    "role_override": role_override,
                    "tags_override": tags_override,
                    "role_effective": role_override,
                    "tags_effective": tags_override,
                }
            updated_at = float(rec.get("updated_at") or 0.0)
            age_s = max(0.0, now - updated_at) if updated_at else None
            payload = dict(rec.get("payload") or {})
            base_url = None
            tags = None
            try:
                base_url = str(payload.get("base_url") or "").strip() or None
            except Exception:
                base_url = None
            try:
                raw_tags = payload.get("tags")
                if isinstance(raw_tags, list):
                    tags = [str(x) for x in raw_tags if x is not None]
            except Exception:
                tags = None
            override = overrides_map.get(aid)
            role_override = _override_role(override)
            tags_override = _override_tags(override)
            role_effective = (
                role_override if role_override is not None else rec.get("role")
            )
            tags_effective = tags_override if tags_override is not None else tags
            out: Dict[str, Any] = {
                "agent_id": aid,
                "configured": configured,
                "online": bool(age_s is not None and age_s <= stale),
                "age_s": age_s,
                "updated_at": updated_at or None,
                "started_at": float(rec.get("started_at") or 0.0) or None,
                "name": rec.get("name"),
                "role": rec.get("role"),
                "controller_kind": rec.get("controller_kind"),
                "version": rec.get("version"),
                "base_url": base_url,
                "tags": tags,
                "capabilities": (
                    payload.get("capabilities") if isinstance(payload, dict) else None
                ),
                "role_override": role_override,
                "tags_override": tags_override,
                "role_effective": role_effective,
                "tags_effective": tags_effective,
            }
            if include_payload:
                out["payload"] = payload
            return out

        agents: List[Dict[str, Any]] = []

        # Configured agents first (self + peers).
        for aid in sorted(configured_ids):
            agents.append(_format(aid, by_id.get(aid), configured=True))

        # Then any other agents present in the DB.
        for aid, rec in sorted(by_id.items()):
            if aid in configured_ids:
                continue
            agents.append(_format(aid, rec, configured=False))

        online = sum(1 for a in agents if a.get("online"))
        configured = sum(1 for a in agents if a.get("configured"))

        payload = {
            "ok": True,
            "now": now,
            "stale_after_s": stale,
            "summary": {"agents": len(agents), "online": online, "configured": configured},
            "agents": agents,
        }
        await log_event(
            state,
            action="fleet.status",
            ok=True,
            payload={
                "limit": int(limit),
                "include_payload": bool(include_payload),
                "stale_after_s": float(stale),
            },
            request=request,
        )
        return payload
    except Exception as e:
        await log_event(
            state,
            action="fleet.status",
            ok=False,
            error=str(e),
            payload={"limit": int(limit)},
            request=request,
        )
        raise HTTPException(status_code=500, detail=str(e))


async def fleet_overrides_list(
    request: Request,
    limit: int = 200,
    _: Dict[str, Any] = Depends(require_admin),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    db = getattr(state, "db", None)
    if db is None:
        await log_event(
            state,
            action="fleet.overrides.list",
            ok=False,
            error="Database not initialized",
            request=request,
        )
        raise HTTPException(status_code=503, detail="Database not initialized")
    try:
        lim = max(1, int(limit))
        rows = await db.list_agent_overrides(limit=lim)
        await log_event(
            state,
            action="fleet.overrides.list",
            ok=True,
            payload={"count": len(rows), "limit": lim},
            request=request,
        )
        return {"ok": True, "overrides": rows, "count": len(rows), "limit": lim}
    except Exception as e:
        await log_event(
            state,
            action="fleet.overrides.list",
            ok=False,
            error=str(e),
            request=request,
        )
        raise HTTPException(status_code=500, detail=str(e))


def _parse_override_tags(raw: str | None) -> list[str] | None:
    if raw is None:
        return None
    text = str(raw).strip()
    if text == "":
        return None
    if text.startswith("["):
        try:
            data = json.loads(text)
            if isinstance(data, list):
                return [str(x).strip() for x in data if str(x).strip()]
        except Exception:
            return None
    tags = [t.strip() for t in text.split(",") if t.strip()]
    return tags or None


async def fleet_overrides_export(
    request: Request,
    format: str = "csv",
    limit: int = 2000,
    _: Dict[str, Any] = Depends(require_admin),
    state: AppState = Depends(get_state),
) -> Response:
    db = getattr(state, "db", None)
    if db is None:
        await log_event(
            state,
            action="fleet.overrides.export",
            ok=False,
            error="Database not initialized",
            request=request,
        )
        raise HTTPException(status_code=503, detail="Database not initialized")
    try:
        lim = max(1, min(20000, int(limit)))
        rows = await db.list_agent_overrides(limit=lim)
        fmt = str(format or "csv").strip().lower()
        if fmt == "json":
            payload = {"ok": True, "overrides": rows, "count": len(rows)}
            await log_event(
                state,
                action="fleet.overrides.export",
                ok=True,
                payload={"format": "json", "count": len(rows)},
                request=request,
            )
            return Response(
                content=json.dumps(payload, separators=(",", ":")),
                media_type="application/json",
                headers={
                    "Content-Disposition": "attachment; filename=fleet_overrides.json"
                },
            )

        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(["agent_id", "role", "tags", "updated_at", "updated_by"])
        for row in rows:
            tags = row.get("tags") or []
            writer.writerow(
                [
                    row.get("agent_id"),
                    row.get("role") or "",
                    json.dumps(tags, separators=(",", ":")),
                    row.get("updated_at"),
                    row.get("updated_by") or "",
                ]
            )
        await log_event(
            state,
            action="fleet.overrides.export",
            ok=True,
            payload={"format": "csv", "count": len(rows)},
            request=request,
        )
        return Response(
            content=buf.getvalue(),
            media_type="text/csv",
            headers={
                "Content-Disposition": "attachment; filename=fleet_overrides.csv"
            },
        )
    except Exception as e:
        await log_event(
            state,
            action="fleet.overrides.export",
            ok=False,
            error=str(e),
            request=request,
        )
        raise HTTPException(status_code=500, detail=str(e))


async def fleet_overrides_template(
    request: Request,
    _: Dict[str, Any] = Depends(require_admin),
    state: AppState = Depends(get_state),
) -> Response:
    payload = "agent_id,role,tags\nexample-agent,roofline,\"[\\\"tag1\\\",\\\"tag2\\\"]\"\n"
    await log_event(
        state,
        action="fleet.overrides.template",
        ok=True,
        payload={},
        request=request,
    )
    return Response(
        content=payload,
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=fleet_overrides_template.csv"},
    )


async def fleet_overrides_import(
    request: Request,
    format: str = "csv",
    dry_run: bool = False,
    _: Dict[str, Any] = Depends(require_admin),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    db = getattr(state, "db", None)
    if db is None:
        await log_event(
            state,
            action="fleet.overrides.import",
            ok=False,
            error="Database not initialized",
            request=request,
        )
        raise HTTPException(status_code=503, detail="Database not initialized")
    try:
        fmt = str(format or "").strip().lower()
        body = await request.body()
        if not body:
            raise HTTPException(status_code=400, detail="Empty body")
        updated_by = str(getattr(request.state, "user", None) or "admin")
        errors: list[str] = []
        processed = 0
        upserted = 0
        changes: list[dict[str, Any]] = []
        change_counts = {"insert": 0, "update": 0, "noop": 0, "error": 0}

        if fmt == "json":
            payload = json.loads(body.decode("utf-8"))
            if isinstance(payload, dict) and "overrides" in payload:
                payload = payload.get("overrides")
            if not isinstance(payload, list):
                raise HTTPException(status_code=400, detail="Expected JSON list")
            items = payload
        else:
            text = body.decode("utf-8", errors="replace")
            reader = csv.DictReader(io.StringIO(text))
            items = list(reader)

        def _normalize_tags(tags: list[str] | None) -> list[str]:
            return sorted({str(t).strip() for t in (tags or []) if str(t).strip()})

        for idx, item in enumerate(items):
            if not isinstance(item, dict):
                errors.append(f"row {idx}: invalid record")
                change_counts["error"] += 1
                continue
            agent_id = str(item.get("agent_id") or "").strip()
            if not agent_id:
                errors.append(f"row {idx}: missing agent_id")
                change_counts["error"] += 1
                continue
            role_raw = item.get("role")
            role = str(role_raw).strip() if role_raw is not None else None
            if role == "":
                role = None
            tags_val = item.get("tags")
            if isinstance(tags_val, list):
                tags = [str(t).strip() for t in tags_val if str(t).strip()]
            else:
                tags = _parse_override_tags(tags_val)
            try:
                before = await db.get_agent_override(agent_id=agent_id)
                before_role = str(before.get("role") or "").strip() if before else None
                before_role = before_role if before_role else None
                before_tags = (
                    [str(t).strip() for t in (before.get("tags") or [])]
                    if before
                    else []
                )
                if before is None:
                    action = "insert" if (role or tags) else "noop"
                else:
                    if before_role == (role or None) and _normalize_tags(before_tags) == _normalize_tags(tags):
                        action = "noop"
                    else:
                        action = "update"

                if dry_run:
                    if len(changes) < 200:
                        changes.append(
                            {
                                "agent_id": agent_id,
                                "action": action,
                                "role_before": before_role,
                                "tags_before": _normalize_tags(before_tags),
                                "role_after": role,
                                "tags_after": _normalize_tags(tags),
                            }
                        )
                    change_counts[action] = change_counts.get(action, 0) + 1
                else:
                    if action != "noop":
                        await db.upsert_agent_override(
                            agent_id=agent_id,
                            role=role,
                            tags=tags,
                            updated_by=updated_by,
                        )
                        upserted += 1
                    change_counts[action] = change_counts.get(action, 0) + 1
            except Exception as e:
                errors.append(f"row {idx}: {e}")
                change_counts["error"] += 1
            processed += 1

        await log_event(
            state,
            action="fleet.overrides.import",
            ok=True,
            payload={
                "processed": processed,
                "upserted": upserted,
                "errors": len(errors),
                "dry_run": bool(dry_run),
            },
            request=request,
        )
        return {
            "ok": True,
            "processed": processed,
            "upserted": upserted,
            "errors": errors[:50],
            "dry_run": bool(dry_run),
            "changes": changes if dry_run else [],
            "change_summary": change_counts,
        }
    except HTTPException as e:
        await log_event(
            state,
            action="fleet.overrides.import",
            ok=False,
            error=str(getattr(e, "detail", e)),
            request=request,
        )
        raise
    except Exception as e:
        await log_event(
            state,
            action="fleet.overrides.import",
            ok=False,
            error=str(e),
            request=request,
        )
        raise HTTPException(status_code=500, detail=str(e))


async def fleet_override_update(
    agent_id: str,
    req: FleetOverrideRequest,
    request: Request,
    _: Dict[str, Any] = Depends(require_admin),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    db = getattr(state, "db", None)
    if db is None:
        await log_event(
            state,
            action="fleet.overrides.update",
            ok=False,
            error="Database not initialized",
            request=request,
        )
        raise HTTPException(status_code=503, detail="Database not initialized")
    try:
        rec = await db.upsert_agent_override(
            agent_id=str(agent_id or "").strip(),
            role=req.role,
            tags=req.tags,
            updated_by=str(getattr(request.state, "user", None) or "admin"),
        )
        await log_event(
            state,
            action="fleet.overrides.update",
            ok=True,
            resource=str(agent_id),
            payload={"role": req.role, "tags": req.tags},
            request=request,
        )
        return {"ok": True, "override": rec}
    except Exception as e:
        await log_event(
            state,
            action="fleet.overrides.update",
            ok=False,
            error=str(e),
            resource=str(agent_id),
            request=request,
        )
        raise HTTPException(status_code=500, detail=str(e))


async def fleet_override_delete(
    agent_id: str,
    request: Request,
    _: Dict[str, Any] = Depends(require_admin),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    db = getattr(state, "db", None)
    if db is None:
        await log_event(
            state,
            action="fleet.overrides.delete",
            ok=False,
            error="Database not initialized",
            request=request,
        )
        raise HTTPException(status_code=503, detail="Database not initialized")
    try:
        removed = await db.delete_agent_override(agent_id=str(agent_id or "").strip())
        await log_event(
            state,
            action="fleet.overrides.delete",
            ok=bool(removed),
            resource=str(agent_id),
            payload={"removed": bool(removed)},
            request=request,
        )
        return {"ok": True, "removed": bool(removed)}
    except Exception as e:
        await log_event(
            state,
            action="fleet.overrides.delete",
            ok=False,
            error=str(e),
            resource=str(agent_id),
            request=request,
        )
        raise HTTPException(status_code=500, detail=str(e))


async def fleet_health(
    request: Request,
    include_self: bool = True,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    ttl_s = float(getattr(state.settings, "fleet_health_cache_ttl_s", 15.0))
    now = time.time()
    async with _FLEET_HEALTH_LOCK:
        cached = _FLEET_HEALTH_CACHE.get("payload")
        ts = float(_FLEET_HEALTH_CACHE.get("ts") or 0.0)
        cached_self = bool(_FLEET_HEALTH_CACHE.get("include_self", True))
        if (
            cached is not None
            and ttl_s > 0
            and now - ts < ttl_s
            and cached_self == bool(include_self)
        ):
            payload = dict(cached)
            payload["cached"] = True
            return payload

    db = getattr(state, "db", None)
    overrides_map = await _load_agent_overrides(state) if db is not None else {}
    stale_after_s = float(getattr(state.settings, "fleet_stale_after_s", 30.0))
    hb_by_id: dict[str, dict[str, Any]] = {}
    hb_by_base: dict[str, dict[str, Any]] = {}
    if db is not None:
        try:
            rows = await db.list_agent_heartbeats(limit=2000)
        except Exception:
            rows = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            aid = str(row.get("agent_id") or "").strip()
            if aid:
                hb_by_id[aid] = dict(row)
            payload = row.get("payload") or {}
            if isinstance(payload, dict):
                bu = str(payload.get("base_url") or "").strip().rstrip("/")
                if bu:
                    hb_by_base[bu] = dict(row)

    def _merge_meta(
        *, agent_id: str | None, base_url: str | None
    ) -> Dict[str, Any]:
        rec = None
        if agent_id:
            rec = hb_by_id.get(agent_id)
        if rec is None and base_url:
            rec = hb_by_base.get(base_url)
        if not rec:
            return {
                "online": None,
                "updated_at": None,
                "role": None,
                "tags": None,
                "role_override": None,
                "tags_override": None,
                "role_effective": None,
                "tags_effective": None,
            }
        updated_at = float(rec.get("updated_at") or 0.0) or None
        age_s = max(0.0, now - updated_at) if updated_at else None
        payload = rec.get("payload") or {}
        if not isinstance(payload, dict):
            payload = {}
        raw_tags = payload.get("tags")
        tags = (
            [str(x) for x in raw_tags if x is not None]
            if isinstance(raw_tags, list)
            else None
        )
        override = overrides_map.get(str(rec.get("agent_id") or "").strip())
        role_override = _override_role(override)
        tags_override = _override_tags(override)
        role_effective = (
            role_override if role_override is not None else rec.get("role")
        )
        tags_effective = tags_override if tags_override is not None else tags
        return {
            "online": bool(age_s is not None and age_s <= stale_after_s),
            "updated_at": updated_at,
            "role": rec.get("role"),
            "tags": tags,
            "role_override": role_override,
            "tags_override": tags_override,
            "role_effective": role_effective,
            "tags_effective": tags_effective,
        }

    timeout_s = float(state.settings.a2a_http_timeout_s)
    peer_targets = list((state.peers or {}).keys())
    if "*" not in peer_targets:
        peer_targets.append("*")
    peers = await _select_peers(state, peer_targets if peer_targets else ["*"])

    results: Dict[str, Any] = {}

    if include_self:
        fn = a2a_service.actions().get("health_status")
        if fn is None:
            results[str(state.settings.agent_id)] = {
                "ok": False,
                "error": "health_status action unavailable",
            }
        else:
            try:
                res = await fn(state, {"include_last_applied": True})
                results[str(state.settings.agent_id)] = {"ok": True, "result": res}
            except Exception as e:
                results[str(state.settings.agent_id)] = {"ok": False, "error": str(e)}

    if peers:
        payload = {"action": "health_status", "params": {"include_last_applied": True}}
        sem = asyncio.Semaphore(min(8, len(peers)))

        async def _call(peer: Any) -> None:
            async with sem:
                out = await _peer_post_json(
                    state=state,
                    peer=peer,
                    path="/v1/a2a/invoke",
                    payload=payload,
                    timeout_s=timeout_s,
                )
                key = str(getattr(peer, "name", "") or getattr(peer, "base_url", ""))
                results[key] = out

        await asyncio.gather(*[_call(p) for p in peers])

    agents: list[dict[str, Any]] = []
    summary = {
        "total": 0,
        "online": 0,
        "wled_ok": 0,
        "fpp_ok": 0,
        "ledfx_ok": 0,
    }

    for key, value in results.items():
        entry: Dict[str, Any] = {"ok": False}
        base_url = None
        if isinstance(value, dict) and value.get("ok") is True:
            res = value.get("result") if isinstance(value.get("result"), dict) else {}
            agent_id = str(res.get("agent_id") or "").strip() or None
            name = res.get("name")
            base_url = (
                res.get("base_url")
                if isinstance(res.get("base_url"), str)
                else None
            )
            meta = _merge_meta(agent_id=agent_id, base_url=base_url)
            wled = res.get("wled") if isinstance(res.get("wled"), dict) else {}
            fpp = res.get("fpp") if isinstance(res.get("fpp"), dict) else {}
            ledfx = res.get("ledfx") if isinstance(res.get("ledfx"), dict) else {}
            entry = {
                "ok": True,
                "agent_id": agent_id,
                "name": name,
                "base_url": base_url,
                "role": meta.get("role_effective"),
                "tags": meta.get("tags_effective"),
                "role_override": meta.get("role_override"),
                "tags_override": meta.get("tags_override"),
                "online": meta.get("online"),
                "updated_at": meta.get("updated_at"),
                "wled": wled,
                "fpp": fpp,
                "ledfx": ledfx,
                "last_applied": res.get("last_applied"),
            }
        elif isinstance(value, dict):
            entry["error"] = value.get("error")
        else:
            entry["error"] = "Unknown error"
        if not entry.get("agent_id"):
            entry["agent_id"] = str(key)
        if not entry.get("base_url"):
            entry["base_url"] = str(key)
        agents.append(entry)
        summary["total"] += 1
        if entry.get("online"):
            summary["online"] += 1
        wled_ok = bool(entry.get("wled", {}).get("ok"))
        fpp_ok = bool(entry.get("fpp", {}).get("ok"))
        ledfx_ok = bool(entry.get("ledfx", {}).get("health") or entry.get("ledfx", {}).get("ok"))
        if wled_ok:
            summary["wled_ok"] += 1
        if fpp_ok:
            summary["fpp_ok"] += 1
        if ledfx_ok:
            summary["ledfx_ok"] += 1

    payload = {
        "ok": True,
        "cached": False,
        "generated_at": now,
        "ttl_s": ttl_s,
        "summary": summary,
        "agents": agents,
    }

    async with _FLEET_HEALTH_LOCK:
        _FLEET_HEALTH_CACHE["ts"] = now
        _FLEET_HEALTH_CACHE["payload"] = payload
        _FLEET_HEALTH_CACHE["include_self"] = bool(include_self)

    await log_event(
        state,
        action="fleet.health",
        ok=True,
        payload={"agents": summary.get("total", 0)},
        request=request,
    )
    return payload


async def fleet_history(
    request: Request,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
    limit: int = 200,
    agent_id: str | None = None,
    since: float | None = None,
    until: float | None = None,
    role: str | None = None,
    tag: str | None = None,
    offset: int = 0,
) -> Dict[str, Any]:
    db = getattr(state, "db", None)
    if db is None:
        await log_event(
            state,
            action="fleet.history",
            ok=False,
            error="Database not initialized",
            request=request,
        )
        raise HTTPException(status_code=503, detail="Database not initialized")
    try:
        lim = max(1, int(limit))
        off = max(0, int(offset))
        rows = await _fetch_fleet_history_rows(
            db=db,
            limit=lim,
            offset=off,
            agent_id=agent_id,
            role=role,
            tag=tag,
            since=since,
            until=until,
        )
        count = len(rows)
        next_offset = off + count if count >= lim else None
        await log_event(
            state,
            action="fleet.history",
            ok=True,
            payload={
                "limit": lim,
                "offset": off,
                "agent_id": agent_id,
                "role": role,
                "tag": tag,
                "count": count,
            },
            request=request,
        )
        return {
            "ok": True,
            "history": rows,
            "count": count,
            "limit": lim,
            "offset": off,
            "next_offset": next_offset,
        }
    except Exception as e:
        await log_event(
            state,
            action="fleet.history",
            ok=False,
            error=str(e),
            payload={
                "limit": int(limit),
                "offset": int(offset),
                "agent_id": agent_id,
                "role": role,
                "tag": tag,
            },
            request=request,
        )
        raise HTTPException(status_code=500, detail=str(e))


async def _fetch_fleet_history_rows(
    *,
    db: Any,
    limit: int,
    offset: int,
    agent_id: str | None,
    role: str | None,
    tag: str | None,
    since: float | None,
    until: float | None,
) -> list[dict[str, Any]]:
    lim = max(1, int(limit))
    off = max(0, int(offset))
    tag_val = str(tag).strip() if tag else ""
    try:
        return await db.list_agent_heartbeat_history(
            limit=lim,
            agent_id=agent_id,
            role=role,
            tag=tag_val or None,
            since=since,
            until=until,
            offset=off,
        )
    except Exception:
        if not tag_val:
            raise

    filtered: list[dict[str, Any]] = []
    scanned = 0
    scan_offset = 0
    page_size = min(1000, max(100, lim))
    max_scan = max(5000, lim * 20)
    while len(filtered) < (off + lim) and scanned < max_scan:
        batch = await db.list_agent_heartbeat_history(
            limit=page_size,
            agent_id=agent_id,
            role=role,
            since=since,
            until=until,
            offset=scan_offset,
        )
        if not batch:
            break
        scanned += len(batch)
        scan_offset += len(batch)
        for row in batch:
            payload = row.get("payload") if isinstance(row, dict) else None
            tags = payload.get("tags") if isinstance(payload, dict) else None
            if isinstance(tags, list) and tag_val in [str(t) for t in tags]:
                filtered.append(row)

    return filtered[off : off + lim]


async def fleet_history_export(
    request: Request,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
    limit: int = 2000,
    agent_id: str | None = None,
    since: float | None = None,
    until: float | None = None,
    role: str | None = None,
    tag: str | None = None,
    offset: int = 0,
    format: str = "csv",
) -> Response:
    db = getattr(state, "db", None)
    if db is None:
        await log_event(
            state,
            action="fleet.history.export",
            ok=False,
            error="Database not initialized",
            request=request,
        )
        raise HTTPException(status_code=503, detail="Database not initialized")
    try:
        lim = max(1, min(20000, int(limit)))
        off = max(0, int(offset))
        rows = await _fetch_fleet_history_rows(
            db=db,
            limit=lim,
            offset=off,
            agent_id=agent_id,
            role=role,
            tag=tag,
            since=since,
            until=until,
        )
        fmt = str(format or "csv").strip().lower()
        if fmt == "json":
            payload = json.dumps({"ok": True, "history": rows}, indent=2)
            await log_event(
                state,
                action="fleet.history.export",
                ok=True,
                payload={"limit": lim, "offset": off, "format": fmt},
                request=request,
            )
            return Response(content=payload, media_type="application/json")

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(
            [
                "id",
                "agent_id",
                "created_at",
                "updated_at",
                "name",
                "role",
                "controller_kind",
                "version",
                "base_url",
                "payload",
            ]
        )
        for row in rows:
            payload = row.get("payload") or {}
            writer.writerow(
                [
                    row.get("id"),
                    row.get("agent_id"),
                    row.get("created_at"),
                    row.get("updated_at"),
                    row.get("name"),
                    row.get("role"),
                    row.get("controller_kind"),
                    row.get("version"),
                    row.get("base_url"),
                    json.dumps(payload, separators=(",", ":")),
                ]
            )
        await log_event(
            state,
            action="fleet.history.export",
            ok=True,
            payload={"limit": lim, "offset": off, "format": fmt},
            request=request,
        )
        return PlainTextResponse(
            output.getvalue(),
            headers={"Content-Disposition": "attachment; filename=fleet_history.csv"},
        )
    except Exception as e:
        await log_event(
            state,
            action="fleet.history.export",
            ok=False,
            error=str(e),
            request=request,
        )
        raise HTTPException(status_code=500, detail=str(e))


async def fleet_history_retention_status(
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    db = getattr(state, "db", None)
    if db is None:
        raise HTTPException(status_code=503, detail="Database not initialized")
    try:
        stats = await db.agent_history_stats()
        now = time.time()
        max_rows = int(getattr(state.settings, "agent_history_max_rows", 0) or 0)
        max_days = int(getattr(state.settings, "agent_history_max_days", 0) or 0)
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
                    getattr(state.settings, "agent_history_maintenance_interval_s", 0)
                    or 0
                ),
            },
            "drift": {
                "excess_rows": int(excess_rows),
                "excess_age_s": float(excess_age_s),
                "oldest_age_s": float(oldest_age_s) if oldest_age_s is not None else None,
                "drift": drift,
            },
            "last_retention": getattr(state, "agent_history_retention_last", None),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def fleet_history_retention_cleanup(
    max_rows: int | None = None,
    max_days: int | None = None,
    _: Dict[str, Any] = Depends(require_admin),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    db = getattr(state, "db", None)
    if db is None:
        raise HTTPException(status_code=503, detail="Database not initialized")
    try:
        cfg_max_rows = int(getattr(state.settings, "agent_history_max_rows", 0) or 0)
        cfg_max_days = int(getattr(state.settings, "agent_history_max_days", 0) or 0)
        use_max_rows = max_rows if max_rows is not None else (cfg_max_rows or None)
        use_max_days = max_days if max_days is not None else (cfg_max_days or None)
        result = await db.enforce_agent_heartbeat_history_retention(
            max_rows=use_max_rows,
            max_days=use_max_days,
        )
        state.agent_history_retention_last = {
            "at": time.time(),
            "result": result,
        }
        return {"ok": True, "result": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def fleet_resolve(
    req: FleetResolveRequest,
    request: Request,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    """
    Resolve fleet target selectors into concrete peers (no fanout).
    """
    try:
        peers = state.peers or {}
        raw_targets = [str(t).strip() for t in (req.targets or []) if str(t).strip()]
        targets = raw_targets or None

        db = getattr(state, "db", None)
        db_discovery = bool(getattr(state.settings, "fleet_db_discovery_enabled", True))
        discovery_enabled = bool(db is not None and db_discovery)

        now = time.time()
        stale = max(
            1.0,
            (
                float(req.stale_after_s)
                if req.stale_after_s is not None
                else float(getattr(state.settings, "fleet_stale_after_s", 30.0))
            ),
        )

        # Snapshot heartbeats (once) so we can compute online/age and resolve by agent_id/base_url.
        rows: list[dict[str, Any]] = []
        if db is not None:
            try:
                rows = await db.list_agent_heartbeats(limit=int(req.limit))
            except Exception:
                rows = []
        overrides_map = await _load_agent_overrides(state) if db is not None else {}

        hb_by_id: Dict[str, Dict[str, Any]] = {}
        hb_by_base_url: Dict[str, Dict[str, Any]] = {}
        for r in rows:
            if not isinstance(r, dict):
                continue
            aid = str(r.get("agent_id") or "").strip()
            if aid:
                hb_by_id[aid] = dict(r)
            payload = r.get("payload") or {}
            if isinstance(payload, dict):
                bu = str(payload.get("base_url") or "").strip().rstrip("/")
                if bu and (bu.startswith("http://") or bu.startswith("https://")):
                    hb_by_base_url[bu] = dict(r)

        def _hb_info(rec: Dict[str, Any] | None) -> Dict[str, Any]:
            if not rec:
                return {
                    "agent_id": None,
                    "name": None,
                    "role": None,
                    "controller_kind": None,
                    "version": None,
                    "base_url": None,
                    "tags": None,
                    "role_override": None,
                    "tags_override": None,
                    "role_effective": None,
                    "tags_effective": None,
                    "updated_at": None,
                    "started_at": None,
                    "age_s": None,
                    "online": None,
                }
            updated_at = float(rec.get("updated_at") or 0.0) or None
            age_s = max(0.0, now - updated_at) if updated_at else None
            payload = rec.get("payload") or {}
            if not isinstance(payload, dict):
                payload = {}
            base_url = str(payload.get("base_url") or "").strip().rstrip("/") or None
            raw_tags = payload.get("tags")
            tags = (
                [str(x) for x in raw_tags if x is not None]
                if isinstance(raw_tags, list)
                else None
            )
            aid = str(rec.get("agent_id") or "").strip()
            override = overrides_map.get(aid) if aid else None
            role_override = _override_role(override)
            tags_override = _override_tags(override)
            role_effective = (
                role_override if role_override is not None else rec.get("role")
            )
            tags_effective = tags_override if tags_override is not None else tags
            return {
                "agent_id": str(rec.get("agent_id") or "") or None,
                "name": rec.get("name") if rec.get("name") is not None else None,
                "role": rec.get("role") if rec.get("role") is not None else None,
                "controller_kind": (
                    rec.get("controller_kind")
                    if rec.get("controller_kind") is not None
                    else None
                ),
                "version": rec.get("version") if rec.get("version") is not None else None,
                "base_url": base_url,
                "tags": tags,
                "role_override": role_override,
                "tags_override": tags_override,
                "role_effective": role_effective,
                "tags_effective": tags_effective,
                "updated_at": updated_at,
                "started_at": float(rec.get("started_at") or 0.0) or None,
                "age_s": age_s,
                "online": bool(age_s is not None and age_s <= stale),
            }

        selected_by_url: Dict[str, Dict[str, Any]] = {}

        def _add(
            *,
            name: str,
            base_url: str,
            source: str,
            matched_by: str | None,
            agent_id: str | None = None,
        ) -> None:
            u = str(base_url or "").strip().rstrip("/")
            if not u or not (u.startswith("http://") or u.startswith("https://")):
                return
            rec = selected_by_url.get(u)
            if rec is None:
                selected_by_url[u] = {
                    "name": str(name or "").strip() or u,
                    "base_url": u,
                    "source": str(source),
                    "matched_by": {str(matched_by)} if matched_by else set(),
                    "agent_id_hint": str(agent_id) if agent_id else None,
                }
                return
            # Merge.
            if matched_by:
                try:
                    rec["matched_by"].add(str(matched_by))  # type: ignore[union-attr]
                except Exception:
                    rec["matched_by"] = {str(matched_by)}
            if source == "configured":
                rec["source"] = "configured"
                if name:
                    rec["name"] = str(name)
            if agent_id and not rec.get("agent_id_hint"):
                rec["agent_id_hint"] = str(agent_id)

        unresolved: list[dict[str, str]] = []

        if not targets:
            # Default behavior: all configured peers (no DB discovery).
            for p in peers.values():
                _add(
                    name=str(getattr(p, "name", "") or ""),
                    base_url=str(getattr(p, "base_url", "") or ""),
                    source="configured",
                    matched_by=None,
                )
        else:
            explicit: list[str] = []
            roles: set[str] = set()
            tags: set[str] = set()
            include_all = False

            for t in targets:
                if t in ("*", "all"):
                    include_all = True
                    continue
                if t.startswith("role:"):
                    v = t[len("role:") :].strip()
                    if v:
                        roles.add(v)
                    continue
                if t.startswith("tag:"):
                    v = t[len("tag:") :].strip()
                    if v:
                        tags.add(v)
                    continue
                explicit.append(t)

            if include_all:
                if discovery_enabled:
                    for rec in rows:
                        if not isinstance(rec, dict):
                            continue
                        aid = str(rec.get("agent_id") or "").strip()
                        if not aid or aid == str(state.settings.agent_id):
                            continue
                        info = _hb_info(rec)
                        if info.get("online") is not True:
                            continue
                        bu = str(info.get("base_url") or "").strip()
                        if not bu:
                            continue
                        _add(
                            name=str(aid),
                            base_url=bu,
                            source="discovered",
                            matched_by="*",
                            agent_id=str(aid),
                        )
                else:
                    for p in peers.values():
                        _add(
                            name=str(getattr(p, "name", "") or ""),
                            base_url=str(getattr(p, "base_url", "") or ""),
                            source="configured",
                            matched_by="*",
                        )

            if roles:
                if not discovery_enabled:
                    for r in sorted(roles):
                        unresolved.append(
                            {"target": f"role:{r}", "reason": "db_discovery_disabled"}
                        )
                else:
                    for r in sorted(roles):
                        matched = 0
                        for rec in rows:
                            if not isinstance(rec, dict):
                                continue
                            if str(rec.get("role") or "").strip() != r:
                                continue
                            info = _hb_info(rec)
                            if info.get("online") is not True:
                                continue
                            bu = str(info.get("base_url") or "").strip()
                            if not bu:
                                continue
                            aid = str(rec.get("agent_id") or "").strip()
                            if not aid or aid == str(state.settings.agent_id):
                                continue
                            matched += 1
                            _add(
                                name=str(aid),
                                base_url=bu,
                                source="discovered",
                                matched_by=f"role:{r}",
                                agent_id=str(aid),
                            )
                        if matched == 0:
                            unresolved.append(
                                {"target": f"role:{r}", "reason": "no_matches"}
                            )

            if tags:
                if not discovery_enabled:
                    for t in sorted(tags):
                        unresolved.append(
                            {"target": f"tag:{t}", "reason": "db_discovery_disabled"}
                        )
                else:
                    for t in sorted(tags):
                        matched = 0
                        for rec in rows:
                            if not isinstance(rec, dict):
                                continue
                            info = _hb_info(rec)
                            if info.get("online") is not True:
                                continue
                            payload = rec.get("payload") or {}
                            if not isinstance(payload, dict):
                                payload = {}
                            raw = payload.get("tags")
                            rec_tags = (
                                {
                                    str(x).strip()
                                    for x in raw
                                    if x is not None and str(x).strip()
                                }
                                if isinstance(raw, list)
                                else set()
                            )
                            if t not in rec_tags:
                                continue
                            bu = str(info.get("base_url") or "").strip()
                            if not bu:
                                continue
                            aid = str(rec.get("agent_id") or "").strip()
                            if not aid or aid == str(state.settings.agent_id):
                                continue
                            matched += 1
                            _add(
                                name=str(aid),
                                base_url=bu,
                                source="discovered",
                                matched_by=f"tag:{t}",
                                agent_id=str(aid),
                            )
                        if matched == 0:
                            unresolved.append(
                                {"target": f"tag:{t}", "reason": "no_matches"}
                            )

            # Explicit names or agent IDs.
            for t in explicit:
                if t in peers:
                    p = peers[t]
                    _add(
                        name=str(getattr(p, "name", t) or t),
                        base_url=str(getattr(p, "base_url", "") or ""),
                        source="configured",
                        matched_by=t,
                    )
                    continue

                if not discovery_enabled:
                    unresolved.append({"target": t, "reason": "not_configured"})
                    continue

                hb = hb_by_id.get(t)
                if not hb:
                    unresolved.append({"target": t, "reason": "not_found"})
                    continue

                info = _hb_info(hb)
                bu = str(info.get("base_url") or "").strip()
                if not bu:
                    unresolved.append({"target": t, "reason": "missing_base_url"})
                    continue
                _add(
                    name=str(t),
                    base_url=bu,
                    source="discovered",
                    matched_by=t,
                    agent_id=str(t),
                )

        resolved: list[dict[str, Any]] = []
        for u, rec in sorted(
            selected_by_url.items(), key=lambda kv: str(kv[1].get("name") or kv[0])
        ):
            agent_id_hint = rec.get("agent_id_hint")
            hb = None
            if agent_id_hint:
                hb = hb_by_id.get(str(agent_id_hint))
            if hb is None:
                hb = hb_by_base_url.get(str(u))
            info = _hb_info(hb)
            resolved.append(
                {
                    "name": rec.get("name"),
                    "base_url": str(u),
                    "source": rec.get("source"),
                    "matched_by": sorted(list(rec.get("matched_by") or [])),
                    "agent": info,
                }
            )

        payload = {
            "ok": True,
            "targets": targets or [],
            "discovery_enabled": discovery_enabled,
            "stale_after_s": stale,
            "resolved": resolved,
            "unresolved": unresolved,
        }
        await log_event(
            state,
            action="fleet.resolve",
            ok=True,
            payload={
                "targets": targets,
                "resolved": len(resolved),
                "unresolved": len(unresolved),
            },
            request=request,
        )
        return payload
    except Exception as e:
        await log_event(
            state,
            action="fleet.resolve",
            ok=False,
            error=str(e),
            payload={"targets": req.targets},
            request=request,
        )
        raise


async def fleet_invoke(
    req: FleetInvokeRequest,
    request: Request,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    try:
        action = (req.action or "").strip()
        timeout_s = (
            float(req.timeout_s)
            if req.timeout_s is not None
            else float(state.settings.a2a_http_timeout_s)
        )
        peers = await _select_peers(state, req.targets)

        results: Dict[str, Any] = {}

        if req.include_self:
            fn = a2a_service.actions().get(action)
            if fn is None:
                results["self"] = {"ok": False, "error": f"Unknown action '{action}'"}
            else:
                try:
                    res = await fn(state, dict(req.params or {}))
                    results["self"] = {"ok": True, "result": res}
                except Exception as e:
                    results["self"] = {"ok": False, "error": str(e)}

        if peers:
            payload = {"action": action, "params": dict(req.params or {})}
            sem = asyncio.Semaphore(min(8, len(peers)))

            async def _call(peer: Any) -> None:
                async with sem:
                    out = await _peer_post_json(
                        state=state,
                        peer=peer,
                        path="/v1/a2a/invoke",
                        payload=payload,
                        timeout_s=timeout_s,
                    )
                    results[str(getattr(peer, "name", ""))] = out

            await asyncio.gather(*[_call(p) for p in peers])

        await log_event(
            state,
            action="fleet.invoke",
            ok=True,
            payload={
                "action": action,
                "targets": req.targets,
                "include_self": bool(req.include_self),
            },
            request=request,
        )
        return {"ok": True, "action": action, "results": results}
    except Exception as e:
        await log_event(
            state,
            action="fleet.invoke",
            ok=False,
            error=str(e),
            payload={
                "action": req.action,
                "targets": req.targets,
                "include_self": bool(req.include_self),
            },
            request=request,
        )
        raise


async def fleet_apply_random_look(
    req: FleetApplyRandomLookRequest,
    request: Request | None = None,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    try:
        # Pick on this agent, then broadcast the same look_spec to peers so devices match.
        looks = getattr(state, "looks", None)
        if looks is None:
            raise RuntimeError("Look service not initialized")
        pack, row = await looks.choose_random(
            theme=req.theme,
            pack_file=req.pack_file,
            seed=req.seed,
        )
        bri = (
            min(state.settings.wled_max_bri, req.brightness)
            if req.brightness is not None
            else None
        )

        results: Dict[str, Any] = {
            "pack_file": pack,
            "picked": {
                "id": row.get("id"),
                "name": row.get("name"),
                "theme": row.get("theme"),
            },
        }

        peers = await _select_peers(state, req.targets)
        timeout_s = float(state.settings.a2a_http_timeout_s)

        if req.include_self:
            try:
                if state.wled_cooldown is not None:
                    await state.wled_cooldown.wait()
                res = await looks.apply_look(row, brightness_override=bri)
                results["self"] = {"ok": True, "result": res}
            except Exception as e:
                results["self"] = {"ok": False, "error": str(e)}

        if state.db is not None:
            try:
                await state.db.set_last_applied(
                    kind="look",
                    name=str(row.get("name") or "") or None,
                    file=str(pack) if pack else None,
                    payload={
                        "look": dict(row or {}),
                        "pack_file": str(pack) if pack else None,
                        "brightness_override": bri,
                        "scope": "fleet",
                    },
                )
                try:
                    from services.events_service import emit_event

                    await emit_event(
                        state,
                        event_type="meta",
                        data={
                            "event": "last_applied",
                            "kind": "look",
                            "name": str(row.get("name") or "") or None,
                            "file": str(pack) if pack else None,
                            "scope": "fleet",
                        },
                    )
                except Exception:
                    pass
            except Exception:
                pass

        if peers:
            # Cache capabilities in parallel.
            caps = await asyncio.gather(
                *[
                    _peer_supported_actions(state=state, peer=p, timeout_s=timeout_s)
                    for p in peers
                ]
            )
            eligible: List[Any] = []
            for peer, actions in zip(peers, caps):
                if "apply_look_spec" in actions:
                    eligible.append(peer)
                else:
                    results[str(getattr(peer, "name", ""))] = {
                        "ok": False,
                        "skipped": True,
                        "reason": "Peer does not support apply_look_spec",
                    }

            if eligible:
                payload = {
                    "action": "apply_look_spec",
                    "params": {"look_spec": row, "brightness_override": bri},
                }
                sem = asyncio.Semaphore(min(8, len(eligible)))

                async def _call(peer: Any) -> None:
                    async with sem:
                        out = await _peer_post_json(
                            state=state,
                            peer=peer,
                            path="/v1/a2a/invoke",
                            payload=payload,
                            timeout_s=timeout_s,
                        )
                        results[str(getattr(peer, "name", ""))] = out

                await asyncio.gather(*[_call(p) for p in eligible])

        await log_event(
            state,
            action="fleet.apply_random_look",
            ok=True,
            resource=str(pack) if pack else None,
            payload={
                "theme": req.theme,
                "targets": req.targets,
                "include_self": bool(req.include_self),
            },
            request=request,
        )
        return {"ok": True, "result": results}
    except Exception as e:
        await log_event(
            state,
            action="fleet.apply_random_look",
            ok=False,
            error=str(e),
            payload={
                "theme": req.theme,
                "targets": req.targets,
                "include_self": bool(req.include_self),
            },
            request=request,
        )
        raise


async def fleet_crossfade(
    req: FleetCrossfadeRequest,
    request: Request | None = None,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    try:
        if req.look is None and req.state is None:
            raise HTTPException(status_code=400, detail="Provide look or state")

        params: Dict[str, Any] = {}
        if req.look is not None:
            if not isinstance(req.look, dict):
                raise HTTPException(status_code=400, detail="look must be an object")
            params["look"] = dict(req.look)
        if req.state is not None:
            if not isinstance(req.state, dict):
                raise HTTPException(status_code=400, detail="state must be an object")
            params["state"] = dict(req.state)

        if req.brightness is not None:
            params["brightness"] = min(
                state.settings.wled_max_bri, max(1, int(req.brightness))
            )
        if req.transition_ms is not None:
            params["transition_ms"] = int(req.transition_ms)

        timeout_s = (
            float(req.timeout_s)
            if req.timeout_s is not None
            else float(state.settings.a2a_http_timeout_s)
        )

        results: Dict[str, Any] = {}

        if req.include_self:
            try:
                res = await a2a_service.actions()["crossfade"](state, params)
                results["self"] = {"ok": True, "result": res}
            except Exception as e:
                results["self"] = {"ok": False, "error": str(e)}

        if state.db is not None and req.look is not None:
            try:
                await state.db.set_last_applied(
                    kind="look",
                    name=str(req.look.get("name") or "") or None,
                    file=str(req.look.get("file") or "") or None,
                    payload={"look": dict(req.look), "scope": "fleet"},
                )
                try:
                    from services.events_service import emit_event

                    await emit_event(
                        state,
                        event_type="meta",
                        data={
                            "event": "last_applied",
                            "kind": "look",
                            "name": str(req.look.get("name") or "") or None,
                            "file": str(req.look.get("file") or "") or None,
                            "scope": "fleet",
                        },
                    )
                except Exception:
                    pass
            except Exception:
                pass

        peers = await _select_peers(state, req.targets)
        if peers:
            caps = await asyncio.gather(
                *[
                    _peer_supported_actions(state=state, peer=p, timeout_s=timeout_s)
                    for p in peers
                ]
            )
            eligible: List[Any] = []
            for peer, actions in zip(peers, caps):
                if "crossfade" in actions:
                    eligible.append(peer)
                else:
                    results[str(getattr(peer, "name", ""))] = {
                        "ok": False,
                        "skipped": True,
                        "reason": "Peer does not support crossfade",
                    }

            if eligible:
                payload = {"action": "crossfade", "params": params}
                sem = asyncio.Semaphore(min(8, len(eligible)))

                async def _call(peer: Any) -> None:
                    async with sem:
                        out = await _peer_post_json(
                            state=state,
                            peer=peer,
                            path="/v1/a2a/invoke",
                            payload=payload,
                            timeout_s=timeout_s,
                        )
                        results[str(getattr(peer, "name", ""))] = out

                await asyncio.gather(*[_call(p) for p in eligible])

        await log_event(
            state,
            action="fleet.crossfade",
            ok=True,
            payload={
                "kind": "look" if req.look is not None else "state",
                "targets": req.targets,
                "include_self": bool(req.include_self),
            },
            request=request,
        )
        return {"ok": True, "results": results}
    except Exception as e:
        await log_event(
            state,
            action="fleet.crossfade",
            ok=False,
            error=str(e),
            payload={
                "targets": req.targets,
                "include_self": bool(req.include_self),
            },
            request=request,
        )
        raise


async def fleet_stop_all(
    req: FleetStopAllRequest,
    request: Request,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    try:
        timeout_s = (
            float(req.timeout_s)
            if req.timeout_s is not None
            else float(state.settings.a2a_http_timeout_s)
        )
        peers = await _select_peers(state, req.targets)
        results: Dict[str, Any] = {}

        if req.include_self:
            try:
                res = await a2a_service.actions()["stop_all"](state, {})
                results["self"] = {"ok": True, "result": res}
            except Exception as e:
                results["self"] = {"ok": False, "error": str(e)}

        if peers:
            payload = {"action": "stop_all", "params": {}}
            sem = asyncio.Semaphore(min(8, len(peers)))

            async def _call(peer: Any) -> None:
                async with sem:
                    out = await _peer_post_json(
                        state=state,
                        peer=peer,
                        path="/v1/a2a/invoke",
                        payload=payload,
                        timeout_s=timeout_s,
                    )
                    results[str(getattr(peer, "name", ""))] = out

            await asyncio.gather(*[_call(p) for p in peers])

        try:
            await persist_runtime_state(state, "fleet_stop_all", {"targets": req.targets})
        except Exception:
            pass
        await log_event(
            state,
            action="fleet.stop_all",
            ok=True,
            payload={"targets": req.targets, "include_self": bool(req.include_self)},
            request=request,
        )
        return {"ok": True, "action": "stop_all", "results": results}
    except Exception as e:
        await log_event(
            state,
            action="fleet.stop_all",
            ok=False,
            error=str(e),
            payload={"targets": req.targets, "include_self": bool(req.include_self)},
            request=request,
        )
        raise
