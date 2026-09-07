from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from services.auth_service import require_admin
from services.chat_service import get_chat, owner

router = APIRouter(prefix="/api/chat", tags=["Chat"])


class NewThread(BaseModel):
    model: str | None = None


class Message(BaseModel):
    text: str = Field(min_length=1, max_length=32000)
    model: str | None = None
    effort: str | None = None


@router.get("/account")
async def account(request: Request):
    owner(request)
    service = get_chat(request)
    await service.start()
    result = await service.rpc("account/read", {"refreshToken": False})
    account = result.get("account")
    return {"connected": bool(account), "account": account, "provider": "codex", "subscription": True}


@router.post("/login", dependencies=[Depends(require_admin)])
async def login(request: Request):
    service = get_chat(request)
    await service.start()
    return await service.rpc("account/login/start", {"type": "chatgptDeviceCode"})


@router.post("/logout", dependencies=[Depends(require_admin)])
async def logout(request: Request):
    service = get_chat(request)
    await service.start()
    return await service.rpc("account/logout")


@router.get("/models")
async def models(request: Request):
    owner(request)
    service = get_chat(request)
    await service.start()
    result = await service.rpc("model/list", {"limit": 100, "includeHidden": False})
    data = list(result.get("data", []))
    while result.get("nextCursor"):
        result = await service.rpc("model/list", {"limit": 100, "includeHidden": False, "cursor": result["nextCursor"]})
        data.extend(result.get("data", []))
    return {"data": data}


@router.get("/limits")
async def limits(request: Request):
    owner(request)
    service = get_chat(request)
    await service.start()
    return await service.rpc("account/rateLimits/read")


@router.get("/threads")
async def threads(request: Request):
    return await get_chat(request).list_threads(owner(request))


@router.post("/threads")
async def new_thread(body: NewThread, request: Request):
    return await get_chat(request).create_thread(owner(request), body.model)


@router.get("/threads/{thread_id}")
async def history(thread_id: str, request: Request):
    return await get_chat(request).history(owner(request), thread_id)


@router.delete("/threads/{thread_id}")
async def archive(thread_id: str, request: Request):
    return await get_chat(request).archive(owner(request), thread_id)


@router.post("/threads/{thread_id}/messages")
async def message(thread_id: str, body: Message, request: Request):
    service = get_chat(request)
    await service.begin(request, thread_id, body.text, body.model, body.effort)
    return StreamingResponse(service.stream(thread_id), media_type="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@router.post("/threads/{thread_id}/stop")
async def stop(thread_id: str, request: Request):
    return await get_chat(request).interrupt(owner(request), thread_id)
