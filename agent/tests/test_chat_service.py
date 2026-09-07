import asyncio
from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI, HTTPException, Request

from services.chat_service import ChatService, allowed_path, match_operation


@pytest.mark.parametrize("path", ["https://example.com/api/wled", "/api/auth/users", "/api/chat/login", "/api/files/../auth/users", "/api/files/%2e%2e/auth", "/api/wled?url=elsewhere", "/other"])
def test_show_tool_rejects_non_show_paths(path):
    assert not allowed_path(path)


def test_tool_route_matching_includes_parameters_and_method():
    schema = {"paths": {"/api/jobs/{job_id}": {"get": {"summary": "Job"}}}}
    assert match_operation(schema, "GET", "/api/jobs/abc")
    assert match_operation(schema, "DELETE", "/api/jobs/abc") is None
    assert match_operation(schema, "GET", "/api/jobs/abc/extra") is None


@pytest.mark.asyncio
async def test_tools_preserve_user_credentials_and_backend_errors():
    app = FastAPI()

    @app.post("/api/test/control")
    async def control(request: Request):
        assert request.headers.get("authorization") == "Bearer user-token"
        assert request.headers.get("x-csrf-token") == "csrf"
        raise HTTPException(403, "Viewer cannot control devices")

    service = ChatService(app, None)
    service.contexts["thread"] = {"headers": {"authorization": "Bearer user-token", "x-csrf-token": "csrf"}}
    result = await service._tool({"threadId": "thread", "tool": "show_request", "arguments": {"method": "POST", "path": "/api/test/control", "body": {}}})
    assert result["ok"] is False
    assert result["status"] == 403
    assert result["body"]["detail"] == "Viewer cannot control devices"


@pytest.mark.asyncio
async def test_tool_cannot_run_without_active_turn():
    service = ChatService(FastAPI(), None)
    with pytest.raises(HTTPException) as exc:
        await service._tool({"threadId": "unknown", "tool": "show_catalog"})
    assert exc.value.status_code == 403


@pytest.mark.asyncio
async def test_conversation_ownership_checked_before_runtime_access():
    class DB:
        async def kv_get_json(self, key):
            return {"threads": [{"id": "mine"}]} if key == "chat_index:alice" else None
    service = ChatService(FastAPI(), SimpleNamespace(db=DB()))
    await service.check_thread("alice", "mine")
    with pytest.raises(HTTPException) as exc:
        await service.history("bob", "mine")
    assert exc.value.status_code == 404


@pytest.mark.asyncio
async def test_stream_filters_reasoning_and_closes_on_completion():
    service = ChatService(FastAPI(), None)
    service.queues["thread"] = asyncio.Queue()
    service.contexts["thread"] = {}
    for message in [
        {"method": "item/started", "params": {"item": {"type": "reasoning", "text": "private"}}},
        {"method": "item/agentMessage/delta", "params": {"delta": "hello"}},
        {"method": "turn/completed", "params": {"turn": {"status": "completed"}}},
    ]:
        await service.queues["thread"].put(message)
    chunks = [chunk async for chunk in service.stream("thread")]
    assert len(chunks) == 2
    assert "private" not in "".join(chunks)
    assert "thread" not in service.contexts
