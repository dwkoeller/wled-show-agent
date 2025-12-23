from __future__ import annotations

import io
from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from starlette.datastructures import UploadFile

from services import command_service, voice_service


class _SettingsNoOpenAI:
    openai_api_key = None
    openai_stt_model = "whisper-1"


class _SettingsWithOpenAI:
    openai_api_key = "test-key"
    openai_stt_model = "whisper-1"


@pytest.mark.asyncio
async def test_voice_transcribe_requires_openai() -> None:
    state = SimpleNamespace(settings=_SettingsNoOpenAI())
    file = UploadFile(filename="speech.webm", file=io.BytesIO(b"voice"))
    with pytest.raises(HTTPException) as exc:
        await voice_service.voice_transcribe(
            file=file, language=None, prompt=None, state=state, _=None
        )
    assert exc.value.status_code == 503


@pytest.mark.asyncio
async def test_voice_command_uses_transcription(monkeypatch) -> None:
    state = SimpleNamespace(settings=_SettingsWithOpenAI())

    async def _fake_transcribe(**_: object) -> str:
        return "turn on the lights"

    async def _fake_command(**kwargs: object) -> dict:
        return {"ok": True, "text": kwargs.get("text")}

    monkeypatch.setattr(voice_service, "_transcribe_audio", _fake_transcribe)
    monkeypatch.setattr(command_service, "run_command_text", _fake_command)

    file = UploadFile(filename="speech.webm", file=io.BytesIO(b"voice"))
    res = await voice_service.voice_command(
        file=file, language=None, prompt=None, request=None, state=state, _=None
    )

    assert res["ok"] is True
    assert res["text"] == "turn on the lights"
    assert res["command"]["text"] == "turn on the lights"
