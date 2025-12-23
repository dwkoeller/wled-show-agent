from __future__ import annotations

import io
from typing import Any, Dict

from fastapi import Depends, File, Form, HTTPException, Request, UploadFile

from openai import AsyncOpenAI

from services import command_service
from services.auth_service import require_a2a_auth
from services.state import AppState, get_state


MAX_VOICE_BYTES = 12 * 1024 * 1024


async def _transcribe_audio(
    *,
    state: AppState,
    file: UploadFile,
    language: str | None,
    prompt: str | None,
) -> str:
    if not state.settings.openai_api_key:
        raise HTTPException(status_code=503, detail="OpenAI is not configured.")

    try:
        data = await file.read()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to read audio: {e}")

    if not data:
        raise HTTPException(status_code=400, detail="Empty audio payload")
    if len(data) > MAX_VOICE_BYTES:
        raise HTTPException(status_code=413, detail="Audio payload is too large")

    bio = io.BytesIO(data)
    bio.name = str(getattr(file, "filename", "") or "audio.webm")
    args: Dict[str, Any] = {
        "model": state.settings.openai_stt_model,
        "file": bio,
    }
    if language:
        args["language"] = str(language)
    if prompt:
        args["prompt"] = str(prompt)

    client = AsyncOpenAI(api_key=state.settings.openai_api_key)
    try:
        resp = await client.audio.transcriptions.create(**args)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        try:
            await client.close()
        except Exception:
            pass

    text = str(getattr(resp, "text", "") or "").strip()
    if not text:
        raise HTTPException(status_code=502, detail="Transcription returned empty text")
    return text


async def voice_transcribe(
    file: UploadFile = File(...),
    language: str | None = Form(default=None),
    prompt: str | None = Form(default=None),
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    text = await _transcribe_audio(
        state=state,
        file=file,
        language=language,
        prompt=prompt,
    )
    return {"ok": True, "text": text, "model": state.settings.openai_stt_model}


async def voice_command(
    file: UploadFile = File(...),
    language: str | None = Form(default=None),
    prompt: str | None = Form(default=None),
    request: Request | None = None,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    text = await _transcribe_audio(
        state=state,
        file=file,
        language=language,
        prompt=prompt,
    )
    result = await command_service.run_command_text(
        state=state,
        request=request,
        text=text,
    )
    return {
        "ok": True,
        "text": text,
        "model": state.settings.openai_stt_model,
        "command": result,
    }
