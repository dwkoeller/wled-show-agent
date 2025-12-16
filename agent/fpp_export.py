from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class ExportedScript:
    filename: str
    rel_path: str
    bytes_written: int


def _b64(s: str) -> str:
    return base64.b64encode(s.encode("utf-8")).decode("ascii")


def render_http_post_script(
    *,
    coordinator_base_url: str,
    path: str,
    payload: Dict[str, Any],
    a2a_api_key: Optional[str] = None,
) -> str:
    coord = (coordinator_base_url or "").strip().rstrip("/")
    if not coord:
        raise ValueError("coordinator_base_url is required")
    if not path.startswith("/"):
        path = "/" + path

    payload_json = json.dumps(payload, ensure_ascii=False)
    payload_b64 = _b64(payload_json)
    key = (a2a_api_key or "").strip()

    # Use base64 to avoid quoting issues with embedded JSON.
    return (
        "#!/bin/sh\n"
        "set -eu\n"
        f"AGENT_URL='{coord}'\n"
        f"A2A_KEY='{key}'\n"
        "b64dec() {\n"
        "  if command -v base64 >/dev/null 2>&1; then\n"
        "    printf '%s' \"$1\" | base64 -d 2>/dev/null || printf '%s' \"$1\" | base64 -D\n"
        "    return 0\n"
        "  fi\n"
        "  echo 'base64 not found' 1>&2\n"
        "  exit 1\n"
        "}\n"
        "curl_agent() {\n"
        "  if [ -n \"$A2A_KEY\" ]; then\n"
        "    curl -sS -H \"X-A2A-Key: $A2A_KEY\" \"$@\"\n"
        "  else\n"
        "    curl -sS \"$@\"\n"
        "  fi\n"
        "}\n"
        f"PAYLOAD_B64='{payload_b64}'\n"
        "PAYLOAD_JSON=\"$(b64dec \"$PAYLOAD_B64\")\"\n"
        "curl_agent -H 'Content-Type: application/json' -X POST \"$AGENT_URL"
        + path
        + "\" -d \"$PAYLOAD_JSON\"\n"
    )


def write_script(*, out_dir: str, filename: str, script_text: str) -> ExportedScript:
    pdir = Path(out_dir)
    pdir.mkdir(parents=True, exist_ok=True)
    fname = filename if filename.endswith(".sh") else (filename + ".sh")
    p = (pdir / fname).resolve()
    p.write_text(script_text, encoding="utf-8")
    try:
        p.chmod(0o755)
    except Exception:
        pass
    return ExportedScript(filename=fname, rel_path=str(p), bytes_written=len(script_text.encode("utf-8")))
