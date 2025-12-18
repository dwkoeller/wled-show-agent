from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

from openai import OpenAI


@dataclass
class ToolResult:
    name: str
    output: Any


class SimpleDirectorAgent:
    """
    Thin wrapper around OpenAI tool-calling. This is optional; the system works without it.

    You provide a dict of tool_name -> callable(kwargs)->Any.
    """

    def __init__(
        self,
        *,
        api_key: str,
        model: str,
        tools: Dict[str, Callable[[Dict[str, Any]], Any]],
        system_prompt: Optional[str] = None,
    ) -> None:
        self.client = OpenAI(api_key=api_key)
        self.model = model
        self.tools = tools
        self.system_prompt = system_prompt

    def _tool_schemas(self) -> List[Dict[str, Any]]:
        # Keep schemas small and permissive to avoid constant breakage.
        return [
            {
                "type": "function",
                "name": "apply_random_look",
                "description": "Apply a random look from the latest generated pack, optionally filtered by theme.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "theme": {"type": "string"},
                        "brightness": {"type": "integer", "minimum": 1, "maximum": 255},
                    },
                    "required": [],
                },
            },
            {
                "type": "function",
                "name": "start_ddp_pattern",
                "description": "Start a realtime DDP pattern for a duration.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "pattern": {"type": "string"},
                        "duration_s": {
                            "type": "number",
                            "minimum": 0.1,
                            "maximum": 600,
                        },
                        "brightness": {"type": "integer", "minimum": 1, "maximum": 255},
                        "fps": {"type": "number", "minimum": 1, "maximum": 60},
                        "direction": {
                            "type": "string",
                            "description": "Rotation direction from street: cw or ccw",
                        },
                        "start_pos": {
                            "type": "string",
                            "description": "Start position from street: front/right/back/left",
                        },
                        "params": {"type": "object"},
                    },
                    "required": ["pattern"],
                },
            },
            {
                "type": "function",
                "name": "stop_ddp",
                "description": "Stop any running realtime DDP stream.",
                "parameters": {"type": "object", "properties": {}, "required": []},
            },
            {
                "type": "function",
                "name": "stop_all",
                "description": "Stop sequences and any running realtime DDP stream.",
                "parameters": {"type": "object", "properties": {}, "required": []},
            },
            {
                "type": "function",
                "name": "generate_looks_pack",
                "description": "Generate a new looks pack file (lots of patterns) into the data directory.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "total_looks": {
                            "type": "integer",
                            "minimum": 50,
                            "maximum": 5000,
                        },
                        "themes": {"type": "array", "items": {"type": "string"}},
                        "brightness": {"type": "integer", "minimum": 1, "maximum": 255},
                        "seed": {"type": "integer"},
                    },
                    "required": [],
                },
            },
            {
                "type": "function",
                "name": "fleet_start_sequence",
                "description": "Start a generated sequence across the fleet (and optionally self).",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "file": {
                            "type": "string",
                            "description": "Sequence filename from /v1/sequences/list",
                        },
                        "loop": {"type": "boolean"},
                        "targets": {"type": "array", "items": {"type": "string"}},
                        "include_self": {"type": "boolean"},
                        "timeout_s": {
                            "type": "number",
                            "minimum": 0.1,
                            "maximum": 30.0,
                        },
                    },
                    "required": ["file"],
                },
            },
            {
                "type": "function",
                "name": "fleet_stop_sequence",
                "description": "Stop any running fleet sequence.",
                "parameters": {"type": "object", "properties": {}, "required": []},
            },
            {
                "type": "function",
                "name": "fpp_start_playlist",
                "description": "Start an FPP playlist by name (requires FPP_BASE_URL).",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "repeat": {"type": "boolean"},
                    },
                    "required": ["name"],
                },
            },
            {
                "type": "function",
                "name": "fpp_stop_playlist",
                "description": "Stop the currently running FPP playlist (requires FPP_BASE_URL).",
                "parameters": {"type": "object", "properties": {}, "required": []},
            },
            {
                "type": "function",
                "name": "fpp_trigger_event",
                "description": "Trigger an FPP event by numeric id (requires FPP_BASE_URL).",
                "parameters": {
                    "type": "object",
                    "properties": {"event_id": {"type": "integer", "minimum": 1}},
                    "required": ["event_id"],
                },
            },
        ]

    def run(self, user_text: str) -> Dict[str, Any]:
        if not user_text.strip():
            return {"ok": False, "error": "Empty command"}

        # One round of tool calling, then final.
        system_prompt = self.system_prompt or (
            "You are a show director for a WLED Christmas mega tree. The tree is split into 4 segments (quadrants). "
            "Use tools to apply looks or start DDP patterns. "
            "Be concise. Prefer apply_random_look for general requests, and start_ddp_pattern for realtime animation requests. "
            "For quadrant motion, you can use DDP patterns like 'quad_chase', 'opposite_pulse', 'quad_twinkle', 'quad_comets', and 'quad_spiral'."
        )

        resp = self.client.responses.create(
            model=self.model,
            input=[
                {
                    "role": "system",
                    "content": system_prompt,
                },
                {"role": "user", "content": user_text},
            ],
            tools=self._tool_schemas(),
            tool_choice="auto",
        )

        tool_results: List[ToolResult] = []

        # Iterate tool calls in the response
        for item in getattr(resp, "output", []) or []:
            if getattr(item, "type", None) != "tool_call":
                continue
            name = getattr(item, "name", None)
            args = getattr(item, "arguments", None) or "{}"
            try:
                kwargs = json.loads(args) if isinstance(args, str) else (args or {})
            except Exception:
                kwargs = {}
            if name in self.tools:
                out = self.tools[name](kwargs)
            else:
                out = {"ok": False, "error": f"Unknown tool '{name}'"}
            tool_results.append(ToolResult(name=name, output=out))

        if not tool_results:
            # No tool call; return the model text output
            text = ""
            for item in getattr(resp, "output", []) or []:
                if getattr(item, "type", None) == "message":
                    # messages contain content parts
                    parts = getattr(item, "content", []) or []
                    for p in parts:
                        if getattr(p, "type", None) == "output_text":
                            text += str(getattr(p, "text", "")) + "\n"
            return {"ok": True, "response": text.strip()}

        # Provide tool results back for a final short message
        resp2 = self.client.responses.create(
            model=self.model,
            input=[
                {"role": "user", "content": user_text},
                {"role": "assistant", "content": "Tools executed."},
                {
                    "role": "tool",
                    "content": json.dumps([tr.__dict__ for tr in tool_results]),
                },
            ],
        )

        text2 = ""
        for item in getattr(resp2, "output", []) or []:
            if getattr(item, "type", None) == "message":
                parts = getattr(item, "content", []) or []
                for p in parts:
                    if getattr(p, "type", None) == "output_text":
                        text2 += str(getattr(p, "text", "")) + "\n"

        return {
            "ok": True,
            "tool_results": [
                {"tool": tr.name, "output": tr.output} for tr in tool_results
            ],
            "response": text2.strip() or "Done.",
        }
