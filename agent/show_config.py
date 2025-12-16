from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, model_validator

from pack_io import read_json, write_json


class CoordinatorConfig(BaseModel):
    base_url: Optional[str] = Field(
        default=None,
        description="Coordinator URL as reachable from external systems like FPP (e.g. http://172.16.200.10:8088).",
    )


class FPPConfig(BaseModel):
    base_url: Optional[str] = Field(default=None, description="FPP base URL (e.g. http://172.16.200.20).")


class PixelOutputConfig(BaseModel):
    protocol: str = Field("e131", description="e131 (sACN) or artnet")
    host: str
    pixel_count: int = Field(0, ge=0, description="Optional; set 0 if unknown.")
    universe_start: int = Field(1, ge=0)
    channels_per_universe: int = Field(510, ge=1, le=512)


class PropConfig(BaseModel):
    id: str
    kind: str = Field("wled", description="wled or pixel")

    # Optional metadata
    name: Optional[str] = None
    tags: List[str] = Field(default_factory=list)

    # Optional channel mapping (for FPP/xLights FSEQ export)
    # Absolute channel numbering is 1-based in xLights/FPP conventions.
    channel_start: Optional[int] = Field(default=None, ge=1, description="Absolute start channel (1-based).")
    channel_count: Optional[int] = Field(default=None, ge=1, description="Total channels for this prop (usually pixels*3).")
    pixel_count: Optional[int] = Field(default=None, ge=0, description="Pixel count for this prop (if known).")

    # WLED
    wled_url: Optional[str] = None
    segment_ids: Optional[List[int]] = None

    # Non-WLED pixel output (ESPixelStick / E1.31 / Art-Net)
    pixel: Optional[PixelOutputConfig] = None

    # Optional: agent service URL (for reference)
    agent_url: Optional[str] = None

    @model_validator(mode="after")
    def _validate_kind(self) -> "PropConfig":
        k = (self.kind or "").strip().lower()
        object.__setattr__(self, "kind", k)
        if k not in ("wled", "pixel"):
            raise ValueError("kind must be 'wled' or 'pixel'")
        if k == "wled":
            if self.pixel is not None:
                raise ValueError("pixel must be null for kind='wled'")
        if k == "pixel":
            if self.pixel is None:
                raise ValueError("pixel is required for kind='pixel'")
        return self


class ShowConfig(BaseModel):
    version: int = Field(1, ge=1)
    name: str = "wled-show"
    subnet: Optional[str] = Field(default=None, description="LAN subnet for controllers (e.g. 172.16.200.0/24).")
    channels_per_universe: int = Field(510, ge=1, le=512, description="xLights/FPP planning default (used for conversions).")

    coordinator: CoordinatorConfig = Field(default_factory=CoordinatorConfig)
    fpp: FPPConfig = Field(default_factory=FPPConfig)

    props: List[PropConfig] = Field(default_factory=list)
    groups: Dict[str, List[str]] = Field(
        default_factory=dict,
        description="Named groups mapping to prop ids (useful for fleet targeting).",
    )

    def as_dict(self) -> Dict[str, Any]:
        return self.model_dump(mode="json", exclude_none=True)


def _ensure_within(base_dir: str, rel_path: str) -> Path:
    base = Path(base_dir).resolve()
    p = (base / rel_path).resolve()
    if p == base:
        return p
    if base not in p.parents:
        raise ValueError("Path must be within DATA_DIR")
    return p


def load_show_config(*, data_dir: str, rel_path: str) -> ShowConfig:
    p = _ensure_within(data_dir, rel_path)
    cfg = read_json(str(p))
    return ShowConfig.model_validate(cfg)


def write_show_config(*, data_dir: str, rel_path: str, config: ShowConfig) -> str:
    p = _ensure_within(data_dir, rel_path)
    return write_json(str(p), config.as_dict())
