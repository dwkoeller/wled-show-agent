from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from show_config import PixelOutputConfig, PropConfig, ShowConfig


_IP_RE = re.compile(r"^(?:\d{1,3}\.){3}\d{1,3}$")


def _looks_like_ipv4(val: str) -> bool:
    s = (val or "").strip()
    if not _IP_RE.match(s):
        return False
    try:
        parts = [int(x) for x in s.split(".")]
    except Exception:
        return False
    return len(parts) == 4 and all(0 <= p <= 255 for p in parts)


def _as_int(val: Any) -> Optional[int]:
    try:
        if val is None:
            return None
        s = str(val).strip()
        if s == "":
            return None
        return int(float(s))
    except Exception:
        return None


def _find_attr(attrs: Dict[str, str], keys: Tuple[str, ...]) -> Optional[str]:
    for k, v in attrs.items():
        kl = k.strip().lower()
        if kl in keys:
            out = str(v).strip()
            if out:
                return out
    return None


def _find_attr_contains(attrs: Dict[str, str], needle: str) -> Optional[str]:
    n = needle.lower()
    for k, v in attrs.items():
        if n in k.strip().lower():
            out = str(v).strip()
            if out:
                return out
    return None


def _guess_protocol(attrs: Dict[str, str], tag: str) -> str:
    proto = _find_attr(attrs, ("protocol", "type", "networktype")) or ""
    hint = f"{tag} {proto} " + " ".join([f"{k}={v}" for k, v in attrs.items()])
    s = hint.lower()
    if "artnet" in s or "art-net" in s:
        return "artnet"
    if "e131" in s or "sacn" in s or "sacn" in s:
        return "e131"
    return "e131"


@dataclass(frozen=True)
class XlightsController:
    name: str
    host: str
    protocol: str
    universe_start: int
    pixel_count: int
    raw: Dict[str, Any]


def parse_xlights_networks_xml(xml_text: str) -> List[XlightsController]:
    """
    Best-effort parser for xLights `xlights_networks.xml`.

    xLights config formats vary by version; this scans the XML for elements that contain an IP-like
    attribute and extracts a few common fields (name/protocol/start universe/pixel count).
    """
    root = ET.fromstring(xml_text)
    found: Dict[str, XlightsController] = {}

    for elem in root.iter():
        attrs = {str(k): str(v) for k, v in (elem.attrib or {}).items()}
        if not attrs:
            continue

        ip: Optional[str] = None
        # Prefer explicit ip-ish keys
        for k, v in attrs.items():
            kl = k.strip().lower()
            if "ip" in kl or kl in ("host", "address", "addr"):
                if _looks_like_ipv4(v):
                    ip = v.strip()
                    break
        # Fallback: any attribute value that looks like an IP
        if ip is None:
            for v in attrs.values():
                if _looks_like_ipv4(v):
                    ip = str(v).strip()
                    break
        if ip is None:
            continue

        name = (
            _find_attr(attrs, ("name", "controller", "description", "desc"))
            or _find_attr_contains(attrs, "name")
            or f"{elem.tag}_{ip}"
        )
        protocol = _guess_protocol(attrs, elem.tag)

        # Universe start: look for a start universe-ish key, else any universe-ish int.
        uni = _as_int(
            _find_attr_contains(attrs, "startuniverse")
            or _find_attr_contains(attrs, "universe")
        )
        if uni is None:
            uni = 1 if protocol == "e131" else 0

        pix = (
            _as_int(
                _find_attr_contains(attrs, "pixel")
                or _find_attr_contains(attrs, "node")
                or _find_attr_contains(attrs, "led")
            )
            or 0
        )

        key = f"{ip}|{name}"
        if key in found:
            continue
        found[key] = XlightsController(
            name=str(name).strip(),
            host=ip,
            protocol=protocol,
            universe_start=int(uni),
            pixel_count=int(pix),
            raw={"tag": elem.tag, "attrs": attrs},
        )

    return sorted(found.values(), key=lambda c: (c.host, c.name))


@dataclass(frozen=True)
class XlightsModel:
    name: str
    start_channel: int
    channel_count: int
    pixel_count: int
    raw: Dict[str, Any]


def _find_int_attr_includes(
    attrs: Dict[str, str], *, includes: Tuple[str, ...], excludes: Tuple[str, ...] = ()
) -> Optional[int]:
    for k, v in attrs.items():
        kl = k.strip().lower()
        if not any(inc in kl for inc in includes):
            continue
        if any(exc in kl for exc in excludes):
            continue
        val = _as_int(v)
        if val is not None:
            return val
    return None


def parse_xlights_models_xml(xml_text: str) -> List[XlightsModel]:
    """
    Best-effort parser for xLights model/layout XML (commonly `xlights_rgbeffects.xml`).

    xLights formats vary by version; this scans for elements that declare a start channel and a channel count.
    """
    root = ET.fromstring(xml_text)
    found: Dict[str, XlightsModel] = {}

    for elem in root.iter():
        attrs = {str(k): str(v) for k, v in (elem.attrib or {}).items()}
        if not attrs:
            continue

        start = _find_int_attr_includes(
            attrs, includes=("startchannel", "startchan"), excludes=("x", "y")
        )
        if start is None or start <= 0:
            continue

        count = _find_int_attr_includes(attrs, includes=("channelcount",), excludes=())
        if count is None:
            # fallback: keys like "Channels"
            count = _find_int_attr_includes(
                attrs, includes=("channels",), excludes=("start", "universe")
            )
        if count is None or count <= 0:
            continue

        name = (
            _find_attr(attrs, ("name", "modelname", "displayname"))
            or _find_attr_contains(attrs, "name")
            or f"{elem.tag}_{start}"
        )
        pixel_count = max(0, int(count) // 3)

        key = f"{start}|{name}"
        if key in found:
            continue
        found[key] = XlightsModel(
            name=str(name).strip(),
            start_channel=int(start),
            channel_count=int(count),
            pixel_count=int(pixel_count),
            raw={"tag": elem.tag, "attrs": attrs},
        )

    return sorted(found.values(), key=lambda m: (m.start_channel, m.name))


def import_xlights_networks_file(path: str) -> List[XlightsController]:
    p = Path(path)
    txt = p.read_text(encoding="utf-8", errors="ignore")
    return parse_xlights_networks_xml(txt)


def import_xlights_models_file(path: str) -> List[XlightsModel]:
    p = Path(path)
    txt = p.read_text(encoding="utf-8", errors="ignore")
    return parse_xlights_models_xml(txt)


def show_config_from_xlights_networks(
    *,
    networks: List[XlightsController],
    show_name: str = "xlights-import",
    subnet: Optional[str] = None,
    coordinator_base_url: Optional[str] = None,
    fpp_base_url: Optional[str] = None,
) -> ShowConfig:
    props: List[PropConfig] = []
    for c in networks:
        pid = (
            re.sub(r"[^a-zA-Z0-9_-]+", "_", c.name.strip())[:64]
            or f"ctrl_{c.host.replace('.', '_')}"
        )
        props.append(
            PropConfig(
                id=pid,
                kind="pixel",
                name=c.name,
                pixel=PixelOutputConfig(
                    protocol=c.protocol,
                    host=c.host,
                    pixel_count=max(0, int(c.pixel_count)),
                    universe_start=max(0, int(c.universe_start)),
                    channels_per_universe=510,
                ),
                tags=["xlights_import"],
            )
        )

    cfg = ShowConfig(
        version=1,
        name=show_name,
        subnet=subnet,
        coordinator={"base_url": coordinator_base_url} if coordinator_base_url else {},
        fpp={"base_url": fpp_base_url} if fpp_base_url else {},
        props=props,
        groups={"all": [p.id for p in props]} if props else {},
    )
    return cfg


def show_config_from_xlights_project(
    *,
    networks: List[XlightsController],
    models: List[XlightsModel],
    show_name: str = "xlights-project",
    subnet: Optional[str] = None,
    coordinator_base_url: Optional[str] = None,
    fpp_base_url: Optional[str] = None,
    include_controllers: bool = True,
    include_models: bool = True,
) -> ShowConfig:
    """
    Create a planning-oriented ShowConfig from xLights networks + model layout.

    - Controllers become `kind="pixel"` props (host/protocol/universe_start).
    - Models become `kind="model"` props with `channel_start`/`channel_count`/`pixel_count`.
    """
    props: List[PropConfig] = []

    # Controller props (optional)
    ctrl_ids: List[str] = []
    if include_controllers:
        for c in networks:
            pid = (
                re.sub(r"[^a-zA-Z0-9_-]+", "_", c.name.strip())[:64]
                or f"ctrl_{c.host.replace('.', '_')}"
            )
            # Best-effort absolute channel start (xLights convention: Universe 1 starts at channel 1)
            ch_start = None
            try:
                if int(c.universe_start) > 0:
                    ch_start = (int(c.universe_start) - 1) * 510 + 1
            except Exception:
                ch_start = None
            ch_count = None
            try:
                if int(c.pixel_count) > 0:
                    ch_count = int(c.pixel_count) * 3
            except Exception:
                ch_count = None

            props.append(
                PropConfig(
                    id=pid,
                    kind="pixel",
                    name=c.name,
                    channel_start=ch_start,
                    channel_count=ch_count,
                    pixel_count=max(0, int(c.pixel_count)),
                    pixel=PixelOutputConfig(
                        protocol=c.protocol,
                        host=c.host,
                        pixel_count=max(0, int(c.pixel_count)),
                        universe_start=max(0, int(c.universe_start)),
                        channels_per_universe=510,
                    ),
                    tags=["xlights_controller"],
                )
            )
            ctrl_ids.append(pid)

    # Model props (optional)
    model_ids: List[str] = []
    if include_models:
        for m in models:
            pid = (
                re.sub(r"[^a-zA-Z0-9_-]+", "_", m.name.strip())[:64]
                or f"model_{m.start_channel}"
            )
            # avoid collisions
            base = pid
            i = 2
            seen = {p.id for p in props}
            while pid in seen:
                pid = f"{base}_{i}"
                i += 1

            props.append(
                PropConfig(
                    id=pid,
                    kind="model",
                    name=m.name,
                    channel_start=int(m.start_channel),
                    channel_count=int(m.channel_count),
                    pixel_count=int(m.pixel_count),
                    tags=["xlights_model"],
                )
            )
            model_ids.append(pid)

    groups: Dict[str, List[str]] = {}
    if ctrl_ids:
        groups["controllers"] = list(ctrl_ids)
    if model_ids:
        groups["models"] = list(model_ids)
    if props:
        groups["all"] = [p.id for p in props]

    cfg = ShowConfig(
        version=1,
        name=show_name,
        subnet=subnet,
        coordinator={"base_url": coordinator_base_url} if coordinator_base_url else {},
        fpp={"base_url": fpp_base_url} if fpp_base_url else {},
        props=props,
        groups=groups,
    )
    return cfg
