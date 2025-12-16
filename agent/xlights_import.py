from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from show_config import PixelOutputConfig, PropConfig, ShowConfig


_IP_RE = re.compile(r"^(?:\\d{1,3}\\.){3}\\d{1,3}$")


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
        uni = _as_int(_find_attr_contains(attrs, "startuniverse") or _find_attr_contains(attrs, "universe"))
        if uni is None:
            uni = 1 if protocol == "e131" else 0

        pix = _as_int(_find_attr_contains(attrs, "pixel") or _find_attr_contains(attrs, "node") or _find_attr_contains(attrs, "led")) or 0

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


def import_xlights_networks_file(path: str) -> List[XlightsController]:
    p = Path(path)
    txt = p.read_text(encoding="utf-8", errors="ignore")
    return parse_xlights_networks_xml(txt)


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
        pid = re.sub(r"[^a-zA-Z0-9_-]+", "_", c.name.strip())[:64] or f"ctrl_{c.host.replace('.', '_')}"
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

