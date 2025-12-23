from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class A2APeer:
    name: str
    base_url: str


def parse_a2a_peers(entries: list[str]) -> dict[str, A2APeer]:
    peers: dict[str, A2APeer] = {}
    for raw in entries:
        item = str(raw).strip()
        if not item:
            continue
        if "=" in item:
            name, url = item.split("=", 1)
            name = name.strip()
            url = url.strip()
        else:
            name = ""
            url = item
        if not url:
            continue
        if not (url.startswith("http://") or url.startswith("https://")):
            url = "http://" + url
        url = url.rstrip("/")
        if not name:
            try:
                from urllib.parse import urlparse

                p = urlparse(url)
                host = p.hostname or "peer"
                name = host
                if p.port:
                    name = f"{host}:{p.port}"
            except Exception:
                name = url
        peers[name] = A2APeer(name=name, base_url=url)
    return peers
