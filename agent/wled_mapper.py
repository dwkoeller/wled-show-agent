from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from wled_client import WLEDClient


def _norm(s: str) -> str:
    return s.strip().lower()


@dataclass
class WLEDNameMaps:
    effect_name_to_id: Dict[str, int]
    palette_name_to_id: Dict[str, int]


class WLEDMapper:
    """
    Fetches /json/eff and /json/pal and builds case-insensitive name -> ID maps.
    Filters out reserved effects 'RSVD' and '-' as recommended.
    """
    def __init__(self, wled: WLEDClient) -> None:
        self.wled = wled
        self._maps: Optional[WLEDNameMaps] = None

    def refresh(self) -> WLEDNameMaps:
        effects = self.wled.get_effects(refresh=True)
        palettes = self.wled.get_palettes(refresh=True)

        eff_map: Dict[str, int] = {}
        for idx, name in enumerate(effects):
            n = _norm(str(name))
            if n in ("rsvd", "-"):
                continue
            if n and n not in eff_map:
                eff_map[n] = idx

        pal_map: Dict[str, int] = {}
        for idx, name in enumerate(palettes):
            n = _norm(str(name))
            if n and n not in pal_map:
                pal_map[n] = idx

        self._maps = WLEDNameMaps(effect_name_to_id=eff_map, palette_name_to_id=pal_map)
        return self._maps

    def maps(self) -> WLEDNameMaps:
        if self._maps is None:
            return self.refresh()
        return self._maps

    def effect_id(self, name: str, *, default: int = 0) -> int:
        m = self.maps().effect_name_to_id
        return int(m.get(_norm(name), default))

    def palette_id(self, name: str, *, default: int = 0) -> int:
        m = self.maps().palette_name_to_id
        return int(m.get(_norm(name), default))
