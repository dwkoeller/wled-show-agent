from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional


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

    def __init__(self, wled: Any | None = None) -> None:
        self.wled = wled
        self._maps: Optional[WLEDNameMaps] = None

    @staticmethod
    def _build_maps(*, effects: List[str], palettes: List[str]) -> WLEDNameMaps:
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

        return WLEDNameMaps(effect_name_to_id=eff_map, palette_name_to_id=pal_map)

    def seed(self, *, effects: List[str], palettes: List[str]) -> WLEDNameMaps:
        """
        Seed the mapper with effect/palette name lists without doing network I/O.

        Useful when running the app fully async (fetch names via AsyncWLEDClient, then seed).
        """
        self._maps = self._build_maps(effects=effects, palettes=palettes)
        return self._maps

    def refresh(self) -> WLEDNameMaps:
        if self.wled is None:
            raise RuntimeError("WLEDMapper has no WLED client; seed() first")
        effects = self.wled.get_effects(refresh=True)
        palettes = self.wled.get_palettes(refresh=True)
        self._maps = self._build_maps(
            effects=[str(x) for x in effects], palettes=[str(x) for x in palettes]
        )
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
