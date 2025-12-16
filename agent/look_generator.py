from __future__ import annotations

import hashlib
import itertools
import random
import math
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from wled_mapper import WLEDMapper


RGB = Tuple[int, int, int]


def _clamp8(x: int) -> int:
    return max(0, min(255, int(x)))


def _hex3(rgb: RGB) -> str:
    return "%02X%02X%02X" % rgb


def _stable_id(*parts: str) -> str:
    h = hashlib.sha1("|".join(parts).encode("utf-8")).hexdigest()[:12]
    return h


def _norm(s: str) -> str:
    return s.strip().lower()


@dataclass(frozen=True)
class Look:
    id: str
    name: str
    theme: str
    tags: List[str]
    spec: Dict[str, Any]  # effect/palette by name + segment params etc


class LookLibraryGenerator:
    """
    Generates a LOT of looks as WLED-state "specs" that reference effect/palette by *name*.
    Those names are mapped to numeric IDs at apply/import time.
    """
    def __init__(self, *, mapper: WLEDMapper, seed: int = 1337) -> None:
        self.mapper = mapper
        self.rng = random.Random(seed)

        # Snapshot current names (for selection)
        self.effects = self._filtered_effects()
        self.palettes = self._palettes()

        # Theme color banks
        self.colors: Dict[str, List[List[RGB]]] = self._build_color_banks()

    def _filtered_effects(self) -> List[str]:
        # Use mapper maps() to filter reserved; but we want the names too.
        eff = self.mapper.wled.get_effects(refresh=True)
        out: List[str] = []
        for e in eff:
            n = str(e).strip()
            if not n:
                continue
            if n.strip().upper() == "RSVD" or n.strip() == "-":
                continue
            out.append(n)
        return out

    def _palettes(self) -> List[str]:
        pal = self.mapper.wled.get_palettes(refresh=True)
        return [str(p).strip() for p in pal if str(p).strip()]

    def _build_color_banks(self) -> Dict[str, List[List[RGB]]]:
        # A "color set" is up to 3 RGB colors, mapped to seg.col[0..2]
        classic = [
            [(255, 0, 0), (0, 255, 0), (0, 0, 0)],
            [(255, 20, 0), (0, 140, 0), (255, 255, 255)],
            [(220, 0, 0), (0, 180, 0), (255, 255, 255)],
            [(255, 0, 0), (0, 200, 60), (20, 20, 20)],
        ]
        candy = [
            [(255, 0, 0), (255, 255, 255), (0, 0, 0)],
            [(220, 0, 0), (255, 255, 255), (80, 80, 80)],
            [(255, 30, 30), (250, 250, 250), (0, 0, 0)],
        ]
        icy = [
            [(0, 180, 255), (255, 255, 255), (0, 0, 32)],
            [(0, 120, 255), (200, 240, 255), (0, 0, 16)],
            [(60, 200, 255), (255, 255, 255), (0, 0, 40)],
        ]
        warm = [
            [(255, 140, 20), (255, 200, 80), (20, 10, 0)],
            [(255, 170, 60), (255, 120, 0), (32, 12, 0)],
            [(255, 200, 120), (255, 120, 40), (20, 10, 0)],
        ]
        rainbow = [
            [(255, 0, 0), (0, 255, 0), (0, 0, 255)],
            [(255, 0, 180), (0, 255, 200), (255, 255, 0)],
            [(255, 90, 0), (0, 200, 255), (120, 0, 255)],
        ]
        halloween = [
            [(255, 80, 0), (160, 0, 255), (0, 255, 0)],
            [(255, 50, 0), (80, 0, 160), (0, 160, 0)],
            [(255, 120, 0), (0, 0, 0), (120, 0, 255)],
        ]
        synth = [
            [(255, 0, 150), (0, 255, 255), (10, 0, 30)],
            [(255, 0, 255), (0, 180, 255), (0, 0, 0)],
        ]
        return {
            "classic": classic,
            "candy_cane": candy,
            "icy": icy,
            "warm_white": warm,
            "rainbow": rainbow,
            "halloween": halloween,
            "synthwave": synth,
        }

    def _pick_effects(self, theme: str) -> List[str]:
        # Heuristic selection by keywords
        e = self.effects
        theme_l = _norm(theme)
        kw: List[str] = []
        avoid: List[str] = []

        if theme_l in ("classic", "candy_cane", "icy", "warm_white"):
            kw += ["Twinkle", "Sparkle", "Dissolve", "Wipe", "Chase", "Scan", "Comet", "Fireworks", "Rain", "Merry", "Glitter"]
        if theme_l == "halloween":
            kw += ["Halloween", "Lightning", "Fireworks", "Dissolve", "Chase", "Twinkle", "Ripple"]
        if theme_l == "rainbow":
            kw += ["Rainbow", "Color", "Palette", "Waves", "Noise", "BPM", "Juggle", "Pride", "Colorwaves"]
        if theme_l == "synthwave":
            kw += ["Color", "Waves", "Noise", "BPM", "Juggle", "Ripple", "Scanner", "Sweep", "Oscillate"]

        # drop audio reactive if present in name? can't reliably; keep anyway; WLED will ignore on non-AR builds.

        pool: List[str] = []
        for name in e:
            if any(k.lower() in name.lower() for k in kw):
                pool.append(name)
        if len(pool) < 20:
            # fallback: full list
            pool = list(e)
        self.rng.shuffle(pool)
        return pool

    def _pick_palettes(self, theme: str) -> List[str]:
        p = self.palettes
        theme_l = _norm(theme)

        # preference by keyword
        desired: List[str] = []
        if theme_l == "rainbow":
            desired += ["Rainbow", "Party", "Rainbow Bands", "Tiamat", "Sunset"]
        if theme_l == "icy":
            desired += ["Ice", "Icefire", "Ocean", "Cloud", "Breeze"]
        if theme_l == "warm_white":
            desired += ["Fire", "Lava", "Autumn", "Sunset", "Vintage"]
        if theme_l == "classic":
            desired += ["Red", "Green", "Forest", "Jul", "Party"]
        if theme_l == "candy_cane":
            desired += ["Red", "Party", "Vintage", "Default"]
        if theme_l == "halloween":
            desired += ["Halloween", "Lava", "Fire", "Magenta", "Party"]
        if theme_l == "synthwave":
            desired += ["Magenta", "Tiamat", "Party", "Sunset", "April Night"]

        candidates: List[str] = []
        for pal in p:
            if any(d.lower() in pal.lower() for d in desired):
                candidates.append(pal)
        if not candidates:
            candidates = ["Default"] if "Default" in p else p[:]
        self.rng.shuffle(candidates)
        return candidates

    def generate(
        self,
        *,
        total: int,
        themes: Sequence[str],
        brightness: int = 180,
        include_multi_segment: bool = False,
        segment_ids: Optional[Sequence[int]] = None,
    ) -> List[Look]:
        """
        Generate up to `total` looks, distributed across `themes`.

        This implementation is intentionally streaming (no huge Cartesian products in memory).
        """
        brightness = _clamp8(brightness)
        seg_ids = list(segment_ids) if segment_ids else [0]
        looks: List[Look] = []
        seen: set[str] = set()

        if not themes:
            themes = ["classic"]

        # parameter banks (lots of variety)
        speeds = [40, 60, 80, 100, 120, 150, 180, 210, 235]
        intensities = [30, 50, 70, 90, 110, 127, 150, 180, 210, 235]
        transitions = [0, 1, 2, 3, 5, 8, 12]

        seg_var = [
            {"rev": False, "grp": 1, "spc": 0},
            {"rev": True, "grp": 1, "spc": 0},
            {"rev": False, "grp": 2, "spc": 0},
            {"rev": False, "grp": 3, "spc": 0},
            {"rev": False, "grp": 4, "spc": 0},
            {"rev": False, "grp": 1, "spc": 1},
            {"rev": True, "grp": 2, "spc": 1},
        ]

        # Determine per-theme targets (roughly even)
        per_theme = max(1, total // max(1, len(themes)))
        extra = total - per_theme * len(themes)

        for ti, theme in enumerate(themes):
            target = per_theme + (1 if ti < extra else 0)
            colorsets = self.colors.get(theme, self.colors.get("classic", [])) or self.colors.get("classic", [])
            effs = self._pick_effects(theme)
            pals = self._pick_palettes(theme)

            # Slightly bias towards holiday-specific effects if present in list
            # (no guarantee those exist on all builds)
            def pick_effect() -> str:
                if theme.lower() == "halloween":
                    for e in effs:
                        if "halloween" in e.lower():
                            return e
                if theme.lower() in ("classic", "candy_cane"):
                    for e in effs:
                        if "merry" in e.lower():
                            return e
                return self.rng.choice(effs)

            attempts = 0
            while len([l for l in looks if l.theme == theme]) < target and attempts < target * 50:
                attempts += 1
                eff = pick_effect()
                pal = self.rng.choice(pals) if pals else "Default"
                colors = self.rng.choice(colorsets) if colorsets else [(255, 255, 255)]
                sx = self.rng.choice(speeds)
                ix = self.rng.choice(intensities)
                sv = self.rng.choice(seg_var)
                tr = self.rng.choice(transitions)

                spec = {
                    "type": "wled_look",
                    "theme": theme,
                    "effect": eff,
                    "palette": pal,
                    "bri": brightness,
                    "transition": tr,
                    "seg": {
                        "id": 0,
                        "fx": eff,   # NAME form (mapped later)
                        "pal": pal,  # NAME form (mapped later)
                        "sx": sx,
                        "ix": ix,
                        "col": [[c[0], c[1], c[2]] for c in colors[:3]],
                        **sv,
                    },
                    "tags": [],
                }

                key = f"{theme}|{eff}|{pal}|{sx}|{ix}|{tr}|{sv.get('rev')}|{sv.get('grp')}|{sv.get('spc')}|{spec['seg']['col']}"
                if key in seen:
                    continue
                seen.add(key)

                name = f"{theme}:{eff} [{pal}] sx{sx} ix{ix}"
                look_id = _stable_id(theme, eff, pal, str(sx), str(ix), str(tr), str(sv.get("rev")), str(sv.get("grp")), str(sv.get("spc")))
                looks.append(Look(id=look_id, name=name, theme=theme, tags=[], spec=spec))

            # Optional: a small handful of multi-segment looks, for extra spice
            # If the WLED instance has multiple segments (e.g., 4), generate patterns that address ALL segments.
            if include_multi_segment and len(seg_ids) >= 2 and len(looks) < total:
                ms_target = max(0, min(80, target // 6))
                attempts = 0
                while ms_target > 0 and attempts < 500:
                    attempts += 1
                    # Build a few "segment styles" for variety.
                    styles = ["alt_colors", "quad_colors", "alt_rev", "split_fx", "split_pal"]
                    # Extra styles when you have 4+ segments (e.g. 4 quadrants).
                    if len(seg_ids) >= 4:
                        styles += [
                            "quad_offset",
                            "quad_bri_gradient",
                            "opposite_pairs",
                            "spotlight",
                            # Street-oriented spotlights (assumes ordered segments start at street-right and
                            # go around the tree; good for quarter-tree output layouts).
                            "street_spotlight_front",
                            "street_spotlight_right",
                        ]
                    style = self.rng.choice(styles)  # segment-aware


                    pal = self.rng.choice(pals) if pals else "Default"
                    eff = self.rng.choice(effs)

                    seg_list: List[Dict[str, Any]] = []
                    # pick multiple color sets
                    csets: List[List[RGB]] = []
                    for _ in range(max(2, min(4, len(seg_ids)))):
                        csets.append(self.rng.choice(colorsets))

                    effs2 = self._pick_effects(theme)
                    pals2 = self._pick_palettes(theme)

                    # Best-effort: infer physical segment order + lengths from WLED.
                    ordered_seg_ids = list(seg_ids)
                    seg_len_by_id: Dict[int, int] = {}
                    try:
                        seg_state = self.mapper.wled.get_segments(refresh=False)
                        seg_map = {}
                        for s in seg_state:
                            if not isinstance(s, dict):
                                continue
                            sid = int(s.get("id", -1))
                            if sid < 0 or sid not in set(int(x) for x in seg_ids):
                                continue
                            start = int(s.get("start", 0))
                            stop = int(s.get("stop", 0))
                            ln = int(s.get("len", 0))
                            if ln <= 0 and stop > start:
                                ln = stop - start
                            seg_len_by_id[sid] = max(0, ln)
                            seg_map[sid] = (start, sid)
                        # Sort by start then id if starts exist
                        if seg_map:
                            ordered_seg_ids = [sid for sid, _ in sorted(seg_map.items(), key=lambda kv: (kv[1][0], kv[1][1]))]
                    except Exception:
                        ordered_seg_ids = list(seg_ids)


                    for si, seg_id in enumerate(ordered_seg_ids):
                        # per-segment effect/palette variations
                        fx_name = eff
                        pal_name = pal
                        if style == "split_fx":
                            fx_name = self.rng.choice(effs2)
                        if style == "split_pal":
                            pal_name = self.rng.choice(pals2) if pals2 else pal

                        colors = csets[si % len(csets)]
                        sx = int(self.rng.choice(speeds))
                        ix = int(self.rng.choice(intensities))
                        rev = bool((si % 2) == 1) if style in ("alt_rev",) else bool(self.rng.choice([False, False, True]))

                        seg_obj: Dict[str, Any] = {
                            "id": int(seg_id),
                            "fx": fx_name,
                            "pal": pal_name,
                            "sx": sx,
                            "ix": ix,
                            "col": [[x[0], x[1], x[2]] for x in colors[:3]],
                            "rev": rev,
                        }

                        # Segment-aware extras (WLED supports per-segment on/off, brightness, and offset).
                        # These are optional and won't change segment bounds.
                        if style == "quad_offset":
                            seg_len = int(seg_len_by_id.get(int(seg_id), 0)) or max(1, int(self.rng.choice([196, 392, 784])))
                            seg_obj["of"] = int((si / max(1, len(ordered_seg_ids))) * seg_len)
                        elif style == "quad_bri_gradient":
                            # Brightness "around" the tree
                            base = int(brightness)
                            # A simple 4-phase wave
                            phase = (si % max(1, len(ordered_seg_ids))) / max(1.0, float(len(ordered_seg_ids)))
                            seg_obj["bri"] = max(0, min(255, int(base * (0.35 + 0.65 * (0.5 + 0.5 * math.sin(2.0 * math.pi * phase))))))
                        elif style == "opposite_pairs":
                            # (0,2) share colors; (1,3) share colors
                            pair = (si % 2)
                            seg_obj["col"] = [[x[0], x[1], x[2]] for x in csets[pair][:3]]
                            seg_obj["rev"] = bool(pair == 1)
                        elif style == "spotlight":
                            # Pick a "spot" segment; dim others
                            spot = int(self.rng.randrange(0, max(1, len(ordered_seg_ids))))
                            seg_obj["bri"] = int(brightness) if si == spot else max(0, int(brightness * 0.15))
                            seg_obj["on"] = True
                        elif style == "street_spotlight_front":
                            # Bias spotlight to the street-facing quadrant.
                            # If ordered_seg_ids starts at street-right and goes counterclockwise,
                            # then the street-facing quadrant is the last one.
                            spot = max(0, len(ordered_seg_ids) - 1)
                            seg_obj["bri"] = int(brightness) if si == spot else max(0, int(brightness * 0.15))
                            seg_obj["on"] = True
                        elif style == "street_spotlight_right":
                            # Bias spotlight to the street-right quadrant (first in ordered_seg_ids).
                            spot = 0
                            seg_obj["bri"] = int(brightness) if si == spot else max(0, int(brightness * 0.15))
                            seg_obj["on"] = True

                        seg_list.append(seg_obj)

                    spec = {
                        "type": "wled_look",
                        "theme": theme,
                        "effect": eff,
                        "palette": pal,
                        "bri": brightness,
                        "transition": 0,
                        "seg": seg_list,
                        "tags": ["multi_segment", style],
                    }

                    key = f"ms|{style}|{theme}|{pal}|" + "|".join(
                        [f"{s['id']}:{s['fx']}:{s['pal']}:{s['sx']}:{s['ix']}:{s.get('rev')}:{s['col']}" for s in seg_list]
                    )
                    if key in seen:
                        continue
                    seen.add(key)
                    name = f"{theme}:MS {eff} [{pal}]"
                    look_id = _stable_id("ms", theme, style, eff, pal, str(len(seg_list)))
                    looks.append(Look(id=look_id, name=name, theme=theme, tags=["multi_segment", style], spec=spec))
                    ms_target -= 1

        # If still short (e.g., too few effects), top up from any theme
        attempts = 0
        while len(looks) < total and attempts < total * 50:
            attempts += 1
            theme = self.rng.choice(list(themes))
            colorsets = self.colors.get(theme, self.colors.get("classic", [])) or self.colors.get("classic", [])
            effs = self._pick_effects(theme)
            pals = self._pick_palettes(theme)
            eff = self.rng.choice(effs)
            pal = self.rng.choice(pals) if pals else "Default"
            colors = self.rng.choice(colorsets)
            sx = self.rng.choice(speeds)
            ix = self.rng.choice(intensities)
            sv = self.rng.choice(seg_var)
            tr = self.rng.choice(transitions)
            spec = {
                "type": "wled_look",
                "theme": theme,
                "effect": eff,
                "palette": pal,
                "bri": brightness,
                "transition": tr,
                "seg": {"id": 0, "fx": eff, "pal": pal, "sx": sx, "ix": ix, "col": [[c[0], c[1], c[2]] for c in colors[:3]], **sv},
                "tags": [],
            }
            key = f"{theme}|{eff}|{pal}|{sx}|{ix}|{tr}|{sv.get('rev')}|{sv.get('grp')}|{sv.get('spc')}|{spec['seg']['col']}"
            if key in seen:
                continue
            seen.add(key)
            name = f"{theme}:{eff} [{pal}] sx{sx} ix{ix}"
            look_id = _stable_id(theme, eff, pal, str(sx), str(ix), str(tr), str(sv.get("rev")), str(sv.get("grp")), str(sv.get("spc")))
            looks.append(Look(id=look_id, name=name, theme=theme, tags=[], spec=spec))

        return looks


def look_to_wled_state(
    look_spec: Dict[str, Any],
    mapper: WLEDMapper,
    *,
    brightness_override: Optional[int] = None,
    segment_ids: Optional[Sequence[int]] = None,
    replicate_to_all_segments: bool = False,
) -> Dict[str, Any]:
    """
    Convert a look spec (effect/palette as names) into a WLED /json/state payload (fx/pal numeric).
    """
    if look_spec.get("type") != "wled_look":
        raise ValueError("Only type=wled_look is supported")

    bri = int(look_spec.get("bri", 180))
    if brightness_override is not None:
        bri = _clamp8(int(brightness_override))

    transition = int(look_spec.get("transition", 0))

    seg = look_spec.get("seg")
    seg_ids = [int(x) for x in (segment_ids or []) if str(x).strip() != ""]

    def _strip_bounds(s: Dict[str, Any]) -> Dict[str, Any]:
        s.pop("start", None)
        s.pop("stop", None)
        return s

    def _sanitize_segment(s: Dict[str, Any]) -> Dict[str, Any]:
        # Default segment on=true to avoid a previously-disabled segment staying dark.
        if "on" not in s:
            s["on"] = True
        # Clamp per-segment brightness if present.
        if "bri" in s:
            try:
                s["bri"] = _clamp8(int(s["bri"]))
            except Exception:
                s.pop("bri", None)
        # Coerce offset to int if present.
        if "of" in s:
            try:
                s["of"] = int(s["of"])
            except Exception:
                s.pop("of", None)
        return s

    if isinstance(seg, dict):
        eff_name = str(seg.get("fx", "Solid"))
        pal_name = str(seg.get("pal", "Default"))
        fx_id = mapper.effect_id(eff_name, default=0)
        pal_id = mapper.palette_id(pal_name, default=0)
        seg_out = dict(seg)
        seg_out["fx"] = fx_id
        seg_out["pal"] = pal_id
        # ensure id present
        seg_out["id"] = int(seg_out.get("id", 0))
        # If the caller provided explicit segment IDs, ensure we only target known segments.
        if seg_ids and seg_out["id"] not in seg_ids:
            seg_out["id"] = int(seg_ids[0])
        seg_out = _sanitize_segment(_strip_bounds(seg_out))

        if replicate_to_all_segments and seg_ids:
            seg_list: List[Dict[str, Any]] = []
            for sid in seg_ids:
                s = dict(seg_out)
                s["id"] = int(sid)
                seg_list.append(s)
            return {"on": True, "bri": bri, "transition": transition, "seg": seg_list}

        return {"on": True, "bri": bri, "transition": transition, "seg": [seg_out]}

    if isinstance(seg, list):
        seg_out_list: List[Dict[str, Any]] = []
        for s in seg:
            if not isinstance(s, dict):
                continue
            s2 = dict(s)
            fx_id = mapper.effect_id(str(s2.get("fx", "Solid")), default=0)
            pal_id = mapper.palette_id(str(s2.get("pal", "Default")), default=0)
            s2["fx"] = fx_id
            s2["pal"] = pal_id
            s2["id"] = int(s2.get("id", 0))
            s2 = _sanitize_segment(_strip_bounds(s2))
            seg_out_list.append(s2)

        # If the caller provided explicit segment IDs, drop any segment updates that don't apply.
        # If nothing matches, fall back to mapping the first segment spec onto the first known segment.
        if seg_ids:
            allowed = set(seg_ids)
            all_specs = list(seg_out_list)
            seg_out_list = [s for s in seg_out_list if int(s.get("id", -1)) in allowed]
            if not seg_out_list and all_specs:
                template = dict(all_specs[0])
                template["id"] = int(seg_ids[0])
                seg_out_list = [template]

        if seg_ids and not seg_out_list:
            seg_out_list = [
                {
                    "id": int(seg_ids[0]),
                    "fx": mapper.effect_id("Solid", default=0),
                    "pal": mapper.palette_id("Default", default=0),
                    "on": True,
                }
            ]

        # If requested, ensure all segments receive something (copy the first seg as template).
        if replicate_to_all_segments and seg_ids:
            present = {int(s.get("id", 0)) for s in seg_out_list}
            if seg_out_list:
                template = dict(seg_out_list[0])
                for sid in seg_ids:
                    if int(sid) in present:
                        continue
                    s = dict(template)
                    s["id"] = int(sid)
                    seg_out_list.append(s)
            seg_out_list.sort(key=lambda x: int(x.get("id", 0)))

        return {"on": True, "bri": bri, "transition": transition, "seg": seg_out_list}

    raise ValueError("Invalid look_spec.seg")
