from __future__ import annotations

import math
import random
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple

from geometry import TreeGeometry
from segment_layout import SegmentLayout


def clamp8(x: float) -> int:
    if x <= 0:
        return 0
    if x >= 255:
        return 255
    return int(x)


def hsv_to_rgb(h: float, s: float, v: float) -> Tuple[int, int, int]:
    """h in [0,1), s,v in [0,1]."""
    h = h % 1.0
    s = max(0.0, min(1.0, s))
    v = max(0.0, min(1.0, v))
    i = int(h * 6.0)
    f = (h * 6.0) - i
    p = v * (1.0 - s)
    q = v * (1.0 - f * s)
    t = v * (1.0 - (1.0 - f) * s)
    i = i % 6
    if i == 0:
        r, g, b = v, t, p
    elif i == 1:
        r, g, b = q, v, p
    elif i == 2:
        r, g, b = p, v, t
    elif i == 3:
        r, g, b = p, q, v
    elif i == 4:
        r, g, b = t, p, v
    else:
        r, g, b = v, p, q
    return clamp8(r * 255.0), clamp8(g * 255.0), clamp8(b * 255.0)


def lerp(a: float, b: float, t: float) -> float:
    return a + (b - a) * t


def mix_rgb(
    a: Tuple[int, int, int], b: Tuple[int, int, int], t: float
) -> Tuple[int, int, int]:
    t = max(0.0, min(1.0, t))
    return (
        clamp8(lerp(a[0], b[0], t)),
        clamp8(lerp(a[1], b[1], t)),
        clamp8(lerp(a[2], b[2], t)),
    )


def scale_rgb(rgb: Tuple[int, int, int], bri: int) -> Tuple[int, int, int]:
    bri = max(0, min(255, int(bri)))
    if bri >= 255:
        return rgb
    s = bri / 255.0
    return (clamp8(rgb[0] * s), clamp8(rgb[1] * s), clamp8(rgb[2] * s))


@dataclass
class RenderContext:
    led_count: int
    geometry: TreeGeometry
    geometry_enabled: bool
    segment_layout: SegmentLayout | None = None


class Pattern:
    name: str = "pattern"

    def __init__(
        self, ctx: RenderContext, params: Optional[Dict[str, Any]] = None
    ) -> None:
        self.ctx = ctx
        self.params: Dict[str, Any] = params or {}

    def frame(self, *, t: float, frame_idx: int, brightness: int) -> bytes:
        raise NotImplementedError


# -----------------------
#  Basic patterns
# -----------------------


class Solid(Pattern):
    name = "solid"

    def frame(self, *, t: float, frame_idx: int, brightness: int) -> bytes:
        color = tuple(self.params.get("color", [255, 255, 255]))
        rgb = scale_rgb((int(color[0]), int(color[1]), int(color[2])), brightness)
        out = bytearray(self.ctx.led_count * 3)
        out[0::3] = bytes([rgb[0]]) * self.ctx.led_count
        out[1::3] = bytes([rgb[1]]) * self.ctx.led_count
        out[2::3] = bytes([rgb[2]]) * self.ctx.led_count
        return bytes(out)


class RainbowCycle(Pattern):
    name = "rainbow_cycle"

    def frame(self, *, t: float, frame_idx: int, brightness: int) -> bytes:
        speed = float(self.params.get("speed", 0.07))  # hue/sec
        spread = float(self.params.get("spread", 1.0))  # how many rainbows across strip
        out = bytearray(self.ctx.led_count * 3)
        n = self.ctx.led_count
        base = (t * speed) % 1.0
        for i in range(n):
            h = (base + (i / max(1, n)) * spread) % 1.0
            rgb = scale_rgb(hsv_to_rgb(h, 1.0, 1.0), brightness)
            j = i * 3
            out[j : j + 3] = bytes(rgb)
        return bytes(out)


class GlitterRainbow(Pattern):
    name = "glitter_rainbow"

    def frame(self, *, t: float, frame_idx: int, brightness: int) -> bytes:
        density = float(self.params.get("density", 0.02))
        rng = random.Random(int(self.params.get("seed", 1337)) + frame_idx)
        base = RainbowCycle(
            self.ctx,
            params={
                "speed": self.params.get("speed", 0.06),
                "spread": self.params.get("spread", 1.3),
            },
        ).frame(t=t, frame_idx=frame_idx, brightness=brightness)
        out = bytearray(base)
        n = self.ctx.led_count
        sparkles = int(n * density)
        for _ in range(max(1, sparkles)):
            i = rng.randrange(0, n)
            j = i * 3
            out[j : j + 3] = bytes(scale_rgb((255, 255, 255), brightness))
        return bytes(out)


class Twinkle(Pattern):
    name = "twinkle"

    def frame(self, *, t: float, frame_idx: int, brightness: int) -> bytes:
        rng = random.Random(int(self.params.get("seed", 42)))
        density = float(self.params.get("density", 0.03))
        fade = float(self.params.get("fade", 0.85))
        color = tuple(self.params.get("color", [255, 255, 255]))
        base = scale_rgb((int(color[0]), int(color[1]), int(color[2])), brightness)

        # Build per-frame deterministic twinkles based on time buckets
        bucket = int(t * float(self.params.get("rate", 6.0)))
        rng.seed(int(self.params.get("seed", 42)) + bucket)

        n = self.ctx.led_count
        out = bytearray(n * 3)
        # faint background
        bg = scale_rgb(base, int(brightness * float(self.params.get("bg", 0.08))))
        out[0::3] = bytes([bg[0]]) * n
        out[1::3] = bytes([bg[1]]) * n
        out[2::3] = bytes([bg[2]]) * n

        tw = int(n * density)
        for _ in range(max(1, tw)):
            i = rng.randrange(0, n)
            j = i * 3
            # twinkle intensity shaped by fractional time within bucket
            frac = (t * float(self.params.get("rate", 6.0))) - bucket
            amp = 1.0 - abs(frac - 0.5) * 2.0
            amp = max(0.0, amp) ** 1.7
            rgb = scale_rgb(base, int(brightness * amp))
            out[j : j + 3] = bytes(rgb)
        return bytes(out)


class Sparkle(Pattern):
    name = "sparkle"

    def frame(self, *, t: float, frame_idx: int, brightness: int) -> bytes:
        rng = random.Random(int(self.params.get("seed", 1234)) + frame_idx)
        density = float(self.params.get("density", 0.015))
        # faint color wash
        wash = tuple(self.params.get("wash", [8, 8, 8]))
        wash_rgb = scale_rgb((int(wash[0]), int(wash[1]), int(wash[2])), brightness)
        n = self.ctx.led_count
        out = bytearray(n * 3)
        out[0::3] = bytes([wash_rgb[0]]) * n
        out[1::3] = bytes([wash_rgb[1]]) * n
        out[2::3] = bytes([wash_rgb[2]]) * n

        sparkles = int(n * density)
        for _ in range(max(1, sparkles)):
            i = rng.randrange(0, n)
            rgb = scale_rgb((255, 255, 255), brightness)
            j = i * 3
            out[j : j + 3] = bytes(rgb)
        return bytes(out)


class Comet(Pattern):
    name = "comet"

    def frame(self, *, t: float, frame_idx: int, brightness: int) -> bytes:
        speed = float(self.params.get("speed", 220.0))  # px/sec
        tail = int(self.params.get("tail", 70))
        color = tuple(self.params.get("color", [255, 255, 255]))
        base = (int(color[0]), int(color[1]), int(color[2]))

        n = self.ctx.led_count
        head = int((t * speed) % max(1, n))
        out = bytearray(n * 3)

        for i in range(n):
            d = (head - i) % n
            if d < 0:
                d += n
            if d > tail:
                continue
            amp = 1.0 - (d / max(1, tail))
            amp = amp**2.0
            rgb = scale_rgb(base, int(brightness * amp))
            j = i * 3
            out[j : j + 3] = bytes(rgb)
        return bytes(out)


class TheaterChase(Pattern):
    name = "theater_chase"

    def frame(self, *, t: float, frame_idx: int, brightness: int) -> bytes:
        color = tuple(self.params.get("color", [255, 0, 0]))
        bg = tuple(self.params.get("bg", [0, 0, 0]))
        color = scale_rgb((int(color[0]), int(color[1]), int(color[2])), brightness)
        bg = scale_rgb((int(bg[0]), int(bg[1]), int(bg[2])), brightness)
        period = int(self.params.get("period", 3))
        speed = float(self.params.get("speed", 6.0))
        phase = int((t * speed) % period)
        n = self.ctx.led_count
        out = bytearray(n * 3)
        for i in range(n):
            rgb = color if ((i + phase) % period == 0) else bg
            j = i * 3
            out[j : j + 3] = bytes(rgb)
        return bytes(out)


class Strobe(Pattern):
    name = "strobe"

    def frame(self, *, t: float, frame_idx: int, brightness: int) -> bytes:
        hz = float(self.params.get("hz", 8.0))
        on = (int(t * hz) % 2) == 0
        color = tuple(self.params.get("color", [255, 255, 255]))
        rgb = (
            scale_rgb((int(color[0]), int(color[1]), int(color[2])), brightness)
            if on
            else (0, 0, 0)
        )
        out = bytearray(self.ctx.led_count * 3)
        out[0::3] = bytes([rgb[0]]) * self.ctx.led_count
        out[1::3] = bytes([rgb[1]]) * self.ctx.led_count
        out[2::3] = bytes([rgb[2]]) * self.ctx.led_count
        return bytes(out)


# -----------------------
#  Geometry-aware patterns
# -----------------------


class CandySpiral(Pattern):
    name = "candy_spiral"

    def frame(self, *, t: float, frame_idx: int, brightness: int) -> bytes:
        # Works best when geometry_enabled.
        speed = float(self.params.get("speed", 0.5))  # rotations/sec
        stripes = int(self.params.get("stripes", 8))
        red = scale_rgb((255, 0, 0), brightness)
        white = scale_rgb((255, 255, 255), brightness)
        alt = scale_rgb((160, 160, 160), brightness)

        n = self.ctx.led_count
        out = bytearray(n * 3)

        if not self.ctx.geometry_enabled:
            # fallback: 1D candy stripe scroll
            band = max(1, int(self.params.get("band", 12)))
            offset = int(t * float(self.params.get("scroll", 30.0)))
            for i in range(n):
                c = red if ((i + offset) // band) % 2 == 0 else white
                j = i * 3
                out[j : j + 3] = bytes(c)
            return bytes(out)

        for i in range(n):
            ang, y, _ = self.ctx.geometry.coords(i)
            # helical coordinate
            phase = (
                (ang / (2.0 * math.pi))
                + (y * float(self.params.get("twist", 1.2)))
                + (t * speed)
            )
            v = (phase * stripes) % 1.0
            c = red if v < 0.5 else white
            # soften edges
            edge = abs(v - 0.5) * 2.0
            if edge < 0.05:
                c = mix_rgb(red, white, 0.5)
            j = i * 3
            out[j : j + 3] = bytes(c)
        return bytes(out)


class VerticalWipe(Pattern):
    name = "vertical_wipe"

    def frame(self, *, t: float, frame_idx: int, brightness: int) -> bytes:
        speed = float(self.params.get("speed", 0.25))  # wipes/sec
        color = tuple(self.params.get("color", [0, 255, 0]))
        bg = tuple(self.params.get("bg", [0, 0, 0]))
        color = scale_rgb((int(color[0]), int(color[1]), int(color[2])), brightness)
        bg = scale_rgb((int(bg[0]), int(bg[1]), int(bg[2])), brightness)
        n = self.ctx.led_count
        out = bytearray(n * 3)

        if not self.ctx.geometry_enabled:
            # 1D wipe
            head = int(((t * speed) % 1.0) * n)
            for i in range(n):
                c = color if i <= head else bg
                j = i * 3
                out[j : j + 3] = bytes(c)
            return bytes(out)

        head_y = (t * speed) % 1.0
        feather = float(self.params.get("feather", 0.03))
        for i in range(n):
            _, y, _ = self.ctx.geometry.coords(i)
            if y <= head_y:
                c = color
            else:
                # feather edge
                d = (y - head_y) / max(1e-6, feather)
                if d < 1.0:
                    c = mix_rgb(color, bg, d)
                else:
                    c = bg
            j = i * 3
            out[j : j + 3] = bytes(c)
        return bytes(out)


class ColorWaves(Pattern):
    name = "color_waves"

    def frame(self, *, t: float, frame_idx: int, brightness: int) -> bytes:
        speed = float(self.params.get("speed", 0.25))
        out = bytearray(self.ctx.led_count * 3)
        n = self.ctx.led_count
        for i in range(n):
            x = i / max(1, n)
            w1 = 0.5 + 0.5 * math.sin(2 * math.pi * (x * 3.0 + t * speed))
            w2 = 0.5 + 0.5 * math.sin(2 * math.pi * (x * 7.0 - t * speed * 0.7))
            h = (w1 * 0.6 + w2 * 0.4 + t * 0.03) % 1.0
            v = 0.6 + 0.4 * math.sin(2 * math.pi * (x * 2.0 + t * speed * 0.33))
            rgb = hsv_to_rgb(h, 1.0, max(0.0, min(1.0, v)))
            rgb = scale_rgb(rgb, brightness)
            j = i * 3
            out[j : j + 3] = bytes(rgb)
        return bytes(out)


class Plasma(Pattern):
    name = "plasma"

    def frame(self, *, t: float, frame_idx: int, brightness: int) -> bytes:
        n = self.ctx.led_count
        out = bytearray(n * 3)
        speed = float(self.params.get("speed", 0.5))
        for i in range(n):
            if self.ctx.geometry_enabled:
                ang, y, _ = self.ctx.geometry.coords(i)
                x = ang / (2.0 * math.pi)
            else:
                x = i / max(1, n)
                y = i / max(1, n)
            v = (
                math.sin((x * 10.0 + t * speed) * 2.0)
                + math.sin((y * 6.0 - t * speed * 0.7) * 2.0)
                + math.sin((x * 4.0 + y * 4.0 + t * speed * 0.3) * 2.0)
            ) / 3.0
            h = (0.6 + v * 0.25 + t * 0.02) % 1.0
            rgb = scale_rgb(hsv_to_rgb(h, 1.0, 1.0), brightness)
            j = i * 3
            out[j : j + 3] = bytes(rgb)
        return bytes(out)


class Snowfall(Pattern):
    name = "snowfall"

    def frame(self, *, t: float, frame_idx: int, brightness: int) -> bytes:
        rng = random.Random(int(self.params.get("seed", 2024)))
        density = float(self.params.get("density", 0.008))
        speed = float(self.params.get("speed", 0.15))  # units/sec
        n = self.ctx.led_count
        out = bytearray(n * 3)

        # background faint blue
        bg = scale_rgb((0, 0, 16), brightness)
        out[0::3] = bytes([bg[0]]) * n
        out[1::3] = bytes([bg[1]]) * n
        out[2::3] = bytes([bg[2]]) * n

        flakes = max(1, int(n * density))
        # deterministic positions based on seed and "flake index"
        for k in range(flakes):
            rng.seed(int(self.params.get("seed", 2024)) + k)
            if self.ctx.geometry_enabled:
                # choose a run and y position, then move down
                run = rng.randrange(0, self.ctx.geometry.runs)
                y0 = rng.random()
                y = (y0 - (t * speed)) % 1.0
                pos = int(y * (self.ctx.geometry.pixels_per_run - 1))
                idx = run * self.ctx.geometry.pixels_per_run + pos
            else:
                i0 = rng.randrange(0, n)
                idx = int((i0 - (t * speed * n)) % n)
            j = idx * 3
            out[j : j + 3] = bytes(scale_rgb((255, 255, 255), brightness))
        return bytes(out)


class Confetti(Pattern):
    name = "confetti"

    def frame(self, *, t: float, frame_idx: int, brightness: int) -> bytes:
        rng = random.Random(int(self.params.get("seed", 9001)) + frame_idx)
        n = self.ctx.led_count
        out = bytearray(n * 3)

        # fade factor across frames simulated by using time bucket
        bg = scale_rgb((0, 0, 0), brightness)
        out[0::3] = bytes([bg[0]]) * n
        out[1::3] = bytes([bg[1]]) * n
        out[2::3] = bytes([bg[2]]) * n

        specks = max(1, int(n * float(self.params.get("density", 0.02))))
        for _ in range(specks):
            i = rng.randrange(0, n)
            h = rng.random()
            rgb = scale_rgb(hsv_to_rgb(h, 1.0, 1.0), brightness)
            j = i * 3
            out[j : j + 3] = bytes(rgb)
        return bytes(out)


class Aurora(Pattern):
    name = "aurora"

    def frame(self, *, t: float, frame_idx: int, brightness: int) -> bytes:
        n = self.ctx.led_count
        out = bytearray(n * 3)
        speed = float(self.params.get("speed", 0.18))
        for i in range(n):
            if self.ctx.geometry_enabled:
                ang, y, _ = self.ctx.geometry.coords(i)
                x = ang / (2 * math.pi)
            else:
                x = i / max(1, n)
                y = i / max(1, n)
            v = 0.5 + 0.5 * math.sin(2 * math.pi * (x * 1.7 + t * speed))
            v2 = 0.5 + 0.5 * math.sin(2 * math.pi * (y * 2.3 - t * speed * 0.7))
            h = (0.33 + 0.1 * math.sin(2 * math.pi * (x + y + t * 0.04))) % 1.0
            sat = 0.7 + 0.3 * v2
            val = 0.15 + 0.85 * (v * 0.6 + v2 * 0.4)
            rgb = scale_rgb(hsv_to_rgb(h, sat, min(1.0, val)), brightness)
            j = i * 3
            out[j : j + 3] = bytes(rgb)
        return bytes(out)


class Fireflies(Pattern):
    name = "fireflies"

    def frame(self, *, t: float, frame_idx: int, brightness: int) -> bytes:
        rng = random.Random(int(self.params.get("seed", 7)))
        n = self.ctx.led_count
        out = bytearray(n * 3)
        bg = scale_rgb((0, 0, 0), brightness)
        out[0::3] = bytes([bg[0]]) * n
        out[1::3] = bytes([bg[1]]) * n
        out[2::3] = bytes([bg[2]]) * n

        count = max(3, int(float(self.params.get("count", 18))))
        for k in range(count):
            rng.seed(int(self.params.get("seed", 7)) + k)
            center = rng.randrange(0, n)
            phase = rng.random() * 2.0 * math.pi
            freq = lerp(0.3, 1.5, rng.random())
            amp = max(0.0, math.sin(t * freq * 2.0 * math.pi + phase))
            amp = amp**2.2
            # warm color
            c = (255, int(lerp(120, 220, rng.random())), 40)
            rgb = scale_rgb(c, int(brightness * amp))
            j = center * 3
            out[j : j + 3] = bytes(rgb)
        return bytes(out)


class MatrixRain(Pattern):
    name = "matrix_rain"

    def frame(self, *, t: float, frame_idx: int, brightness: int) -> bytes:
        rng = random.Random(int(self.params.get("seed", 1338)))
        n = self.ctx.led_count
        out = bytearray(n * 3)
        # dim background green
        bg = scale_rgb((0, 8, 0), brightness)
        out[0::3] = bytes([bg[0]]) * n
        out[1::3] = bytes([bg[1]]) * n
        out[2::3] = bytes([bg[2]]) * n

        if not self.ctx.geometry_enabled:
            # 1D: draw random streaks
            streaks = max(5, int(float(self.params.get("streaks", 30))))
            for k in range(streaks):
                rng.seed(int(self.params.get("seed", 1338)) + k)
                start = rng.randrange(0, n)
                length = rng.randrange(8, 40)
                speed = lerp(0.2, 1.0, rng.random())
                head = int((start + t * speed * n) % n)
                for d in range(length):
                    i = (head - d) % n
                    amp = max(0.0, 1.0 - d / max(1, length))
                    rgb = scale_rgb((0, 255, 80), int(brightness * amp))
                    j = i * 3
                    out[j : j + 3] = bytes(rgb)
            return bytes(out)

        # Geometry: per-run vertical streaks
        runs = self.ctx.geometry.runs
        ppr = self.ctx.geometry.pixels_per_run
        streaks_per_run = max(1, int(float(self.params.get("streaks_per_run", 1))))
        for run in range(runs):
            for k in range(streaks_per_run):
                seed = int(self.params.get("seed", 1338)) + run * 100 + k
                rng.seed(seed)
                length = rng.randrange(10, 60)
                y0 = rng.random()
                speed = lerp(0.05, 0.35, rng.random())
                y = (y0 - t * speed) % 1.0
                head = int(y * (ppr - 1))
                for d in range(length):
                    pos = head + d
                    if pos >= ppr:
                        break
                    idx = run * ppr + pos
                    amp = max(0.0, 1.0 - d / max(1, length))
                    rgb = scale_rgb((0, 255, 80), int(brightness * amp))
                    j = idx * 3
                    out[j : j + 3] = bytes(rgb)
        return bytes(out)


class BreathingSolid(Pattern):
    name = "breathing_solid"

    def frame(self, *, t: float, frame_idx: int, brightness: int) -> bytes:
        color = tuple(self.params.get("color", [255, 30, 0]))
        base = (int(color[0]), int(color[1]), int(color[2]))
        hz = float(self.params.get("hz", 0.18))
        # smooth breathing between 10% and 100%
        breath = 0.10 + 0.90 * (0.5 + 0.5 * math.sin(2.0 * math.pi * t * hz)) ** 1.3
        bri = int(brightness * breath)
        rgb = scale_rgb(base, bri)
        out = bytearray(self.ctx.led_count * 3)
        out[0::3] = bytes([rgb[0]]) * self.ctx.led_count
        out[1::3] = bytes([rgb[1]]) * self.ctx.led_count
        out[2::3] = bytes([rgb[2]]) * self.ctx.led_count
        return bytes(out)


class GradientScroll(Pattern):
    name = "gradient_scroll"

    def frame(self, *, t: float, frame_idx: int, brightness: int) -> bytes:
        c1 = tuple(self.params.get("c1", [255, 0, 0]))
        c2 = tuple(self.params.get("c2", [0, 255, 0]))
        c1 = (int(c1[0]), int(c1[1]), int(c1[2]))
        c2 = (int(c2[0]), int(c2[1]), int(c2[2]))
        speed = float(self.params.get("speed", 0.12))
        out = bytearray(self.ctx.led_count * 3)
        n = self.ctx.led_count
        for i in range(n):
            x = (i / max(1, n)) + t * speed
            x = x % 1.0
            rgb = mix_rgb(c1, c2, x)
            rgb = scale_rgb(rgb, brightness)
            j = i * 3
            out[j : j + 3] = bytes(rgb)
        return bytes(out)


class BarberPole(Pattern):
    name = "barber_pole"

    def frame(self, *, t: float, frame_idx: int, brightness: int) -> bytes:
        c1 = scale_rgb(tuple(self.params.get("c1", [255, 0, 0])), brightness)
        c2 = scale_rgb(tuple(self.params.get("c2", [255, 255, 255])), brightness)
        stripes = int(self.params.get("stripes", 10))
        twist = float(self.params.get("twist", 1.0))
        speed = float(self.params.get("speed", 0.4))
        out = bytearray(self.ctx.led_count * 3)
        n = self.ctx.led_count

        if not self.ctx.geometry_enabled:
            band = max(1, int(self.params.get("band", 16)))
            offset = int(t * float(self.params.get("scroll", 40.0)))
            for i in range(n):
                c = c1 if ((i + offset) // band) % 2 == 0 else c2
                j = i * 3
                out[j : j + 3] = bytes(c)
            return bytes(out)

        for i in range(n):
            ang, y, _ = self.ctx.geometry.coords(i)
            phase = (ang / (2.0 * math.pi)) + y * twist + t * speed
            v = (phase * stripes) % 1.0
            c = c1 if v < 0.5 else c2
            j = i * 3
            out[j : j + 3] = bytes(c)
        return bytes(out)


class SpiralRainbow(Pattern):
    name = "spiral_rainbow"

    def frame(self, *, t: float, frame_idx: int, brightness: int) -> bytes:
        speed = float(self.params.get("speed", 0.25))
        twist = float(self.params.get("twist", 1.4))
        out = bytearray(self.ctx.led_count * 3)
        n = self.ctx.led_count
        for i in range(n):
            if self.ctx.geometry_enabled:
                ang, y, _ = self.ctx.geometry.coords(i)
                base = (ang / (2.0 * math.pi)) + y * twist + t * speed
            else:
                base = (i / max(1, n)) * twist + t * speed
            h = base % 1.0
            rgb = scale_rgb(hsv_to_rgb(h, 1.0, 1.0), brightness)
            j = i * 3
            out[j : j + 3] = bytes(rgb)
        return bytes(out)


def _hash01(i: int, seed: int) -> float:
    x = (i * 0x1F123BB5) ^ (seed * 0x9E3779B9)
    x = (x ^ (x >> 16)) & 0xFFFFFFFF
    x = (x * 0x7FEB352D) & 0xFFFFFFFF
    x = (x ^ (x >> 15)) & 0xFFFFFFFF
    return (x & 0xFFFF) / 65535.0


class FireFlicker(Pattern):
    name = "fire_flicker"

    def frame(self, *, t: float, frame_idx: int, brightness: int) -> bytes:
        seed = int(self.params.get("seed", 99))
        n = self.ctx.led_count
        out = bytearray(n * 3)
        speed = float(self.params.get("speed", 2.0))
        for i in range(n):
            if self.ctx.geometry_enabled:
                _, y, _ = self.ctx.geometry.coords(i)
                heat = max(0.0, 1.0 - y)  # hotter at bottom
            else:
                heat = 1.0 - (i / max(1, n))
            flick = 0.4 + 0.6 * math.sin((t * speed) + _hash01(i, seed) * 6.28)
            flick = max(0.0, flick)
            v = min(1.0, heat * 0.7 + flick * 0.6)
            # warm gradient: deep red -> orange -> yellow-white
            if v < 0.33:
                rgb = mix_rgb((80, 0, 0), (255, 30, 0), v / 0.33)
            elif v < 0.66:
                rgb = mix_rgb((255, 30, 0), (255, 140, 0), (v - 0.33) / 0.33)
            else:
                rgb = mix_rgb((255, 140, 0), (255, 240, 200), (v - 0.66) / 0.34)
            rgb = scale_rgb(rgb, brightness)
            j = i * 3
            out[j : j + 3] = bytes(rgb)
        return bytes(out)


class PulseRings(Pattern):
    name = "pulse_rings"

    def frame(self, *, t: float, frame_idx: int, brightness: int) -> bytes:
        base_h = float(self.params.get("hue", 0.0))
        speed = float(self.params.get("speed", 0.3))
        rings = int(self.params.get("rings", 6))
        out = bytearray(self.ctx.led_count * 3)
        n = self.ctx.led_count

        for i in range(n):
            if self.ctx.geometry_enabled:
                _, y, _ = self.ctx.geometry.coords(i)
            else:
                y = i / max(1, n)
            v = 0.5 + 0.5 * math.sin(2.0 * math.pi * (y * rings - t * speed))
            h = (base_h + 0.15 * math.sin(2.0 * math.pi * (t * 0.07 + y))) % 1.0
            rgb = scale_rgb(hsv_to_rgb(h, 1.0, max(0.0, v)), brightness)
            j = i * 3
            out[j : j + 3] = bytes(rgb)
        return bytes(out)


class LaserSweep(Pattern):
    name = "laser_sweep"

    def frame(self, *, t: float, frame_idx: int, brightness: int) -> bytes:
        # A bright vertical plane sweeping around the tree.
        color = tuple(self.params.get("color", [0, 255, 80]))
        color = scale_rgb((int(color[0]), int(color[1]), int(color[2])), brightness)
        bg = scale_rgb((0, 0, 0), int(brightness * 0.05))
        speed = float(self.params.get("speed", 0.25))  # rotations/sec
        width = float(self.params.get("width", 0.08))  # fraction of circle
        n = self.ctx.led_count
        out = bytearray(n * 3)

        if not self.ctx.geometry_enabled:
            # 1D scan bar
            head = int(((t * speed) % 1.0) * n)
            w = max(3, int(width * n))
            for i in range(n):
                d = abs(i - head)
                amp = max(0.0, 1.0 - d / max(1, w))
                rgb = mix_rgb(bg, color, amp)
                j = i * 3
                out[j : j + 3] = bytes(rgb)
            return bytes(out)

        head = (t * speed) % 1.0
        for i in range(n):
            ang, _, _ = self.ctx.geometry.coords(i)
            x = (ang / (2.0 * math.pi)) % 1.0
            d = abs(x - head)
            d = min(d, 1.0 - d)  # wrap
            amp = max(0.0, 1.0 - d / max(1e-6, width))
            rgb = mix_rgb(bg, color, amp)
            j = i * 3
            out[j : j + 3] = bytes(rgb)
        return bytes(out)


class StaticNoise(Pattern):
    name = "static_noise"

    def frame(self, *, t: float, frame_idx: int, brightness: int) -> bytes:
        seed = int(self.params.get("seed", 31415))
        n = self.ctx.led_count
        out = bytearray(n * 3)
        # slight temporal smoothing by bucket
        bucket = int(t * float(self.params.get("rate", 10.0)))
        for i in range(n):
            v = _hash01(i + bucket * 131, seed)
            h = _hash01(i + bucket * 17, seed + 1)
            rgb = scale_rgb(hsv_to_rgb(h, 1.0, v), brightness)
            j = i * 3
            out[j : j + 3] = bytes(rgb)
        return bytes(out)


class Cylon(Pattern):
    name = "cylon"

    def frame(self, *, t: float, frame_idx: int, brightness: int) -> bytes:
        color = tuple(self.params.get("color", [255, 0, 0]))
        color = (int(color[0]), int(color[1]), int(color[2]))
        color = scale_rgb(color, brightness)
        n = self.ctx.led_count
        out = bytearray(n * 3)
        speed = float(self.params.get("speed", 0.22))
        width = int(self.params.get("width", 20))
        # sawtooth back and forth
        ph = (t * speed) % 2.0
        pos = ph if ph <= 1.0 else 2.0 - ph
        head = int(pos * (n - 1))
        for i in range(n):
            d = abs(i - head)
            if d > width:
                continue
            amp = (1.0 - d / max(1, width)) ** 2.2
            rgb = scale_rgb(color, int(brightness * amp))
            j = i * 3
            out[j : j + 3] = bytes(rgb)
        return bytes(out)


class Checker(Pattern):
    name = "checker"

    def frame(self, *, t: float, frame_idx: int, brightness: int) -> bytes:
        c1 = scale_rgb(tuple(self.params.get("c1", [255, 0, 0])), brightness)
        c2 = scale_rgb(tuple(self.params.get("c2", [0, 255, 0])), brightness)
        block = int(self.params.get("block", 10))
        speed = float(self.params.get("speed", 0.6))
        shift = int((t * speed) * block)  # animate
        n = self.ctx.led_count
        out = bytearray(n * 3)
        for i in range(n):
            v = ((i + shift) // max(1, block)) % 2
            rgb = c1 if v == 0 else c2
            j = i * 3
            out[j : j + 3] = bytes(rgb)
        return bytes(out)


class WipeRandom(Pattern):
    name = "wipe_random"

    def frame(self, *, t: float, frame_idx: int, brightness: int) -> bytes:
        speed = float(self.params.get("speed", 0.15))
        n = self.ctx.led_count
        out = bytearray(n * 3)
        # pick new color each wipe
        wipe = int(t * speed)
        frac = (t * speed) - wipe
        h = _hash01(wipe, int(self.params.get("seed", 777))) % 1.0
        rgb = scale_rgb(hsv_to_rgb(h, 1.0, 1.0), brightness)
        head = int(frac * n)
        for i in range(n):
            j = i * 3
            if i <= head:
                out[j : j + 3] = bytes(rgb)
        return bytes(out)


# -----------------------
# Segment-aware patterns
# -----------------------


def _seg_info(
    ctx: RenderContext, i: int, default_segments: int = 4
) -> tuple[int, int, int, int]:
    """
    Returns (seg_order, seg_count, local_index, seg_len).

    - seg_order is 0..seg_count-1 in physical order (sorted by start if segment_layout exists).
    - local_index is 0..seg_len-1 within that segment.
    """
    n = int(ctx.led_count)
    if ctx.segment_layout and ctx.segment_layout.segments:
        segs = ctx.segment_layout.segments
        seg_count = len(segs)
        order = ctx.segment_layout.order_for_index(i)
        if order is None:
            # fallback to proportional if idx outside known ranges
            seg_len = max(1, n // max(1, seg_count))
            order = min(seg_count - 1, max(0, int(i // seg_len)))
            local = i - order * seg_len
            return int(order), int(seg_count), int(local), int(seg_len)
        seg = segs[int(order)]
        local = int(i) - int(seg.start)
        seg_len = max(1, seg.length)
        local = max(0, min(seg_len - 1, local))
        return int(order), int(seg_count), int(local), int(seg_len)

    seg_count = max(1, int(default_segments))
    seg_len = max(1, n // seg_count)
    order = min(seg_count - 1, max(0, int(i) // seg_len))
    local = int(i) - order * seg_len
    return int(order), int(seg_count), int(local), int(seg_len)


class QuadChase(Pattern):
    """A rotating 4-quadrant spotlight/chase (segment-aware if layout is available)."""

    name = "quad_chase"

    def frame(self, *, t: float, frame_idx: int, brightness: int) -> bytes:
        speed = float(self.params.get("speed", 0.6))  # segments per second
        tail = float(self.params.get("tail", 1.6))  # higher = wider spill
        hue_speed = float(self.params.get("hue_speed", 0.06))
        phase_offset = float(
            self.params.get("phase_offset", 0.0)
        )  # segment-order offset at t=0

        n = self.ctx.led_count
        out = bytearray(n * 3)

        # segment motion in "segment space"
        # phase: 0..seg_count, wraps
        # active segment is int(phase) but we do smooth spill based on distance
        # (this looks good even if you have >4 segments)
        # Determine seg_count from layout if present
        _, seg_count, _, _ = _seg_info(self.ctx, 0)
        phase = (t * speed + phase_offset) % max(1.0, float(seg_count))

        for i in range(n):
            seg_order, sc, _, _ = _seg_info(self.ctx, i, default_segments=seg_count)

            # distance on a ring (0 is active)
            d = abs((seg_order - phase) % sc)
            d = min(d, sc - d)
            w = max(0.0, 1.0 - (d / max(1e-6, tail)))

            hue = (seg_order / max(1.0, float(sc)) + (t * hue_speed)) % 1.0
            rgb = scale_rgb(hsv_to_rgb(hue, 1.0, w), brightness)
            j = i * 3
            out[j : j + 3] = bytes(rgb)

        return bytes(out)


class OppositePulse(Pattern):
    """Pulse opposite quadrants (0&2 vs 1&3)."""

    name = "opposite_pulse"

    def frame(self, *, t: float, frame_idx: int, brightness: int) -> bytes:
        freq = float(self.params.get("speed", 0.4))
        n = self.ctx.led_count
        out = bytearray(n * 3)

        _, seg_count, _, _ = _seg_info(self.ctx, 0)
        seg_count = max(2, seg_count)

        p = 0.5 * (1.0 + math.sin(2.0 * math.pi * freq * t))  # 0..1

        for i in range(n):
            seg_order, sc, _, _ = _seg_info(self.ctx, i, default_segments=seg_count)
            # even vs odd segments
            w = p if (seg_order % 2 == 0) else (1.0 - p)
            hue = 0.0 if (seg_order % 2 == 0) else 0.33
            rgb = scale_rgb(hsv_to_rgb(hue, 1.0, w), brightness)
            j = i * 3
            out[j : j + 3] = bytes(rgb)

        return bytes(out)


class QuadTwinkle(Pattern):
    """Twinkle with different base hue per segment."""

    name = "quad_twinkle"

    def frame(self, *, t: float, frame_idx: int, brightness: int) -> bytes:
        density = float(self.params.get("density", 0.06))
        speed = float(self.params.get("speed", 0.6))
        seed = int(self.params.get("seed", 424242))

        n = self.ctx.led_count
        out = bytearray(n * 3)

        _, seg_count, _, _ = _seg_info(self.ctx, 0)
        seg_count = max(1, seg_count)

        for i in range(n):
            seg_order, sc, _, _ = _seg_info(self.ctx, i, default_segments=seg_count)
            base_h = (seg_order / max(1.0, float(sc))) % 1.0

            r = _hash01(i, seed)
            if r > density:
                continue

            # A per-pixel triangle wave with randomized phase
            ph = (t * speed + r * 7.0) % 1.0
            tw = 1.0 - abs(ph - 0.5) * 2.0  # 0..1..0
            tw = max(0.0, min(1.0, tw))

            rgb = scale_rgb(hsv_to_rgb(base_h, 1.0, tw), brightness)
            j = i * 3
            out[j : j + 3] = bytes(rgb)

        return bytes(out)


class QuadComets(Pattern):
    """One comet per segment, each moving within its own segment range."""

    name = "quad_comets"

    def frame(self, *, t: float, frame_idx: int, brightness: int) -> bytes:
        speed = float(self.params.get("speed", 0.22))  # segment-lengths per second
        tail = int(self.params.get("tail", 90))

        n = self.ctx.led_count
        out = bytearray(n * 3)

        _, seg_count, _, seg_len0 = _seg_info(self.ctx, 0)
        seg_count = max(1, seg_count)

        for i in range(n):
            seg_order, sc, local, seg_len = _seg_info(
                self.ctx, i, default_segments=seg_count
            )
            head = (t * speed * seg_len) % max(1.0, float(seg_len))
            d = local - head
            if d < 0:
                d += seg_len  # wrap within segment
            w = max(0.0, 1.0 - (d / max(1.0, float(tail))))
            hue = (seg_order / max(1.0, float(sc))) % 1.0
            rgb = scale_rgb(hsv_to_rgb(hue, 1.0, w), brightness)
            j = i * 3
            out[j : j + 3] = bytes(rgb)

        return bytes(out)


class QuadSpiral(Pattern):
    """A spiral that advances around the tree, with per-segment phase offsets when known."""

    name = "quad_spiral"

    def frame(self, *, t: float, frame_idx: int, brightness: int) -> bytes:
        speed = float(self.params.get("speed", 0.18))
        stripes = float(self.params.get("stripes", 10.0))
        twist = float(self.params.get("twist", 2.2))
        seg_phase = float(self.params.get("seg_phase", 0.25))
        phase_offset = float(self.params.get("phase_offset", 0.0))

        n = self.ctx.led_count
        out = bytearray(n * 3)

        _, seg_count, _, _ = _seg_info(self.ctx, 0)
        seg_count = max(1, seg_count)

        for i in range(n):
            seg_order, sc, _, _ = _seg_info(self.ctx, i, default_segments=seg_count)
            if self.ctx.geometry_enabled:
                angle, y, _ = self.ctx.geometry.coords(i)
                theta = (angle / (2.0 * math.pi)) % 1.0
            else:
                theta = (i / max(1.0, float(n))) % 1.0
                y = theta

            v = (
                theta * stripes
                + y * twist
                + ((seg_order + phase_offset) / max(1.0, float(sc))) * seg_phase
                + t * speed
            ) % 1.0
            # Smooth stripes
            b = 0.5 + 0.5 * math.sin(2.0 * math.pi * v)
            b = max(0.0, min(1.0, b))
            hue = (v + 0.15) % 1.0
            rgb = scale_rgb(hsv_to_rgb(hue, 1.0, b), brightness)
            j = i * 3
            out[j : j + 3] = bytes(rgb)

        return bytes(out)


# -----------------------
# Registry + factory
# -----------------------

PATTERN_REGISTRY: Dict[str, type[Pattern]] = {
    Solid.name: Solid,
    RainbowCycle.name: RainbowCycle,
    GlitterRainbow.name: GlitterRainbow,
    Twinkle.name: Twinkle,
    Sparkle.name: Sparkle,
    Comet.name: Comet,
    TheaterChase.name: TheaterChase,
    Strobe.name: Strobe,
    CandySpiral.name: CandySpiral,
    VerticalWipe.name: VerticalWipe,
    ColorWaves.name: ColorWaves,
    Plasma.name: Plasma,
    Snowfall.name: Snowfall,
    Confetti.name: Confetti,
    Aurora.name: Aurora,
    Fireflies.name: Fireflies,
    MatrixRain.name: MatrixRain,
    BreathingSolid.name: BreathingSolid,
    GradientScroll.name: GradientScroll,
    BarberPole.name: BarberPole,
    SpiralRainbow.name: SpiralRainbow,
    FireFlicker.name: FireFlicker,
    PulseRings.name: PulseRings,
    LaserSweep.name: LaserSweep,
    StaticNoise.name: StaticNoise,
    Cylon.name: Cylon,
    Checker.name: Checker,
    WipeRandom.name: WipeRandom,
    QuadChase.name: QuadChase,
    OppositePulse.name: OppositePulse,
    QuadTwinkle.name: QuadTwinkle,
    QuadComets.name: QuadComets,
    QuadSpiral.name: QuadSpiral,
}


class PatternFactory:
    def __init__(
        self,
        led_count: int,
        geometry: TreeGeometry,
        segment_layout: SegmentLayout | None = None,
    ) -> None:
        self.led_count = led_count
        self.geometry = geometry
        self.segment_layout = segment_layout

    def available(self) -> List[str]:
        return sorted(PATTERN_REGISTRY.keys())

    def create(self, name: str, params: Optional[Dict[str, Any]] = None) -> Pattern:
        cls = PATTERN_REGISTRY.get(name)
        if cls is None:
            raise ValueError(
                f"Unknown pattern '{name}'. Available: {', '.join(self.available())}"
            )
        ctx = RenderContext(
            led_count=self.led_count,
            geometry=self.geometry,
            geometry_enabled=self.geometry.enabled_for(self.led_count),
            segment_layout=getattr(self, "segment_layout", None),
        )
        return cls(ctx, params=params or {})
