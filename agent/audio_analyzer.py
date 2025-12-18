from __future__ import annotations

import math
import os
import subprocess
import tempfile
import wave
from array import array
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

from jobs import JobCanceled


class AudioAnalyzeError(RuntimeError):
    pass


@dataclass(frozen=True)
class BeatAnalysis:
    bpm: float
    beats_s: List[float]
    duration_s: float
    sample_rate_hz: int
    method: str

    def as_dict(self) -> Dict[str, object]:
        return {
            "bpm": float(self.bpm),
            "beats_s": [float(x) for x in self.beats_s],
            "duration_s": float(self.duration_s),
            "sample_rate_hz": int(self.sample_rate_hz),
            "method": str(self.method),
        }


def _has_cmd(name: str) -> bool:
    from shutil import which

    return which(name) is not None


def _decode_to_wav_pcm(
    *,
    in_path: str,
    sample_rate_hz: int = 44100,
) -> str:
    """
    Decode arbitrary audio to a temporary mono 16-bit PCM WAV via ffmpeg.
    """
    if not _has_cmd("ffmpeg"):
        raise AudioAnalyzeError(
            "ffmpeg not found; only .wav files are supported without ffmpeg"
        )
    tmp = tempfile.NamedTemporaryFile(prefix="wsa_audio_", suffix=".wav", delete=False)
    tmp.close()
    out = tmp.name
    cmd = [
        "ffmpeg",
        "-y",
        "-i",
        str(in_path),
        "-ac",
        "1",
        "-ar",
        str(int(sample_rate_hz)),
        "-f",
        "wav",
        "-acodec",
        "pcm_s16le",
        out,
    ]
    try:
        p = subprocess.run(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False, text=True
        )
    except Exception as e:
        raise AudioAnalyzeError(f"ffmpeg failed: {e}")
    if p.returncode != 0:
        raise AudioAnalyzeError(f"ffmpeg decode failed: {p.stderr[-500:]}")
    return out


def _read_wav_mono_s16(path: str) -> Tuple[int, array]:
    p = Path(path)
    if not p.is_file():
        raise AudioAnalyzeError("Audio file not found")
    with wave.open(str(p), "rb") as wf:
        nch = int(wf.getnchannels())
        sampwidth = int(wf.getsampwidth())
        sr = int(wf.getframerate())
        nframes = int(wf.getnframes())
        if sampwidth != 2:
            raise AudioAnalyzeError(
                f"Unsupported WAV sample width: {sampwidth * 8} bits (expected 16-bit PCM)"
            )
        raw = wf.readframes(nframes)

    samples = array("h")
    samples.frombytes(raw)
    if nch <= 1:
        return sr, samples

    # Downmix to mono (average channels)
    mono = array("h")
    frames = len(samples) // nch
    for i in range(frames):
        acc = 0
        base = i * nch
        for c in range(nch):
            acc += int(samples[base + c])
        mono.append(int(acc / nch))
    return sr, mono


def analyze_beats(
    *,
    audio_path: str,
    min_bpm: int = 60,
    max_bpm: int = 200,
    hop_ms: int = 10,
    window_ms: int = 50,
    peak_threshold: float = 1.35,
    min_interval_s: float = 0.20,
    prefer_ffmpeg: bool = True,
    progress_cb: Callable[[float, float, str], None] | None = None,
    cancel_cb: Callable[[], bool] | None = None,
) -> BeatAnalysis:
    """
    Lightweight, dependency-free beat detection.

    - If ffmpeg is available (and prefer_ffmpeg=True), we decode non-WAV formats to WAV PCM.
    - Beat detection uses short-time energy deltas + peak picking.
    """
    src = str(audio_path)
    temp_wav: Optional[str] = None
    try:
        if cancel_cb and cancel_cb():
            raise JobCanceled("Job canceled")

        ext = os.path.splitext(src)[1].lower()
        wav_path = src
        method = "wav_energy_peaks"
        if ext != ".wav":
            if prefer_ffmpeg:
                temp_wav = _decode_to_wav_pcm(in_path=src)
                wav_path = temp_wav
                method = "ffmpeg->wav_energy_peaks"
            else:
                raise AudioAnalyzeError(
                    "Only .wav is supported (set prefer_ffmpeg=true to enable ffmpeg decoding)."
                )

        sr, samples = _read_wav_mono_s16(wav_path)
        if sr <= 0 or len(samples) < 1000:
            raise AudioAnalyzeError("Audio too short to analyze")

        hop = max(1, int(sr * (max(5, int(hop_ms)) / 1000.0)))
        win = max(hop, int(sr * (max(10, int(window_ms)) / 1000.0)))

        # Short-time energy
        total_windows = (
            max(1, int((len(samples) - win) / float(hop))) if len(samples) > win else 1
        )
        energies: List[float] = []
        processed = 0
        report_every = max(100, int(total_windows // 200) or 1)
        for start in range(0, len(samples) - win, hop):
            if cancel_cb and cancel_cb():
                raise JobCanceled("Job canceled")
            acc = 0.0
            for s in samples[start : start + win]:
                x = float(s) / 32768.0
                acc += x * x
            energies.append(acc / float(win))
            processed += 1
            if progress_cb and (processed % report_every == 0):
                progress_cb(float(processed), float(total_windows), "Analyzing audio…")

        if len(energies) < 8:
            raise AudioAnalyzeError("Audio too short to analyze")

        # Onset strength: positive energy delta
        onset: List[float] = [0.0]
        for i in range(1, len(energies)):
            d = energies[i] - energies[i - 1]
            onset.append(d if d > 0 else 0.0)

        mean = sum(onset) / float(len(onset))
        var = sum((x - mean) ** 2 for x in onset) / float(max(1, len(onset) - 1))
        std = math.sqrt(var)
        thr = mean + (std * float(peak_threshold))

        # Peak picking
        beats_idx: List[int] = []
        last_t = -1e9
        for i in range(1, len(onset) - 1):
            if onset[i] < thr:
                continue
            if onset[i] < onset[i - 1] or onset[i] < onset[i + 1]:
                continue
            t = (i * hop) / float(sr)
            if t - last_t < float(min_interval_s):
                continue
            beats_idx.append(i)
            last_t = t

        beats_s = [(i * hop) / float(sr) for i in beats_idx]
        duration_s = len(samples) / float(sr)

        # Estimate BPM from beat intervals (median)
        intervals = [beats_s[i] - beats_s[i - 1] for i in range(1, len(beats_s))]
        bpm = 0.0
        if intervals:
            vals = []
            for dt in intervals:
                if dt <= 0:
                    continue
                b = 60.0 / dt
                if float(min_bpm) <= b <= float(max_bpm):
                    vals.append(b)
            if vals:
                vals.sort()
                mid = len(vals) // 2
                bpm = (
                    vals[mid]
                    if len(vals) % 2 == 1
                    else (vals[mid - 1] + vals[mid]) / 2.0
                )

        return BeatAnalysis(
            bpm=float(bpm),
            beats_s=beats_s,
            duration_s=float(duration_s),
            sample_rate_hz=int(sr),
            method=method,
        )
    finally:
        if temp_wav:
            try:
                Path(temp_wav).unlink(missing_ok=True)  # type: ignore[arg-type]
            except Exception:
                pass
