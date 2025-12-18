from __future__ import annotations

import wave
from array import array

from audio_analyzer import analyze_beats


def _write_click_track_wav(
    *,
    path: str,
    bpm: float = 120.0,
    duration_s: float = 10.0,
    sample_rate_hz: int = 44100,
    click_ms: float = 20.0,
    amp: int = 28000,
) -> None:
    n = int(duration_s * sample_rate_hz)
    samples = array("h", [0] * n)

    interval_s = 60.0 / float(bpm)
    click_len = max(1, int((click_ms / 1000.0) * sample_rate_hz))

    t = 0.0
    while t < duration_s:
        idx = int(t * sample_rate_hz)
        for i in range(click_len):
            j = idx + i
            if j >= n:
                break
            samples[j] = int(amp)
        t += interval_s

    with wave.open(str(path), "wb") as wf:
        wf.setnchannels(1)
        wf.setsampwidth(2)
        wf.setframerate(int(sample_rate_hz))
        wf.writeframes(samples.tobytes())


def test_analyze_beats_click_track(tmp_path) -> None:
    wav = tmp_path / "click.wav"
    _write_click_track_wav(path=str(wav), bpm=120.0, duration_s=10.0)

    analysis = analyze_beats(audio_path=str(wav), min_bpm=80, max_bpm=160)

    assert analysis.duration_s > 9.0
    assert len(analysis.beats_s) >= 15
    assert 110.0 <= analysis.bpm <= 130.0
