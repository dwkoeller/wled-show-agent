from __future__ import annotations

import json

import pytest

from sequence_service import SequenceService


class _Dummy:
    pass


def test_sequence_generate_beat_aligned(tmp_path) -> None:
    svc = SequenceService(
        wled=_Dummy(), looks=_Dummy(), ddp=_Dummy(), data_dir=str(tmp_path)
    )

    looks = [{"id": 1}, {"id": 2}]
    beats_s = [i * 0.5 for i in range(0, 40)]  # 20 seconds worth of beats

    fname = svc.generate(
        name="BeatMix",
        looks=looks,
        duration_s=4,
        step_s=8,  # ignored when beats_s is provided
        include_ddp=False,
        beats_s=beats_s,
        beats_per_step=4,  # 2 seconds/step
        ddp_patterns=["rainbow_cycle"],
        seed=1,
    )

    seq = json.loads((tmp_path / "sequences" / fname).read_text(encoding="utf-8"))
    steps = list(seq.get("steps") or [])

    assert len(steps) == 2
    assert steps[0]["duration_s"] == pytest.approx(2.0, rel=1e-6)
    assert steps[1]["duration_s"] == pytest.approx(2.0, rel=1e-6)
    assert sum(float(s["duration_s"]) for s in steps) == pytest.approx(4.0, rel=1e-6)


def test_sequence_generate_step_s_fills_duration(tmp_path) -> None:
    svc = SequenceService(
        wled=_Dummy(), looks=_Dummy(), ddp=_Dummy(), data_dir=str(tmp_path)
    )

    looks = [{"id": 1}]
    fname = svc.generate(
        name="Fixed",
        looks=looks,
        duration_s=10,
        step_s=4,
        include_ddp=False,
        ddp_patterns=["rainbow_cycle"],
        seed=1,
    )

    seq = json.loads((tmp_path / "sequences" / fname).read_text(encoding="utf-8"))
    steps = list(seq.get("steps") or [])

    assert [float(s["duration_s"]) for s in steps] == [4.0, 4.0, 2.0]
    assert sum(float(s["duration_s"]) for s in steps) == pytest.approx(10.0, rel=1e-6)
