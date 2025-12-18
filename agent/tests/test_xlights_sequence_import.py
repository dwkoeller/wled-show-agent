from __future__ import annotations

import pytest

from xlights_sequence_import import (
    import_xlights_xsq_timing_file,
    parse_xlights_xsq_timing_tracks,
)


def test_parse_xsq_timing_tracks_ms_to_seconds() -> None:
    xml = """
    <xsequence>
      <timing name="Beat">0,500,1000,1500,2000,2500</timing>
      <timing name="Half">0,1000,2000</timing>
    </xsequence>
    """.strip()

    tracks = parse_xlights_xsq_timing_tracks(xml)
    assert tracks
    assert tracks[0].name == "Beat"
    assert tracks[0].marks_s[:3] == pytest.approx([0.0, 0.5, 1.0], rel=1e-6)


def test_import_xsq_timing_file_select_track(tmp_path) -> None:
    xsq = tmp_path / "song.xsq"
    xsq.write_text(
        """
        <xsequence>
          <timing name="Beat">0,500,1000,1500,2000,2500,3000</timing>
          <timing name="Slow">0,1000,2000,3000</timing>
        </xsequence>
        """.strip(),
        encoding="utf-8",
    )

    analysis = import_xlights_xsq_timing_file(xsq_path=str(xsq), timing_track="Beat")
    assert analysis["method"].startswith("xlights_xsq_timing:")
    assert analysis["beats_s"][1] == pytest.approx(0.5, rel=1e-6)
    assert analysis["bpm"] == pytest.approx(120.0, rel=0.05)
