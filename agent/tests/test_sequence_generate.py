from __future__ import annotations

import json

from utils.sequence_generate import generate_sequence_file


def test_generate_sequence_file_writes(tmp_path) -> None:
    looks = [{"id": 1}, {"id": 2}]
    fname = generate_sequence_file(
        data_dir=str(tmp_path),
        name="Quick",
        looks=looks,
        duration_s=3,
        step_s=1,
        include_ddp=False,
        renderable_only=False,
        ddp_patterns=["rainbow_cycle"],
        seed=7,
    )

    out_path = tmp_path / "sequences" / fname
    assert out_path.exists()
    payload = json.loads(out_path.read_text(encoding="utf-8"))
    assert payload.get("name") == "Quick"
    assert isinstance(payload.get("steps"), list)
    assert len(payload["steps"]) >= 1
