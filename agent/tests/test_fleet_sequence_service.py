from __future__ import annotations

import time
from dataclasses import dataclass

from fleet_sequence_service import FleetSequenceService
from pack_io import write_json


@dataclass(frozen=True)
class Peer:
    name: str
    base_url: str = "http://peer:8088"


def test_fleet_sequence_service_invokes_peers(tmp_path) -> None:
    data_dir = tmp_path
    seq_dir = data_dir / "sequences"
    seq_dir.mkdir(parents=True, exist_ok=True)

    seq_file = "sequence_test.json"
    write_json(
        str(seq_dir / seq_file),
        {
            "name": "t",
            "steps": [
                {
                    "type": "ddp",
                    "pattern": "solid",
                    "params": {"color": [255, 0, 0]},
                    "duration_s": 0.1,
                },
                {"type": "look", "look": {"id": 1, "name": "x"}, "duration_s": 0.1},
            ],
        },
    )

    peer = Peer(name="roofline1")
    calls: list[tuple[str, str, dict]] = []

    def local_invoke(action: str, params: dict) -> None:
        calls.append(("self", action, dict(params)))

    def peer_supported_actions(_peer: Peer, _timeout_s: float) -> set[str]:
        return {"start_ddp_pattern", "apply_look_spec"}

    def peer_invoke(_peer: Peer, action: str, params: dict, _timeout_s: float) -> dict:
        calls.append((_peer.name, action, dict(params)))
        return {"ok": True}

    svc = FleetSequenceService(
        data_dir=str(data_dir),
        peers={"roofline1": peer},
        local_invoke=local_invoke,
        peer_invoke=peer_invoke,
        peer_supported_actions=peer_supported_actions,
        default_timeout_s=0.5,
    )

    svc.start(file=seq_file, loop=False, include_self=True)
    for _ in range(50):
        if not svc.status().running:
            break
        time.sleep(0.05)

    assert svc.status().running is False
    assert any(c[0] == "roofline1" and c[1] == "start_ddp_pattern" for c in calls)
    assert any(c[0] == "roofline1" and c[1] == "apply_look_spec" for c in calls)

    # DDP step should not invent optional keys when not present in the sequence JSON.
    ddp_call = next(c for c in calls if c[1] == "start_ddp_pattern")
    assert "brightness" not in ddp_call[2]
    assert "fps" not in ddp_call[2]
