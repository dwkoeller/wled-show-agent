from __future__ import annotations

import pytest

from show_config import ShowConfig, load_show_config, write_show_config


def test_show_config_round_trip(tmp_path) -> None:
    cfg = ShowConfig(name="test-show")
    out = write_show_config(
        data_dir=str(tmp_path), rel_path="show/config.json", config=cfg
    )
    loaded = load_show_config(data_dir=str(tmp_path), rel_path="show/config.json")
    assert loaded.name == "test-show"
    assert out.endswith("show/config.json")


def test_show_config_blocks_path_traversal(tmp_path) -> None:
    cfg = ShowConfig(name="x")
    write_show_config(data_dir=str(tmp_path), rel_path="show/config.json", config=cfg)

    with pytest.raises(ValueError):
        load_show_config(data_dir=str(tmp_path), rel_path="../config.json")
