from __future__ import annotations

from services.files_service import _scan_files, _scan_rel_files


def test_scan_files_recursive_and_limit(tmp_path) -> None:
    root = tmp_path
    base = tmp_path / "data"
    base.mkdir()
    (base / "a.txt").write_text("a", encoding="utf-8")
    (base / "b.txt").write_text("b", encoding="utf-8")
    (base / "c.bin").write_text("c", encoding="utf-8")
    sub = base / "nested"
    sub.mkdir()
    (sub / "d.txt").write_text("d", encoding="utf-8")

    res = _scan_files(
        root_dir=str(root),
        base_dir=str(base),
        pattern="*.txt",
        recursive=True,
        limit=2,
    )
    assert len(res) == 2
    assert all(path.startswith("data/") for path in res)

    res_all = _scan_files(
        root_dir=str(root),
        base_dir=str(base),
        pattern="*.txt",
        recursive=True,
        limit=10,
    )
    assert "data/nested/d.txt" in res_all


def test_scan_rel_files_collects_all(tmp_path) -> None:
    root = tmp_path
    base = tmp_path / "data"
    base.mkdir()
    (base / "alpha.txt").write_text("a", encoding="utf-8")
    sub = base / "nested"
    sub.mkdir()
    (sub / "beta.txt").write_text("b", encoding="utf-8")

    res = _scan_rel_files(root_dir=str(root), base_dir=str(base))
    assert set(res) == {"data/alpha.txt", "data/nested/beta.txt"}
