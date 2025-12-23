from __future__ import annotations

from services import fpp_service


def test_playlist_filename_default() -> None:
    assert fpp_service._playlist_filename("show") == "show.json"
    assert fpp_service._playlist_filename("show.json") == "show.json"


def test_extract_playlist_names_from_list() -> None:
    body = [{"name": "A"}, "B", {"name": "A"}, {"name": ""}]
    assert fpp_service._extract_playlist_names(body) == ["A", "B"]


def test_extract_playlist_names_from_dict() -> None:
    body = {"playlists": [{"name": "Mix"}, "Chill"]}
    assert fpp_service._extract_playlist_names(body) == ["Chill", "Mix"]


def test_build_playlist_payload() -> None:
    payload = fpp_service._build_playlist_payload(
        name="NightShow",
        items=[{"type": "sequence", "sequenceName": "song.fseq"}],
        repeat=True,
        description="Test",
    )
    assert payload["name"] == "NightShow"
    assert payload["repeat"] == 1
    assert payload["playlist"] == payload["mainPlaylist"]
    assert payload["description"] == "Test"
