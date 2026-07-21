"""Tests for games and game stats source composition."""

from src.pipelines.sources.game_stats import game_stats_source
from src.pipelines.sources.games import games_source


def test_games_source_excludes_game_stats():
    """Game box scores must not load via games_source.

    game_team_stats/game_player_stats merges time out on Supabase at full-season
    size; they load exclusively via game_stats_source's week-by-week path.
    """
    source = games_source(years=[2024])

    assert set(source.resources.keys()) == {
        "games",
        "drives",
        "game_media",
        "game_weather",
        "records",
    }


def test_game_stats_source_owns_game_stats():
    """game_stats_source is the only loader for game box score tables."""
    source = game_stats_source(years=[2024])

    assert set(source.resources.keys()) == {"game_team_stats", "game_player_stats"}


def _drives_rows(years):
    """Materialize drives_resource rows with the dlt wrapper unwrapped."""
    from src.pipelines.sources.games import drives_resource

    return list(drives_resource(years))


def _fake_games_and_drives(games_by_year, drives_by_type):
    """Return a make_request side effect serving /games and /drives."""

    def side_effect(client, path, params=None):
        if path == "/games":
            return games_by_year[params["year"]]
        if path == "/drives":
            return drives_by_type[params["seasonType"]]
        raise AssertionError(f"unexpected path {path}")

    return side_effect


def test_drives_resource_drops_orphan_game_ids():
    """Drives whose gameId is absent from /games must not be yielded.

    CFBD's /drives can keep drives for cancelled/delisted games (e.g.
    2025's 401754543) that /games no longer lists; loading them violates
    fk_drives_game.
    """
    from unittest.mock import MagicMock, patch

    games = {2025: [{"id": 100}, {"id": 200}]}
    drives = {
        "regular": [
            {"id": "100-1", "gameId": 100},
            {"id": "100-2", "gameId": 100},
            {"id": "100-3", "gameId": 100},
            {"id": "999-1", "gameId": 999},  # orphan, under the 25% tripwire
        ],
        "postseason": [{"id": "200-1", "gameId": 200}],
    }

    with (
        patch("src.pipelines.sources.games.get_client") as mock_get_client,
        patch("src.pipelines.sources.games.make_request") as mock_make_request,
    ):
        mock_get_client.return_value = MagicMock()
        mock_make_request.side_effect = _fake_games_and_drives(games, drives)
        rows = _drives_rows([2025])

    assert [r["id"] for r in rows] == ["100-1", "100-2", "100-3", "200-1"]
    assert all(r["season"] == 2025 for r in rows)


def test_drives_resource_raises_on_mass_drop():
    """Dropping >25% of a year's drives means breakage, not stray ids."""
    from unittest.mock import MagicMock, patch

    import pytest
    from dlt.extract.exceptions import ResourceExtractionError

    games = {2025: [{"id": 100}]}
    drives = {
        "regular": [
            {"id": "a", "gameId": 100},
            {"id": "b", "gameId": 901},
            {"id": "c", "gameId": 902},
        ],
        "postseason": [],
    }

    with (
        patch("src.pipelines.sources.games.get_client") as mock_get_client,
        patch("src.pipelines.sources.games.make_request") as mock_make_request,
    ):
        mock_get_client.return_value = MagicMock()
        mock_make_request.side_effect = _fake_games_and_drives(games, drives)
        # dlt wraps the resource's ValueError in ResourceExtractionError
        with pytest.raises(ResourceExtractionError, match="Orphan filter"):
            _drives_rows([2025])


def test_drives_filter_reuses_shared_games_cache():
    """The orphan filter must use the exact /games response the games
    resource loaded -- never a second fetch. Two CFBD /games calls seconds
    apart can disagree (CDN cache variance, deploy run 29845763420), which
    re-admits orphans the games merge never saw."""
    from unittest.mock import MagicMock, patch

    from src.pipelines.sources.games import drives_resource, games_resource

    first_games = [{"id": 100}]
    diverged_games = [{"id": 100}, {"id": 999}]  # what a second fetch would say
    drives = {
        "regular": [
            {"id": "100-1", "gameId": 100},
            {"id": "100-2", "gameId": 100},
            {"id": "100-3", "gameId": 100},
            {"id": "999-1", "gameId": 999},  # orphan per the FIRST response
        ],
        "postseason": [],
    }
    games_responses = iter([first_games, diverged_games])

    def side_effect(client, path, params=None):
        if path == "/games":
            return next(games_responses)
        if path == "/drives":
            return drives[params["seasonType"]]
        raise AssertionError(f"unexpected path {path}")

    cache: dict[int, list[dict]] = {}
    with (
        patch("src.pipelines.sources.games.get_client") as mock_get_client,
        patch("src.pipelines.sources.games.make_request") as mock_make_request,
    ):
        mock_get_client.return_value = MagicMock()
        mock_make_request.side_effect = side_effect

        games_rows = list(games_resource([2025], cache))
        drive_rows = list(drives_resource([2025], cache))

    assert [g["id"] for g in games_rows] == [100]
    # 999-1 dropped: the filter saw first_games via the cache, not the
    # diverged second response (which was never fetched)
    assert [d["id"] for d in drive_rows] == ["100-1", "100-2", "100-3"]
    games_calls = [c for c in mock_make_request.call_args_list if c.args[1] == "/games"]
    assert len(games_calls) == 1
