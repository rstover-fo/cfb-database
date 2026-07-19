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
