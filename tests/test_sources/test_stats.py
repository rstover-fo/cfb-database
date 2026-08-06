"""Tests for the stats source's per-game fan-out."""

from unittest.mock import MagicMock, patch


def _play_stats_rows(years, games_by_type, stats_by_game):
    """Materialize play_stats_resource rows with the dlt wrapper unwrapped.

    Returns (rows, requested_game_ids) so a test can assert on which games
    were actually paid for, not just on what came back.
    """
    from src.pipelines.sources.stats import play_stats_resource

    requested: list[int] = []

    def side_effect(client, path, params=None):
        if path == "/games":
            return list(games_by_type[params["seasonType"]])
        if path == "/plays/stats":
            requested.append(params["gameId"])
            return list(stats_by_game.get(params["gameId"], []))
        raise AssertionError(f"unexpected path {path}")

    with (
        patch("src.pipelines.sources.stats.get_client") as mock_get_client,
        patch("src.pipelines.sources.stats.make_request") as mock_make_request,
    ):
        mock_get_client.return_value = MagicMock()
        mock_make_request.side_effect = side_effect
        rows = list(play_stats_resource(years=years))

    return rows, requested


class TestPlayStatsSkipsUnplayedGames:
    """This resource spends one /plays/stats call PER GAME.

    From 2026-08-01 `get_current_season()` returned 2026 and the season was
    not final, so the daily load walked all 1,638 *scheduled* 2026 games every
    day. It got ~370 in before CFBD answered 429, which failed the whole
    `stats` extract package -- discarding player_returning's already-fetched
    payload -- and burst-blocked `ratings` and `game_stats` behind it. Three
    consecutive red daily loads.
    """

    def test_unplayed_games_are_never_requested(self):
        games = {
            "regular": [
                {"id": 1, "completed": True},
                {"id": 2, "completed": False},
                {"id": 3, "completed": False},
            ],
            "postseason": [],
        }
        rows, requested = _play_stats_rows([2026], games, {1: [{"stat": "a"}]})

        assert requested == [1], "an unplayed game has no play stats to return"
        assert rows == [{"stat": "a"}]

    def test_a_season_with_no_completed_games_costs_nothing(self):
        """The exact 2026 preseason shape: a full schedule, nothing played."""
        games = {
            "regular": [{"id": i, "completed": False} for i in range(1, 51)],
            "postseason": [],
        }
        _, requested = _play_stats_rows([2026], games, {})

        assert requested == []

    def test_a_missing_completed_flag_is_treated_as_unplayed(self):
        """Absent is not the same as True: paying for a call on a maybe is
        what this resource cannot afford at one call per game."""
        games = {"regular": [{"id": 1}], "postseason": []}
        _, requested = _play_stats_rows([2026], games, {})

        assert requested == []

    def test_completed_postseason_games_still_load(self):
        games = {
            "regular": [{"id": 1, "completed": True}],
            "postseason": [{"id": 9, "completed": True}],
        }
        _, requested = _play_stats_rows([2025], games, {})

        assert requested == [1, 9]

    def test_explicit_game_ids_are_not_filtered(self):
        """A caller naming game ids has already decided what to fetch -- the
        backfill path must not silently drop them."""
        from src.pipelines.sources.stats import play_stats_resource

        requested = []

        def side_effect(client, path, params=None):
            requested.append(params["gameId"])
            return []

        with (
            patch("src.pipelines.sources.stats.get_client") as mock_get_client,
            patch("src.pipelines.sources.stats.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = side_effect
            list(play_stats_resource(game_ids=[11, 22]))

        assert requested == [11, 22]
