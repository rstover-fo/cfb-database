"""Tests for the in-game win-probability backfill (Tier 3 analytics, Pillar D).

No DB, no network:
  - win_probability_by_game_resource: mocks the CFBD client/make_request and
    checks per-game call fan-out, game_id stamping, and the merge key.
  - scripts/backfill_ingame_wp.py's pure helpers (filter_unloaded,
    apply_budget, chunk_ids): plain data-in/data-out, no I/O.
"""

from unittest.mock import MagicMock, patch

from scripts.backfill_ingame_wp import apply_budget, chunk_ids, filter_unloaded


class TestWinProbabilityByGameResource:
    """src.pipelines.sources.metrics.win_probability_by_game_resource"""

    def test_one_api_call_per_game_id(self):
        from src.pipelines.sources.metrics import win_probability_by_game_resource

        with (
            patch("src.pipelines.sources.metrics.get_client") as mock_get_client,
            patch("src.pipelines.sources.metrics.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.return_value = []

            list(win_probability_by_game_resource([401012891, 401012892, 401012893]))

            assert mock_make_request.call_count == 3

    def test_calls_use_gameid_param_not_year(self):
        """The whole point of this resource: /metrics/wp is gameId-scoped."""
        from src.pipelines.sources.metrics import win_probability_by_game_resource

        with (
            patch("src.pipelines.sources.metrics.get_client") as mock_get_client,
            patch("src.pipelines.sources.metrics.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.return_value = []

            list(win_probability_by_game_resource([401012891]))

            args, kwargs = mock_make_request.call_args
            assert args[1] == "/metrics/wp"
            params = kwargs.get("params") or args[2]
            assert params == {"gameId": 401012891}
            assert "year" not in params

    def test_game_id_stamped_when_missing_from_response(self):
        from src.pipelines.sources.metrics import win_probability_by_game_resource

        mock_response = [
            {
                "playId": "4010128915404",
                "playText": "Alabama rush for 3 yards",
                "homeWinProbability": 0.62,
                "playNumber": 1,
            },
            {
                "playId": "4010128915405",
                "playText": "Georgia pass incomplete",
                "homeWinProbability": 0.60,
                "playNumber": 2,
            },
        ]

        with (
            patch("src.pipelines.sources.metrics.get_client") as mock_get_client,
            patch("src.pipelines.sources.metrics.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.return_value = mock_response

            results = list(win_probability_by_game_resource([401012891]))

        assert len(results) == 2
        assert all(row["gameId"] == 401012891 for row in results)

    def test_game_id_from_response_is_preserved_not_overwritten(self):
        from src.pipelines.sources.metrics import win_probability_by_game_resource

        mock_response = [{"gameId": 401012891, "playId": "p1", "homeWinProbability": 0.5}]

        with (
            patch("src.pipelines.sources.metrics.get_client") as mock_get_client,
            patch("src.pipelines.sources.metrics.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.return_value = mock_response

            results = list(win_probability_by_game_resource([401012891]))

        assert results[0]["gameId"] == 401012891

    def test_skips_games_that_400(self):
        """Some games (thin/incomplete data) 400 -- skip and continue, don't raise."""
        import httpx

        from src.pipelines.sources.metrics import win_probability_by_game_resource

        response_400 = httpx.Response(400, request=httpx.Request("GET", "https://x/metrics/wp"))
        error_400 = httpx.HTTPStatusError(
            "bad request", request=response_400.request, response=response_400
        )

        with (
            patch("src.pipelines.sources.metrics.get_client") as mock_get_client,
            patch("src.pipelines.sources.metrics.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = [
                error_400,
                [{"gameId": 2, "playId": "p1", "homeWinProbability": 0.5}],
            ]

            results = list(win_probability_by_game_resource([1, 2]))

        assert mock_make_request.call_count == 2
        assert len(results) == 1
        assert results[0]["gameId"] == 2

    def test_merge_key_is_play_id(self):
        from src.pipelines.sources.metrics import win_probability_by_game_resource

        resource = win_probability_by_game_resource([401012891])
        schema = resource.compute_table_schema()

        assert schema["write_disposition"] == "merge"
        assert schema["columns"]["play_id"]["primary_key"] is True

    def test_resource_name(self):
        from src.pipelines.sources.metrics import win_probability_by_game_resource

        resource = win_probability_by_game_resource([401012891])
        assert resource.name == "win_probability_by_game"


class TestFilterUnloaded:
    def test_removes_already_loaded_ids(self):
        assert filter_unloaded([1, 2, 3, 4], {2, 4}) == [1, 3]

    def test_preserves_order(self):
        assert filter_unloaded([5, 3, 1], {3}) == [5, 1]

    def test_empty_loaded_set_returns_all(self):
        assert filter_unloaded([1, 2, 3], set()) == [1, 2, 3]

    def test_everything_loaded_returns_empty(self):
        assert filter_unloaded([1, 2], {1, 2}) == []

    def test_empty_input_returns_empty(self):
        assert filter_unloaded([], {1, 2}) == []


class TestApplyBudget:
    def test_under_budget_selects_all_none_remaining(self):
        selected, remaining = apply_budget([1, 2, 3], max_calls=10)
        assert selected == [1, 2, 3]
        assert remaining == 0

    def test_over_budget_caps_and_reports_remaining(self):
        selected, remaining = apply_budget([1, 2, 3, 4, 5], max_calls=2)
        assert selected == [1, 2]
        assert remaining == 3

    def test_exact_budget_no_remainder(self):
        selected, remaining = apply_budget([1, 2, 3], max_calls=3)
        assert selected == [1, 2, 3]
        assert remaining == 0

    def test_zero_max_calls_selects_nothing(self):
        selected, remaining = apply_budget([1, 2, 3], max_calls=0)
        assert selected == []
        assert remaining == 3

    def test_negative_max_calls_selects_nothing(self):
        selected, remaining = apply_budget([1, 2, 3], max_calls=-5)
        assert selected == []
        assert remaining == 3

    def test_empty_input(self):
        selected, remaining = apply_budget([], max_calls=100)
        assert selected == []
        assert remaining == 0


class TestChunkIds:
    def test_partitions_evenly(self):
        assert chunk_ids([1, 2, 3, 4], chunk_size=2) == [[1, 2], [3, 4]]

    def test_partitions_with_remainder(self):
        assert chunk_ids([1, 2, 3, 4, 5], chunk_size=2) == [[1, 2], [3, 4], [5]]

    def test_chunk_size_larger_than_input_yields_one_chunk(self):
        assert chunk_ids([1, 2, 3], chunk_size=10) == [[1, 2, 3]]

    def test_empty_input_yields_no_chunks(self):
        assert chunk_ids([], chunk_size=5) == []

    def test_chunk_size_one(self):
        assert chunk_ids([1, 2, 3], chunk_size=1) == [[1], [2], [3]]

    def test_nonpositive_chunk_size_raises(self):
        import pytest

        with pytest.raises(ValueError):
            chunk_ids([1, 2, 3], chunk_size=0)


class TestResumeAndBudgetIntegration:
    """A crash-loses-at-most-a-chunk sanity check combining the three helpers,
    mirroring what run_backfill does end to end."""

    def test_full_pipeline_of_helpers(self):
        all_games = list(range(1, 21))  # 20 games
        loaded = {2, 4, 6, 8, 10}  # 5 already loaded

        pending = filter_unloaded(all_games, loaded)
        assert len(pending) == 15

        to_load, remaining_after_budget = apply_budget(pending, max_calls=7)
        assert len(to_load) == 7
        assert remaining_after_budget == 8

        chunks = chunk_ids(to_load, chunk_size=3)
        assert chunks == [[1, 3, 5], [7, 9, 11], [12]]
        assert sum(len(c) for c in chunks) == len(to_load)
