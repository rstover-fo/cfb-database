"""Tests for the stats source's per-game fan-out."""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import httpx
import pytest

FIXTURES = Path(__file__).parent.parent / "fixtures" / "cfbd_2026"


def _load_fixture(name):
    with open(FIXTURES / name) as f:
        return json.load(f)


def _http_error(status_code: int) -> httpx.HTTPStatusError:
    request = httpx.Request("GET", "https://api.collegefootballdata.com/stats/game/advanced")
    response = httpx.Response(status_code, request=request)
    return httpx.HTTPStatusError("error", request=request, response=response)


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


class TestPlayStatsGameIdsMissTracking:
    """R1/F6: play_stats_resource's game_ids branch records a suppressed 400
    in `misses`, but an empty 200 is deliberately NOT a miss -- zero
    player-stat associations is a legitimate outcome for early-era or
    lower-division games (pinned here to lock the decision)."""

    def test_400_response_appends_to_misses(self):
        from src.pipelines.sources.stats import play_stats_resource

        with (
            patch("src.pipelines.sources.stats.get_client") as mock_get_client,
            patch("src.pipelines.sources.stats.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = [_http_error(400), [{"gameId": 2}]]

            misses: list[int] = []
            results = list(play_stats_resource(game_ids=[1, 2], misses=misses))

            assert len(results) == 1
            assert misses == [1]

    def test_empty_200_is_not_a_miss(self):
        from src.pipelines.sources.stats import play_stats_resource

        with (
            patch("src.pipelines.sources.stats.get_client") as mock_get_client,
            patch("src.pipelines.sources.stats.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.return_value = []

            misses: list[int] = []
            results = list(play_stats_resource(game_ids=[1], misses=misses))

            assert results == []
            assert misses == []

    def test_misses_none_is_safe(self):
        from src.pipelines.sources.stats import play_stats_resource

        with (
            patch("src.pipelines.sources.stats.get_client") as mock_get_client,
            patch("src.pipelines.sources.stats.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = [_http_error(400), []]

            results = list(play_stats_resource(game_ids=[1, 2]))

            assert results == []


class TestStatsSourceResourceComposition:
    def test_default_resources_include_new_additions(self):
        from src.pipelines.sources.stats import stats_source

        source = stats_source(years=[2024])

        names = set(source.resources.keys())
        assert {"player_success_season", "player_success_game", "game_advanced"} <= names

    def test_advanced_game_stats_is_not_in_the_default_return(self):
        """R1: /game/box/advanced dropped its `year` param and always 400s
        year-scoped now -- it must not be part of a normal stats load."""
        from src.pipelines.sources.stats import stats_source

        source = stats_source(years=[2024])

        assert "advanced_game_stats" not in set(source.resources.keys())

    def test_advanced_game_stats_resource_is_still_importable(self):
        """Kept for the historical-refresh campaign (game-id-driven)."""
        from src.pipelines.sources.stats import advanced_game_stats_resource

        assert callable(advanced_game_stats_resource)


class TestAdvancedGameStatsResourceGameIdsMode:
    """R1 rework: advanced_game_stats_resource takes explicit game_ids and
    calls /game/box/advanced?id=<gameId> per id, mirroring
    play_stats_resource's explicit-ids branch."""

    @pytest.fixture(autouse=True)
    def _reset_advanced_box_health(self):
        """The health window is process-wide, so one test must not pollute the next."""
        from src.pipelines.sources.stats import reset_advanced_box_health

        reset_advanced_box_health()
        yield
        reset_advanced_box_health()

    def test_one_call_per_game_id_with_id_param(self):
        from src.pipelines.sources.stats import advanced_game_stats_resource

        with (
            patch("src.pipelines.sources.stats.get_client") as mock_get_client,
            patch("src.pipelines.sources.stats.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.return_value = []

            list(advanced_game_stats_resource(game_ids=[401628319, 401628320]))

            assert mock_make_request.call_count == 2
            calls = mock_make_request.call_args_list
            assert calls[0].args[1] == "/game/box/advanced"
            assert calls[0].kwargs["params"] == {"id": 401628319}
            assert calls[1].kwargs["params"] == {"id": 401628320}

    def test_400_response_skips_game_and_continues(self):
        from src.pipelines.sources.stats import advanced_game_stats_resource

        with (
            patch("src.pipelines.sources.stats.get_client") as mock_get_client,
            patch("src.pipelines.sources.stats.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = [_http_error(400), [{"gameId": 2, "team": "X"}]]

            results = list(advanced_game_stats_resource(game_ids=[1, 2]))

            assert len(results) == 1
            assert results[0]["gameId"] == 2

    def test_400_response_appends_to_misses(self):
        """R1/F6: a suppressed 400 must be recorded in `misses` when the
        caller passes a list, not just silently skipped."""
        from src.pipelines.sources.stats import advanced_game_stats_resource

        with (
            patch("src.pipelines.sources.stats.get_client") as mock_get_client,
            patch("src.pipelines.sources.stats.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = [_http_error(400), [{"gameId": 2, "team": "X"}]]

            misses: list[int] = []
            results = list(advanced_game_stats_resource(game_ids=[1, 2], misses=misses))

            assert len(results) == 1
            assert misses == [1]

    def test_empty_200_appends_to_misses(self):
        """R1/F6: unlike play_stats, an empty response here IS a miss --
        every game with a completed box score has advanced stats."""
        from src.pipelines.sources.stats import advanced_game_stats_resource

        with (
            patch("src.pipelines.sources.stats.get_client") as mock_get_client,
            patch("src.pipelines.sources.stats.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.return_value = []

            misses: list[int] = []
            results = list(advanced_game_stats_resource(game_ids=[1], misses=misses))

            assert results == []
            assert misses == [1]

    def test_success_does_not_append_to_misses(self):
        from src.pipelines.sources.stats import advanced_game_stats_resource

        with (
            patch("src.pipelines.sources.stats.get_client") as mock_get_client,
            patch("src.pipelines.sources.stats.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.return_value = [{"gameId": 1, "team": "X"}]

            misses: list[int] = []
            results = list(advanced_game_stats_resource(game_ids=[1], misses=misses))

            assert len(results) == 1
            assert misses == []

    def test_misses_none_is_safe(self):
        """Default misses=None (the plain stats-load style call) must not
        raise on either a 400 or an empty response."""
        from src.pipelines.sources.stats import advanced_game_stats_resource

        with (
            patch("src.pipelines.sources.stats.get_client") as mock_get_client,
            patch("src.pipelines.sources.stats.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = [_http_error(400), []]

            results = list(advanced_game_stats_resource(game_ids=[1, 2]))

            assert results == []

    def test_500_after_retries_appends_to_misses_and_continues(self):
        from src.pipelines.sources.stats import advanced_game_stats_resource

        with (
            patch("src.pipelines.sources.stats.get_client") as mock_get_client,
            patch("src.pipelines.sources.stats.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = [_http_error(500), [{"gameId": 2, "team": "X"}]]

            misses: list[int] = []
            results = list(advanced_game_stats_resource(game_ids=[1, 2], misses=misses))

            assert results == [{"gameId": 2, "team": "X"}]
            assert misses == [1]

    def test_500_without_misses_reraises(self):
        """dlt wraps exceptions raised while iterating a resource, so the
        re-raised HTTPStatusError surfaces as the cause of
        ResourceExtractionError."""
        from dlt.extract.exceptions import ResourceExtractionError

        from src.pipelines.sources.stats import advanced_game_stats_resource

        with (
            patch("src.pipelines.sources.stats.get_client") as mock_get_client,
            patch("src.pipelines.sources.stats.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = [_http_error(500), [{"gameId": 2, "team": "X"}]]

            with pytest.raises(ResourceExtractionError) as exc_info:
                list(advanced_game_stats_resource(game_ids=[1, 2]))

            assert isinstance(exc_info.value.__cause__, httpx.HTTPStatusError)
            assert exc_info.value.__cause__.response.status_code == 500
            assert mock_make_request.call_count == 1

    def test_misses_without_report_suppresses_5xx_without_probe(self):
        """misses-only callers (no report) never probe or trip health."""
        from src.pipelines.sources.stats import advanced_game_stats_resource

        with (
            patch("src.pipelines.sources.stats.get_client") as mock_get_client,
            patch("src.pipelines.sources.stats.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = [_http_error(500)] * 6 + [[{"gameId": 99, "team": "X"}]]

            misses: list[int] = []
            results = list(
                advanced_game_stats_resource(game_ids=[1, 2, 3, 4, 5, 6, 7], misses=misses)
            )

            assert results == [{"gameId": 99, "team": "X"}]
            assert misses == [1, 2, 3, 4, 5, 6]
            assert mock_make_request.call_count == 7

    def test_merge_disposition_and_primary_key_unchanged(self):
        from src.pipelines.sources.stats import advanced_game_stats_resource

        with patch("src.pipelines.sources.stats.get_client") as mock_get_client:
            mock_get_client.return_value = MagicMock()

            resource = advanced_game_stats_resource(game_ids=[1])

            assert resource.write_disposition == "merge"
            schema = resource.compute_table_schema()
            pk_columns = {name for name, col in schema["columns"].items() if col.get("primary_key")}
            assert pk_columns == {"game_id", "team"}


class TestAdvancedBoxHealth:
    def test_trip_requires_five_server_errors_and_zero_answered(self):
        from src.pipelines.sources.stats import AdvancedBoxHealth

        health = AdvancedBoxHealth(window=10)
        for gid in range(1, 5):
            health.record_server_error(gid)
        assert health.should_probe() is False
        health.record_server_error(5)
        assert health.should_probe() is True

    def test_answered_game_blocks_trip_until_it_slides_out(self):
        from src.pipelines.sources.stats import AdvancedBoxHealth

        health = AdvancedBoxHealth(window=5)
        for gid in range(1, 5):
            health.record_server_error(gid)
        health.record_answered(99, has_data=True)
        assert health.should_probe() is False
        # The answered entry stays in the 5-slot window for the next 4
        # errors; the 5th slides it out and the window is all-error again.
        for gid in range(5, 9):
            health.record_server_error(gid)
            assert health.should_probe() is False
        health.record_server_error(9)
        assert health.should_probe() is True

    def test_clear_window_after_probe(self):
        from src.pipelines.sources.stats import AdvancedBoxHealth

        health = AdvancedBoxHealth(window=10)
        for gid in range(1, 6):
            health.record_server_error(gid)
        assert health.should_probe() is True
        health.clear_window()
        assert health.should_probe() is False

    def test_empty_200_and_400_count_as_answered(self):
        from src.pipelines.sources.stats import AdvancedBoxHealth

        health = AdvancedBoxHealth(window=10)
        for gid in range(1, 5):
            health.record_server_error(gid)
        health.record_answered(50, has_data=False)
        health.record_server_error(5)
        assert health.should_probe() is False


class TestAdvancedGameStatsHealthAndProbe:
    @pytest.fixture(autouse=True)
    def _reset_advanced_box_health(self):
        from src.pipelines.sources.stats import reset_advanced_box_health

        reset_advanced_box_health()
        yield
        reset_advanced_box_health()

    def test_incident_replay_no_probe_all_eleven_attempted(self):
        from src.pipelines.sources.stats import (
            AdvancedBoxBatchReport,
            advanced_game_stats_resource,
        )

        game_ids = (
            [401837096, 401837097]
            + [401836219, 401833987, 401833986, 401833985, 401762521]
            + [401837098, 401837099, 401837100, 401837101]
        )
        side_effect = (
            [_http_error(500), _http_error(500)]
            + [[{"gameId": gid, "team": "A"}] for gid in game_ids[2:7]]
            + [_http_error(500)] * 4
        )

        with (
            patch("src.pipelines.sources.stats.get_client") as mock_get_client,
            patch("src.pipelines.sources.stats.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = side_effect

            misses: list[int] = []
            report = AdvancedBoxBatchReport()
            results = list(
                advanced_game_stats_resource(game_ids=game_ids, misses=misses, report=report)
            )

        assert report.outage is None
        assert report.probe_game_id is None
        assert len(report.attempted) == 11
        assert len(results) == 5
        six_5xx = [401837096, 401837097, 401837098, 401837099, 401837100, 401837101]
        assert misses == six_5xx
        assert report.server_error_game_ids == six_5xx
        assert mock_make_request.call_count == 11

    def test_bad_data_cluster_probes_fallback_then_continues(self):
        from src.pipelines.sources.stats import (
            ADVANCED_BOX_PROBE_FALLBACK_GAME_ID,
            AdvancedBoxBatchReport,
            advanced_game_stats_resource,
        )

        game_ids = list(range(101, 108))
        with (
            patch("src.pipelines.sources.stats.get_client") as mock_get_client,
            patch("src.pipelines.sources.stats.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            probe_response = [{"gameId": ADVANCED_BOX_PROBE_FALLBACK_GAME_ID, "team": "P"}]
            mock_make_request.side_effect = (
                [_http_error(500)] * 5
                + probe_response
                + [[{"gameId": gid, "team": "X"}] for gid in game_ids[5:]]
            )

            misses: list[int] = []
            report = AdvancedBoxBatchReport(fallback_probe_game_id=None)
            results = list(
                advanced_game_stats_resource(game_ids=game_ids, misses=misses, report=report)
            )

        assert report.outage is None
        assert report.probe_game_id == ADVANCED_BOX_PROBE_FALLBACK_GAME_ID
        assert len(report.attempted) == len(game_ids)
        assert len(results) == 2
        assert misses == game_ids[:5]
        assert mock_make_request.call_count == len(game_ids) + 1

    def test_outage_stops_gracefully_after_probe_5xx(self):
        from src.pipelines.sources.stats import (
            AdvancedBoxBatchReport,
            advanced_game_stats_resource,
        )

        game_ids = [1] + list(range(2, 12))
        with (
            patch("src.pipelines.sources.stats.get_client") as mock_get_client,
            patch("src.pipelines.sources.stats.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = (
                [[{"gameId": 1, "team": "A"}]] + [_http_error(500)] * 10 + [_http_error(503)]
            )

            misses: list[int] = []
            report = AdvancedBoxBatchReport(fallback_probe_game_id=1)
            results = list(
                advanced_game_stats_resource(game_ids=game_ids, misses=misses, report=report)
            )

        assert report.outage is not None
        assert "probe game 1" in report.outage
        assert report.attempted == list(range(1, 12))
        assert results == [{"gameId": 1, "team": "A"}]
        assert misses == list(range(2, 12))

    def test_probe_uses_last_in_run_success_before_fallback(self):
        from src.pipelines.sources.stats import (
            AdvancedBoxBatchReport,
            advanced_game_stats_resource,
        )

        game_ids = [7] + list(range(8, 18))
        with (
            patch("src.pipelines.sources.stats.get_client") as mock_get_client,
            patch("src.pipelines.sources.stats.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = (
                [[{"gameId": 7, "team": "W"}]]
                + [_http_error(500)] * 10
                + [[{"gameId": 7, "team": "W"}]]
            )
            report = AdvancedBoxBatchReport(fallback_probe_game_id=50)
            list(advanced_game_stats_resource(game_ids=game_ids, misses=[], report=report))

        assert report.probe_game_id == 7

    def test_probe_uses_fallback_when_no_in_run_success(self):
        from src.pipelines.sources.stats import (
            AdvancedBoxBatchReport,
            advanced_game_stats_resource,
            reset_advanced_box_health,
        )

        reset_advanced_box_health()
        with (
            patch("src.pipelines.sources.stats.get_client") as mock_get_client,
            patch("src.pipelines.sources.stats.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = [_http_error(500)] * 5 + [[{"gameId": 50, "team": "P"}]]
            report = AdvancedBoxBatchReport(fallback_probe_game_id=50)
            list(advanced_game_stats_resource(game_ids=list(range(1, 6)), misses=[], report=report))

        assert report.probe_game_id == 50

    def test_probe_uses_module_constant_when_no_other_candidate(self):
        from src.pipelines.sources.stats import (
            ADVANCED_BOX_PROBE_FALLBACK_GAME_ID,
            AdvancedBoxBatchReport,
            advanced_game_stats_resource,
            reset_advanced_box_health,
        )

        reset_advanced_box_health()
        with (
            patch("src.pipelines.sources.stats.get_client") as mock_get_client,
            patch("src.pipelines.sources.stats.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = [_http_error(500)] * 5 + [
                [{"gameId": ADVANCED_BOX_PROBE_FALLBACK_GAME_ID, "team": "P"}]
            ]
            report = AdvancedBoxBatchReport(fallback_probe_game_id=None)
            list(advanced_game_stats_resource(game_ids=list(range(1, 6)), misses=[], report=report))

        assert report.probe_game_id == ADVANCED_BOX_PROBE_FALLBACK_GAME_ID

    @pytest.mark.parametrize("auth_status", [401, 403])
    def test_probe_auth_rejection_is_a_failure_not_healthy(self, auth_status):
        """PR #156 review (Codex): a 401/403 on the probe means CFBD rejected
        our credentials -- no later call can be trusted -- so it must fail the
        run like a 5xx, never count as 'the server answered'."""
        from src.pipelines.sources.stats import (
            AdvancedBoxBatchReport,
            advanced_game_stats_resource,
        )

        with (
            patch("src.pipelines.sources.stats.get_client") as mock_get_client,
            patch("src.pipelines.sources.stats.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = [_http_error(500)] * 5 + [
                _http_error(auth_status),
                AssertionError("draining must stop after the failed probe"),
            ]

            misses: list[int] = []
            report = AdvancedBoxBatchReport(fallback_probe_game_id=999)
            results = list(
                advanced_game_stats_resource(
                    game_ids=list(range(1, 8)), misses=misses, report=report
                )
            )

        assert results == []
        assert report.outage is not None
        assert str(auth_status) in report.outage
        assert report.budget_stop is None
        # The five 5xx games were attempted and recorded before the stop.
        assert report.attempted == [1, 2, 3, 4, 5]
        assert misses == [1, 2, 3, 4, 5]

    def test_budget_exhaustion_is_a_pacing_stop_not_an_outage(self):
        """PR #156 review (PR Lens): RequestBudgetExhausted is the LOCAL
        monthly budget guard, not a CFBD failure -- it must set budget_stop
        (caller exits 0) instead of outage (caller exits 1), while still
        stopping gracefully so yielded rows load and the prefix ledgers."""
        from src.pipelines.sources.base import RequestBudgetExhausted
        from src.pipelines.sources.stats import (
            AdvancedBoxBatchReport,
            advanced_game_stats_resource,
        )

        with (
            patch("src.pipelines.sources.stats.get_client") as mock_get_client,
            patch("src.pipelines.sources.stats.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = [
                [{"gameId": 1, "team": "A"}],
                RequestBudgetExhausted("API budget exhausted"),
                AssertionError("game 3 must never be requested"),
            ]

            misses: list[int] = []
            report = AdvancedBoxBatchReport()
            results = list(
                advanced_game_stats_resource(game_ids=[1, 2, 3], misses=misses, report=report)
            )

        assert results == [{"gameId": 1, "team": "A"}]
        assert report.budget_stop is not None
        assert "game 2" in report.budget_stop
        assert report.outage is None
        # Game 2's call was refused before it was spent: not attempted, not
        # a miss, so it stays in the backlog.
        assert report.attempted == [1]
        assert misses == []

    def test_budget_exhaustion_without_report_still_raises(self):
        from dlt.extract.exceptions import ResourceExtractionError

        from src.pipelines.sources.base import RequestBudgetExhausted
        from src.pipelines.sources.stats import advanced_game_stats_resource

        with (
            patch("src.pipelines.sources.stats.get_client") as mock_get_client,
            patch("src.pipelines.sources.stats.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = RequestBudgetExhausted("API budget exhausted")

            with pytest.raises(ResourceExtractionError) as exc_info:
                list(advanced_game_stats_resource(game_ids=[1], misses=[]))

            assert isinstance(exc_info.value.__cause__, RequestBudgetExhausted)

    def test_probe_400_counts_as_healthy(self):
        from src.pipelines.sources.stats import (
            AdvancedBoxBatchReport,
            advanced_game_stats_resource,
        )

        with (
            patch("src.pipelines.sources.stats.get_client") as mock_get_client,
            patch("src.pipelines.sources.stats.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = [_http_error(500)] * 5 + [_http_error(400)]

            misses: list[int] = []
            report = AdvancedBoxBatchReport(fallback_probe_game_id=999)
            list(
                advanced_game_stats_resource(
                    game_ids=list(range(1, 6)), misses=misses, report=report
                )
            )

        assert report.outage is None

    def test_transport_error_in_report_mode_stops_gracefully(self):
        """Review finding 2026-10-19: a timeout-shaped outage (transport
        error after the client's retries) must not escape the generator --
        a raise would abort the dlt load and lose the batch's rows, the
        original run-39253607809 bug via a different exception type."""
        from src.pipelines.sources.stats import (
            AdvancedBoxBatchReport,
            advanced_game_stats_resource,
        )

        with (
            patch("src.pipelines.sources.stats.get_client") as mock_get_client,
            patch("src.pipelines.sources.stats.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = [
                [{"gameId": 1, "team": "A"}],
                httpx.ConnectError("boom"),
                AssertionError("game 3 must never be requested"),
            ]

            misses: list[int] = []
            report = AdvancedBoxBatchReport()
            results = list(
                advanced_game_stats_resource(game_ids=[1, 2, 3], misses=misses, report=report)
            )

        assert results == [{"gameId": 1, "team": "A"}]
        assert report.outage is not None
        assert "game 2" in report.outage
        # The failing game's outcome is unknown: not attempted, not a miss,
        # so it stays in the backlog with no ledger row.
        assert report.attempted == [1]
        assert misses == []

    def test_unhandled_4xx_in_report_mode_stops_gracefully(self):
        from src.pipelines.sources.stats import (
            AdvancedBoxBatchReport,
            advanced_game_stats_resource,
        )

        with (
            patch("src.pipelines.sources.stats.get_client") as mock_get_client,
            patch("src.pipelines.sources.stats.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = [
                [{"gameId": 1, "team": "A"}],
                _http_error(401),
            ]

            misses: list[int] = []
            report = AdvancedBoxBatchReport()
            results = list(
                advanced_game_stats_resource(game_ids=[1, 2], misses=misses, report=report)
            )

        assert results == [{"gameId": 1, "team": "A"}]
        assert report.outage is not None
        assert "401" in report.outage
        assert report.attempted == [1]
        assert misses == []

    def test_transport_error_without_report_still_raises(self):
        """misses-only and plain callers keep the raising contract -- the
        graceful stop is exclusively the backfill report path's."""
        from dlt.extract.exceptions import ResourceExtractionError

        from src.pipelines.sources.stats import advanced_game_stats_resource

        with (
            patch("src.pipelines.sources.stats.get_client") as mock_get_client,
            patch("src.pipelines.sources.stats.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = httpx.ConnectError("boom")

            with pytest.raises(ResourceExtractionError) as exc_info:
                list(advanced_game_stats_resource(game_ids=[1], misses=[]))

            assert isinstance(exc_info.value.__cause__, httpx.ConnectError)

    def test_per_game_timeout_is_a_miss_and_draining_continues(self):
        """Run 39637616538 (Historical Refresh #54): a per-game ReadTimeout
        after the client's retries must be treated like a persistent 5xx --
        a no_data miss for that game -- not crash the extract."""
        from src.pipelines.sources.stats import (
            AdvancedBoxBatchReport,
            advanced_game_stats_resource,
        )

        with (
            patch("src.pipelines.sources.stats.get_client") as mock_get_client,
            patch("src.pipelines.sources.stats.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = [
                [{"gameId": 1, "team": "A"}],
                httpx.ReadTimeout("timed out"),
                [{"gameId": 3, "team": "B"}],
            ]

            misses: list[int] = []
            report = AdvancedBoxBatchReport()
            results = list(
                advanced_game_stats_resource(game_ids=[1, 2, 3], misses=misses, report=report)
            )

        assert [r["gameId"] for r in results] == [1, 3]
        assert misses == [2]
        assert report.server_error_game_ids == [2]
        assert report.attempted == [1, 2, 3]
        assert report.outage is None

    def test_mass_timeouts_trip_outage_and_stop_gracefully(self):
        """A widespread run of timeouts must look like an outage: the window
        fills with server errors, the probe times out too, and the run stops
        cleanly (rows yielded so far kept) instead of crashing."""
        from src.pipelines.sources.stats import (
            AdvancedBoxBatchReport,
            advanced_game_stats_resource,
        )

        with (
            patch("src.pipelines.sources.stats.get_client") as mock_get_client,
            patch("src.pipelines.sources.stats.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = [httpx.ReadTimeout("timed out")] * 5 + [
                httpx.ReadTimeout("probe timed out"),
                AssertionError("draining must stop after the failed probe"),
            ]

            misses: list[int] = []
            report = AdvancedBoxBatchReport(fallback_probe_game_id=999)
            results = list(
                advanced_game_stats_resource(
                    game_ids=list(range(1, 8)), misses=misses, report=report
                )
            )

        assert results == []
        assert report.outage is not None
        assert "ReadTimeout" in report.outage
        assert report.attempted == [1, 2, 3, 4, 5]
        assert misses == [1, 2, 3, 4, 5]
        assert report.server_error_game_ids == [1, 2, 3, 4, 5]

    def test_timeout_without_misses_reraises(self):
        from dlt.extract.exceptions import ResourceExtractionError

        from src.pipelines.sources.stats import advanced_game_stats_resource

        with (
            patch("src.pipelines.sources.stats.get_client") as mock_get_client,
            patch("src.pipelines.sources.stats.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = httpx.ReadTimeout("timed out")

            with pytest.raises(ResourceExtractionError) as exc_info:
                list(advanced_game_stats_resource(game_ids=[1]))

            assert isinstance(exc_info.value.__cause__, httpx.ReadTimeout)

    def test_cross_batch_window_shared_across_invocations(self):
        from src.pipelines.sources.stats import (
            AdvancedBoxBatchReport,
            advanced_game_stats_resource,
        )

        with (
            patch("src.pipelines.sources.stats.get_client") as mock_get_client,
            patch("src.pipelines.sources.stats.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = [_http_error(500)] * 4

            misses: list[int] = []
            report1 = AdvancedBoxBatchReport()
            list(advanced_game_stats_resource(game_ids=[1, 2, 3, 4], misses=misses, report=report1))
            assert report1.probe_game_id is None

            mock_make_request.side_effect = [_http_error(500)] + [[{"gameId": 99, "team": "P"}]]
            report2 = AdvancedBoxBatchReport(fallback_probe_game_id=99)
            list(advanced_game_stats_resource(game_ids=[5], misses=misses, report=report2))
            assert report2.probe_game_id == 99
            assert report2.outage is None


class TestPlayerSuccessSeasonResource:
    def test_era_skip_on_400(self):
        from src.pipelines.sources.stats import player_success_season_resource

        with (
            patch("src.pipelines.sources.stats.get_client") as mock_get_client,
            patch("src.pipelines.sources.stats.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = _http_error(400)

            assert list(player_success_season_resource(years=[2004])) == []

    def test_yields_rows_with_top_level_join_fields(self):
        """id/season/team/position must arrive as top-level columns -- the
        player-grain join spine -- not buried in a nested structure."""
        from src.pipelines.sources.stats import player_success_season_resource

        fixture = _load_fixture("stats_player_success.json")

        with (
            patch("src.pipelines.sources.stats.get_client") as mock_get_client,
            patch("src.pipelines.sources.stats.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.return_value = fixture

            results = list(player_success_season_resource(years=[2024]))

            assert len(results) == len(fixture)
            for row in results:
                assert isinstance(row["id"], str)
                assert isinstance(row["season"], int)
                assert "team" in row
                assert "position" in row
            assert mock_make_request.call_args.args[1] == "/stats/player/success"
            assert mock_make_request.call_args.kwargs["params"] == {"year": 2024}

    def test_merge_disposition_and_primary_key(self):
        from src.pipelines.sources.stats import player_success_season_resource

        with patch("src.pipelines.sources.stats.get_client") as mock_get_client:
            mock_get_client.return_value = MagicMock()

            resource = player_success_season_resource(years=[2024])

            assert resource.write_disposition == "merge"
            schema = resource.compute_table_schema()
            pk_columns = {name for name, col in schema["columns"].items() if col.get("primary_key")}
            assert pk_columns == {"season", "id", "team"}


class TestPlayerSuccessGameResource:
    def test_iterates_regular_1_16_and_postseason_1_4(self):
        """The exact param sequence: year-alone 400s this endpoint, so it
        must walk weeks -- regular 1-16, postseason 1-4."""
        from src.pipelines.sources.stats import player_success_game_resource

        with (
            patch("src.pipelines.sources.stats.get_client") as mock_get_client,
            patch("src.pipelines.sources.stats.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.return_value = []

            list(player_success_game_resource(years=[2024]))

            calls = [c.kwargs["params"] for c in mock_make_request.call_args_list]
            expected = [
                {"year": 2024, "seasonType": "regular", "week": w} for w in range(1, 17)
            ] + [{"year": 2024, "seasonType": "postseason", "week": w} for w in range(1, 5)]
            assert calls == expected

    def test_empty_week_is_skipped_silently(self):
        from src.pipelines.sources.stats import player_success_game_resource

        with (
            patch("src.pipelines.sources.stats.get_client") as mock_get_client,
            patch("src.pipelines.sources.stats.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.return_value = []

            results = list(player_success_game_resource(years=[2024]))

            assert results == []

    def test_400_week_is_skipped_and_continues(self):
        """20 total (year, seasonType, week) calls -- the first 400s, one
        later week returns real data, the rest are empty. The 400 must not
        abort the resource."""
        from src.pipelines.sources.stats import player_success_game_resource

        fixture = _load_fixture("stats_player_success_game.json")

        responses = [_http_error(400)] + [[]] * 4 + [fixture] + [[]] * 14
        assert len(responses) == 20  # 16 regular + 4 postseason

        with (
            patch("src.pipelines.sources.stats.get_client") as mock_get_client,
            patch("src.pipelines.sources.stats.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = responses

            results = list(player_success_game_resource(years=[2024]))

            assert len(results) == len(fixture)

    def test_yields_rows_with_top_level_join_fields(self):
        from src.pipelines.sources.stats import player_success_game_resource

        fixture = _load_fixture("stats_player_success_game.json")

        with (
            patch("src.pipelines.sources.stats.get_client") as mock_get_client,
            patch("src.pipelines.sources.stats.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = [fixture] + [[]] * 19

            results = list(player_success_game_resource(years=[2024]))

            assert len(results) == len(fixture)
            for row in results:
                assert "id" in row
                assert "season" in row
                assert "team" in row
                assert "position" in row
                assert "gameId" in row

    def test_merge_disposition_and_primary_key(self):
        from src.pipelines.sources.stats import player_success_game_resource

        with patch("src.pipelines.sources.stats.get_client") as mock_get_client:
            mock_get_client.return_value = MagicMock()

            resource = player_success_game_resource(years=[2024])

            assert resource.write_disposition == "merge"
            schema = resource.compute_table_schema()
            pk_columns = {name for name, col in schema["columns"].items() if col.get("primary_key")}
            assert pk_columns == {"game_id", "id"}


class TestGameAdvancedResource:
    def test_iterates_regular_and_postseason(self):
        from src.pipelines.sources.stats import game_advanced_resource

        with (
            patch("src.pipelines.sources.stats.get_client") as mock_get_client,
            patch("src.pipelines.sources.stats.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.return_value = []

            list(game_advanced_resource(years=[2024]))

            calls = [c.kwargs["params"] for c in mock_make_request.call_args_list]
            assert calls == [
                {"year": 2024, "seasonType": "regular"},
                {"year": 2024, "seasonType": "postseason"},
            ]
            assert mock_make_request.call_args_list[0].args[1] == "/stats/game/advanced"

    def test_yields_rows_from_fixture(self):
        from src.pipelines.sources.stats import game_advanced_resource

        fixture = _load_fixture("stats_game_advanced.json")

        with (
            patch("src.pipelines.sources.stats.get_client") as mock_get_client,
            patch("src.pipelines.sources.stats.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = [fixture, []]

            results = list(game_advanced_resource(years=[2024]))

            assert len(results) == len(fixture)
            assert results[0]["gameId"] == fixture[0]["gameId"]

    def test_400_season_type_is_skipped(self):
        from src.pipelines.sources.stats import game_advanced_resource

        with (
            patch("src.pipelines.sources.stats.get_client") as mock_get_client,
            patch("src.pipelines.sources.stats.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = [[], _http_error(400)]

            assert list(game_advanced_resource(years=[2024])) == []

    def test_table_name_is_game_advanced_team_stats(self):
        """Resource name `game_advanced` (used by stats_source's `only`
        filter) maps to table stats.game_advanced_team_stats."""
        from src.pipelines.sources.stats import game_advanced_resource

        with patch("src.pipelines.sources.stats.get_client") as mock_get_client:
            mock_get_client.return_value = MagicMock()

            resource = game_advanced_resource(years=[2024])

            assert resource.name == "game_advanced"
            schema = resource.compute_table_schema()
            assert schema["name"] == "game_advanced_team_stats"

    def test_merge_disposition_and_primary_key(self):
        from src.pipelines.sources.stats import game_advanced_resource

        with patch("src.pipelines.sources.stats.get_client") as mock_get_client:
            mock_get_client.return_value = MagicMock()

            resource = game_advanced_resource(years=[2024])

            assert resource.write_disposition == "merge"
            schema = resource.compute_table_schema()
            pk_columns = {name for name, col in schema["columns"].items() if col.get("primary_key")}
            assert pk_columns == {"game_id", "team"}
