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
        assert {"player_success_game", "game_advanced"} <= names

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


class TestPlayerSuccessSourceSplit:
    """2026-10-19 (Daily Season Load #98): a timeout on /stats/player/success
    failed the whole stats source and discarded the already-fetched
    advanced_team_stats / player_returning / player_season_stats /
    player_usage extracts -- a dlt source failure discards every sibling
    resource. The season resource now ships in its own source
    (player_success_source) so its failure cannot touch the critical stats
    siblings."""

    def test_player_success_source_contains_only_the_season_resource(self):
        from src.pipelines.sources.stats import player_success_source

        source = player_success_source(years=[2024])

        assert set(source.resources.keys()) == {"player_success_season"}

    def test_stats_source_no_longer_carries_player_success_season(self):
        """The isolation guarantee: with the resource outside stats_source,
        a /stats/player/success failure structurally cannot discard the
        stats extract."""
        from src.pipelines.sources.stats import stats_source

        source = stats_source(years=[2024])

        assert "player_success_season" not in set(source.resources.keys())

    def test_split_preserves_the_pre_split_resource_set(self):
        """The union across both sources is exactly the pre-split
        stats_source list -- no resource gained, lost, or duplicated, so a
        caller loading both sources still loads everything."""
        from src.pipelines.sources.stats import player_success_source, stats_source

        stats_names = set(stats_source(years=[2024]).resources.keys())
        success_names = set(player_success_source(years=[2024]).resources.keys())

        assert stats_names & success_names == set()
        assert stats_names | success_names == {
            "team_season_stats",
            "player_season_stats",
            "advanced_team_stats",
            "player_usage",
            "player_returning",
            "play_stats",
            "game_havoc",
            "player_success_season",
            "player_success_game",
            "game_advanced",
        }

    def test_table_name_schema_and_pk_are_unchanged(self):
        """Proof (f): the split reuses the same resource function, so the
        table name, merge disposition, and primary key are byte-identical to
        the pre-split load -- no DB change, existing rows untouched."""
        from src.pipelines.sources.stats import player_success_source

        source = player_success_source(years=[2024])
        resource = source.resources["player_success_season"]

        assert resource.write_disposition == "merge"
        schema = resource.compute_table_schema()
        assert schema["name"] == "player_success_season"
        pk_columns = {name for name, col in schema["columns"].items() if col.get("primary_key")}
        assert pk_columns == {"season", "id", "team"}


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

    @pytest.mark.parametrize("timeout_cls", [httpx.ConnectTimeout, httpx.PoolTimeout])
    def test_connection_level_timeouts_stay_on_the_outage_path(self, timeout_cls):
        """Codex P1 on PR #158: ConnectTimeout and PoolTimeout are
        connection-level -- CFBD may never have received the request -- so
        the game must NOT be retired as no_data; the batch stops as an
        outage with the game left in the backlog."""
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
                timeout_cls("connection-level timeout"),
                AssertionError("game 3 must never be requested"),
            ]

            misses: list[int] = []
            report = AdvancedBoxBatchReport()
            results = list(
                advanced_game_stats_resource(game_ids=[1, 2, 3], misses=misses, report=report)
            )

        assert results == [{"gameId": 1, "team": "A"}]
        assert report.outage is not None
        assert report.attempted == [1]
        assert misses == []
        assert report.server_error_game_ids == []

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

    def test_dict_shaped_200_is_a_miss_and_never_reaches_dlt(self):
        """Historical Refresh #55 (run 39816658236, 2026-10-22): a 200
        response carrying a JSON OBJECT instead of a list of row dicts was
        `yield from`-ed as its KEY STRINGS, which dlt normalized into an
        all-null row that violated game_id NOT NULL at the load stage and
        lost the whole 50-game batch. The object response must become a
        no_data miss, with nothing yielded for that game."""
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
                {"gameInfo": None, "teams": [], "players": []},
                [{"gameId": 3, "team": "B"}],
            ]

            misses: list[int] = []
            report = AdvancedBoxBatchReport()
            results = list(
                advanced_game_stats_resource(game_ids=[1, 2, 3], misses=misses, report=report)
            )

        # Only the good games' rows reached dlt -- no key strings, no rows
        # without a gameId.
        assert results == [{"gameId": 1, "team": "A"}, {"gameId": 3, "team": "B"}]
        assert all(isinstance(r, dict) and r.get("gameId") is not None for r in results)
        # The object-response game is a plain miss (ledger no_data,
        # requeueable), answered as far as the health window is concerned.
        assert misses == [2]
        assert report.attempted == [1, 2, 3]
        assert report.server_error_game_ids == []
        assert report.outage is None

    def test_rows_without_game_id_are_dropped_before_dlt(self):
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
                # One usable row, plus junk that must never reach dlt.
                [{"gameId": 1, "team": "A"}, {"team": "orphan"}, None, "gameInfo"],
                # Nothing usable at all -> a miss.
                [{"team": "orphan-only"}],
            ]

            misses: list[int] = []
            report = AdvancedBoxBatchReport()
            results = list(
                advanced_game_stats_resource(game_ids=[1, 2], misses=misses, report=report)
            )

        assert results == [{"gameId": 1, "team": "A"}]
        assert misses == [2]
        assert report.attempted == [1, 2]

    def test_miss_paths_yield_nothing_and_good_games_load(self):
        """One game exhausting 5xx retries and one per-game read timeout,
        inside a batch of good games: neither miss may emit anything into
        the resource (no null-game_id rows), and the good games' rows all
        come through."""
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
                _http_error(500),
                [{"gameId": 3, "team": "B"}],
                httpx.ReadTimeout("timed out"),
                [{"gameId": 5, "team": "C"}],
            ]

            misses: list[int] = []
            report = AdvancedBoxBatchReport()
            results = list(
                advanced_game_stats_resource(game_ids=[1, 2, 3, 4, 5], misses=misses, report=report)
            )

        assert results == [
            {"gameId": 1, "team": "A"},
            {"gameId": 3, "team": "B"},
            {"gameId": 5, "team": "C"},
        ]
        assert all(isinstance(r, dict) and r.get("gameId") is not None for r in results)
        assert misses == [2, 4]
        assert report.server_error_game_ids == [2, 4]
        assert report.attempted == [1, 2, 3, 4, 5]
        assert report.outage is None

    def test_run56_stop_batch_path_emits_no_null_rows(self):
        """Historical Refresh #56 (run 39988227198, 2026-10-23, on main
        without this PR): game 401836853 hit 500s then a ReadTimeout on the
        final attempt, the batch stopped via 'rows loaded so far are kept and
        ledgered' -- and those kept rows included an all-null row from an
        unusable 200 earlier in the batch, so dlt still died with
        NotNullViolation on advanced_game_stats.game_id. This replays that
        combination on this PR's code: the unusable 200 is filtered, the
        ReadTimeout is a per-game miss (not a stop), and when a
        connection-level failure later DOES stop the batch, every row kept
        from before the stop carries a non-null gameId."""
        from src.pipelines.sources.stats import (
            AdvancedBoxBatchReport,
            advanced_game_stats_resource,
        )

        game_ids = [101, 102, 401836853, 104, 105, 106]
        with (
            patch("src.pipelines.sources.stats.get_client") as mock_get_client,
            patch("src.pipelines.sources.stats.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = [
                [{"gameId": 101, "team": "A"}],
                {"gameInfo": None, "teams": [], "players": []},  # unusable 200
                httpx.ReadTimeout("The read operation timed out"),  # 401836853
                [{"gameId": 104, "team": "B"}],
                httpx.ConnectError("connection failed"),  # stops the batch
                AssertionError("game 106 must never be requested"),
            ]

            misses: list[int] = []
            report = AdvancedBoxBatchReport()
            results = list(
                advanced_game_stats_resource(game_ids=game_ids, misses=misses, report=report)
            )

        # The rows kept before the stop are exactly the clean ones -- no key
        # strings, no all-null rows, nothing lacking a gameId.
        assert results == [{"gameId": 101, "team": "A"}, {"gameId": 104, "team": "B"}]
        assert all(isinstance(r, dict) and r.get("gameId") is not None for r in results)
        # The unusable 200 and the read timeout are per-game misses; the
        # read timeout also counts toward the health window.
        assert misses == [102, 401836853]
        assert report.server_error_game_ids == [401836853]
        # The connection failure stopped the batch: its game is not
        # attempted (stays in the backlog) and the stop is an outage.
        assert report.attempted == [101, 102, 401836853, 104]
        assert report.outage is not None
        assert report.budget_stop is None

    def test_single_choke_point_for_rows_reaching_dlt(self):
        """Every no-data path (500-retries-exhausted, read timeout, 400,
        empty/unusable 200) and every stop path must be unable to emit a
        null-game_id row BY CONSTRUCTION: the resource has exactly one yield
        statement, and it is fed by _usable_advanced_box_rows. A second
        yield, or a yield that bypasses the filter, reintroduces the run
        #55/#56/#57 NotNullViolation class of failure."""
        import inspect
        import re

        from src.pipelines.sources import stats as stats_module

        module_src = inspect.getsource(stats_module)
        fn_src = module_src.split("def advanced_game_stats_resource", 1)[1]
        fn_src = fn_src.split("\n@dlt.resource", 1)[0]

        yield_statements = re.findall(r"^\s*yield\b.*$", fn_src, re.MULTILINE)
        assert len(yield_statements) == 1
        assert yield_statements[0].strip() == "yield from rows"
        assert "rows = _usable_advanced_box_rows(game_id, data)" in fn_src

    def test_run60_204_no_content_is_a_no_data_miss_end_to_end(self):
        """Historical Refresh #60 (run 40552220540, 2026-10-27, on main
        without this PR): /game/box/advanced answered 204 No Content for
        game 401838337, the client raised JSONDecodeError parsing the empty
        body, and the batch stopped on its FIRST game -- zero backlog
        progress, repeating every run. End-to-end through the REAL
        CFBDClient and make_request (only the HTTP transport and the local
        rate limiter are faked): the 204 parses as [], flows through the
        same choke point as every other no-data path (a miss, ledgerable as
        no_data), the rest of the batch loads, and no row without a gameId
        is emitted."""
        from src.pipelines.sources import base as base_module
        from src.pipelines.sources.stats import (
            AdvancedBoxBatchReport,
            advanced_game_stats_resource,
        )
        from src.pipelines.utils.api_client import CFBDClient

        def fake_transport_get(endpoint, params=None, **kwargs):
            request = httpx.Request("GET", "https://fixture" + endpoint)
            gid = params["id"]
            if gid == 401838337:
                return httpx.Response(204, request=request)
            return httpx.Response(200, json=[{"gameId": gid, "team": "X"}], request=request)

        class StubLimiter:
            def check_budget(self, n=1):
                return True

            def record_call(self):
                return None

        client = CFBDClient(api_key="test-key")
        with (
            patch("src.pipelines.sources.stats.get_client", return_value=client),
            patch.object(client._client, "get", side_effect=fake_transport_get),
            patch.object(base_module, "get_rate_limiter", return_value=StubLimiter()),
        ):
            misses: list[int] = []
            report = AdvancedBoxBatchReport()
            results = list(
                advanced_game_stats_resource(
                    game_ids=[401838400, 401838337, 401838300], misses=misses, report=report
                )
            )

        assert results == [
            {"gameId": 401838400, "team": "X"},
            {"gameId": 401838300, "team": "X"},
        ]
        assert all(isinstance(r, dict) and r.get("gameId") is not None for r in results)
        # The 204 game is a plain no-data miss: ledgerable, requeueable,
        # answered as far as the health window is concerned, no stop.
        assert misses == [401838337]
        assert report.attempted == [401838400, 401838337, 401838300]
        assert report.server_error_game_ids == []
        assert report.outage is None
        assert report.budget_stop is None

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
