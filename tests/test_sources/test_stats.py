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

    def test_split_source_keeps_the_cfbd_stats_schema_identity(self):
        """Codex review on PR #157 (P1): stats.player_success_season's
        destination layout -- bigint success-rate base columns with
        passing/rushing __success_rate__v_double variant twins (see
        src/schemas/baseline/20260907_pre_f05.sql) -- is a product of the
        stored `cfbd_stats` dlt schema's history. A new schema name would
        start a fresh schema, infer those fields as double, and conflict
        with the existing bigint destination columns on the first load. The
        split source must therefore keep dlt schema name `cfbd_stats` so the
        stored schema (variant layout included) is restored and reused."""
        from src.pipelines.sources.stats import player_success_source, stats_source

        assert player_success_source(years=[2024]).schema.name == "cfbd_stats"
        assert player_success_source(years=[2024]).schema.name == (
            stats_source(years=[2024]).schema.name
        )

    def test_runner_keeps_the_cfbd_stats_pipeline_identity(self):
        """Companion to the schema-identity test above: dlt restores stored
        schemas from the destination keyed by pipeline_name, so the runner
        must reuse pipeline_name="cfbd_stats" (and dataset_name="stats") or
        a fresh CI runner would still normalize against an empty schema.
        Same source-text pattern as tests/test_backfill_refresh.py::
        test_pipeline_identity_matches_run_stats_pipeline (no DB, no
        dlt.pipeline() call)."""
        import inspect

        from src.pipelines.run import run_player_success_pipeline

        src = inspect.getsource(run_player_success_pipeline)
        assert 'pipeline_name="cfbd_stats"' in src
        assert 'dataset_name="stats"' in src


class TestAdvancedGameStatsResourceGameIdsMode:
    """R1 rework: advanced_game_stats_resource takes explicit game_ids and
    calls /game/box/advanced?id=<gameId> per id, mirroring
    play_stats_resource's explicit-ids branch."""

    @pytest.fixture(autouse=True)
    def _reset_advanced_box_5xx_circuit(self):
        """The 5xx breaker is process-wide, so one test must not open it for the next."""
        from src.pipelines.sources.stats import reset_advanced_box_5xx_circuit

        reset_advanced_box_5xx_circuit()
        yield
        reset_advanced_box_5xx_circuit()

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

    def test_three_consecutive_5xx_opens_circuit(self):
        from dlt.extract.exceptions import ResourceExtractionError

        from src.pipelines.sources.stats import (
            AdvancedBoxScoreCircuitOpen,
            advanced_game_stats_resource,
        )

        with (
            patch("src.pipelines.sources.stats.get_client") as mock_get_client,
            patch("src.pipelines.sources.stats.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = [
                _http_error(500),
                _http_error(500),
                _http_error(500),
                [{"gameId": 55, "team": "X"}],
            ]

            misses: list[int] = []
            with pytest.raises(ResourceExtractionError) as exc_info:
                list(advanced_game_stats_resource(game_ids=[11, 22, 44, 55], misses=misses))

            cause = exc_info.value.__cause__
            assert isinstance(cause, AdvancedBoxScoreCircuitOpen)
            assert isinstance(cause.__cause__, httpx.HTTPStatusError)
            assert cause.__cause__.response.status_code == 500
            assert "3" in str(cause)
            assert "44" in str(cause)
            assert misses == [11, 22, 44]
            assert mock_make_request.call_count == 3

    def test_success_resets_consecutive_5xx_counter(self):
        from src.pipelines.sources.stats import advanced_game_stats_resource

        with (
            patch("src.pipelines.sources.stats.get_client") as mock_get_client,
            patch("src.pipelines.sources.stats.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = [
                _http_error(500),
                [{"gameId": 22, "team": "X"}],
                _http_error(500),
                _http_error(500),
            ]

            misses: list[int] = []
            results = list(advanced_game_stats_resource(game_ids=[11, 22, 33, 44], misses=misses))

            assert results == [{"gameId": 22, "team": "X"}]
            assert misses == [11, 33, 44]

    def test_suppressed_400_resets_consecutive_5xx_counter(self):
        from src.pipelines.sources.stats import advanced_game_stats_resource

        with (
            patch("src.pipelines.sources.stats.get_client") as mock_get_client,
            patch("src.pipelines.sources.stats.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = [
                _http_error(500),
                _http_error(500),
                _http_error(400),
                _http_error(500),
                _http_error(500),
                [{"gameId": 66, "team": "X"}],
            ]

            misses: list[int] = []
            results = list(
                advanced_game_stats_resource(game_ids=[11, 22, 33, 44, 55, 66], misses=misses)
            )

            assert results == [{"gameId": 66, "team": "X"}]
            assert misses == [11, 22, 33, 44, 55]

    def test_empty_200_resets_consecutive_5xx_counter(self):
        from src.pipelines.sources.stats import advanced_game_stats_resource

        with (
            patch("src.pipelines.sources.stats.get_client") as mock_get_client,
            patch("src.pipelines.sources.stats.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = [
                _http_error(500),
                _http_error(500),
                [],
                _http_error(500),
                _http_error(500),
                [{"gameId": 66, "team": "X"}],
            ]

            misses: list[int] = []
            results = list(
                advanced_game_stats_resource(game_ids=[11, 22, 33, 44, 55, 66], misses=misses)
            )

            assert results == [{"gameId": 66, "team": "X"}]
            assert misses == [11, 22, 33, 44, 55]

    def test_503_counts_the_same_as_500(self):
        from dlt.extract.exceptions import ResourceExtractionError

        from src.pipelines.sources.stats import (
            AdvancedBoxScoreCircuitOpen,
            advanced_game_stats_resource,
        )

        with (
            patch("src.pipelines.sources.stats.get_client") as mock_get_client,
            patch("src.pipelines.sources.stats.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = [
                _http_error(500),
                _http_error(503),
                _http_error(503),
                [{"gameId": 55, "team": "X"}],
            ]

            misses: list[int] = []
            with pytest.raises(ResourceExtractionError) as exc_info:
                list(advanced_game_stats_resource(game_ids=[11, 22, 44, 55], misses=misses))

            cause = exc_info.value.__cause__
            assert isinstance(cause, AdvancedBoxScoreCircuitOpen)
            assert isinstance(cause.__cause__, httpx.HTTPStatusError)
            assert cause.__cause__.response.status_code == 503
            assert misses == [11, 22, 44]
            assert mock_make_request.call_count == 3

    def test_three_single_game_invocations_of_5xx_open_circuit(self):
        from dlt.extract.exceptions import ResourceExtractionError

        from src.pipelines.sources.stats import (
            AdvancedBoxScoreCircuitOpen,
            advanced_game_stats_resource,
        )

        with (
            patch("src.pipelines.sources.stats.get_client") as mock_get_client,
            patch("src.pipelines.sources.stats.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = [
                _http_error(500),
                _http_error(500),
                _http_error(500),
            ]

            misses: list[int] = []
            assert list(advanced_game_stats_resource(game_ids=[11], misses=misses)) == []
            assert list(advanced_game_stats_resource(game_ids=[22], misses=misses)) == []
            with pytest.raises(ResourceExtractionError) as exc_info:
                list(advanced_game_stats_resource(game_ids=[44], misses=misses))

            cause = exc_info.value.__cause__
            assert isinstance(cause, AdvancedBoxScoreCircuitOpen)
            assert isinstance(cause.__cause__, httpx.HTTPStatusError)
            assert cause.__cause__.response.status_code == 500
            assert "3" in str(cause)
            assert "44" in str(cause)
            assert misses == [11, 22, 44]
            assert mock_make_request.call_count == 3

    def test_success_between_single_game_invocations_resets_counter(self):
        from src.pipelines.sources.stats import advanced_game_stats_resource

        with (
            patch("src.pipelines.sources.stats.get_client") as mock_get_client,
            patch("src.pipelines.sources.stats.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = [
                _http_error(500),
                [{"gameId": 22, "team": "X"}],
                _http_error(500),
                _http_error(500),
            ]

            misses: list[int] = []
            assert list(advanced_game_stats_resource(game_ids=[11], misses=misses)) == []
            assert list(advanced_game_stats_resource(game_ids=[22], misses=misses)) == [
                {"gameId": 22, "team": "X"}
            ]
            assert list(advanced_game_stats_resource(game_ids=[33], misses=misses)) == []
            assert list(advanced_game_stats_resource(game_ids=[44], misses=misses)) == []

            assert misses == [11, 33, 44]
            assert mock_make_request.call_count == 4

    def test_merge_disposition_and_primary_key_unchanged(self):
        from src.pipelines.sources.stats import advanced_game_stats_resource

        with patch("src.pipelines.sources.stats.get_client") as mock_get_client:
            mock_get_client.return_value = MagicMock()

            resource = advanced_game_stats_resource(game_ids=[1])

            assert resource.write_disposition == "merge"
            schema = resource.compute_table_schema()
            pk_columns = {name for name, col in schema["columns"].items() if col.get("primary_key")}
            assert pk_columns == {"game_id", "team"}


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
