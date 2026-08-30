"""Tests for the player-season-overview source and its drainer.

Covers src/pipelines/sources/player_overview.py::player_overview_source /
player_season_overview_resource (mocked CFBD client -- no network) and
src/pipelines/run.py::run_player_overview_pipeline plus its helpers
(_season_is_final, _dedup_rows) (mocked psycopg2/rate-limiter/dlt -- no DB,
no network). Mirrors test_sources/test_metrics_wp.py's split between
resource-level and pipeline-level tests, since /player/season/overview is
the same DB-set-difference-drainer shape as /metrics/wp.
"""

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
    request = httpx.Request("GET", "https://api.collegefootballdata.com/player/season/overview")
    response = httpx.Response(status_code, request=request)
    return httpx.HTTPStatusError("error", request=request, response=response)


# ---------------------------------------------------------------------------
# player_season_overview_resource / player_overview_source
# ---------------------------------------------------------------------------


class TestPlayerOverviewSource:
    def test_requires_player_seasons(self):
        from src.pipelines.sources.player_overview import player_overview_source

        with pytest.raises(ValueError, match="player_seasons parameter is required"):
            player_overview_source(player_seasons=[])

    def test_returns_player_season_overview_resource(self):
        from src.pipelines.sources.player_overview import player_overview_source

        with patch("src.pipelines.sources.player_overview.get_client") as mock_get_client:
            mock_get_client.return_value = MagicMock()

            source = player_overview_source(player_seasons=[(2024, "5083552")])

            assert set(source.resources.keys()) == {"player_season_overview"}

    def test_forwards_misses_collector_to_the_resource(self):
        from src.pipelines.sources.player_overview import player_overview_source

        with (
            patch("src.pipelines.sources.player_overview.get_client") as mock_get_client,
            patch("src.pipelines.sources.player_overview.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = _http_error(404)

            misses: list[tuple[str, int]] = []
            source = player_overview_source(player_seasons=[(2024, "5083552")], misses=misses)
            list(source.resources["player_season_overview"])

            assert misses == [("2024:5083552", 404)]


class TestPlayerSeasonOverviewResource:
    def test_one_call_per_year_player_id_pair(self):
        from src.pipelines.sources.player_overview import player_season_overview_resource

        with (
            patch("src.pipelines.sources.player_overview.get_client") as mock_get_client,
            patch("src.pipelines.sources.player_overview.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.return_value = []

            list(
                player_season_overview_resource(
                    player_seasons=[(2024, "5083552"), (2023, "4832789")]
                )
            )

            assert mock_make_request.call_count == 2
            calls = mock_make_request.call_args_list
            assert calls[0].args[1] == "/player/season/overview"
            assert calls[0].kwargs["params"] == {"year": 2024, "playerId": "5083552"}
            assert calls[1].kwargs["params"] == {"year": 2023, "playerId": "4832789"}

    def test_yields_rows_matching_the_fixture_shape(self):
        """season/id/team/position are top-level -- the player-grain join
        spine -- while boxScoreStats/usage/ppa arrive intact for dlt to
        flatten (usage, ppa) or child-table (boxScoreStats.categories)."""
        from src.pipelines.sources.player_overview import player_season_overview_resource

        fixture = _load_fixture("player_season_overview.json")

        with (
            patch("src.pipelines.sources.player_overview.get_client") as mock_get_client,
            patch("src.pipelines.sources.player_overview.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.return_value = fixture

            results = list(player_season_overview_resource(player_seasons=[(2024, "5083552")]))

            assert len(results) == 1
            row = results[0]
            assert row["season"] == 2024
            assert row["id"] == "5083552"
            assert row["team"] == "Abilene Christian"
            assert row["position"] == "WR"
            assert row["boxScoreStats"]["categories"][0]["name"] == "receiving"
            assert row["usage"]["overall"] == 0.079
            assert row["ppa"]["average"]["all"] == 1.41

    def test_bare_object_response_is_also_accepted(self):
        """The OpenAPI spec declares PlayerSeasonOverview as a bare object,
        not a list -- the probe fixture happened to capture it wrapped in a
        single-item list, so the resource must accept either shape."""
        from src.pipelines.sources.player_overview import player_season_overview_resource

        bare_row = _load_fixture("player_season_overview.json")[0]

        with (
            patch("src.pipelines.sources.player_overview.get_client") as mock_get_client,
            patch("src.pipelines.sources.player_overview.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.return_value = bare_row

            results = list(player_season_overview_resource(player_seasons=[(2024, "5083552")]))

            assert len(results) == 1
            assert results[0]["id"] == "5083552"

    def test_empty_response_yields_nothing(self):
        from src.pipelines.sources.player_overview import player_season_overview_resource

        with (
            patch("src.pipelines.sources.player_overview.get_client") as mock_get_client,
            patch("src.pipelines.sources.player_overview.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.return_value = {}

            assert list(player_season_overview_resource(player_seasons=[(2024, "1")])) == []

    def test_row_missing_season_or_id_is_skipped(self):
        from src.pipelines.sources.player_overview import player_season_overview_resource

        with (
            patch("src.pipelines.sources.player_overview.get_client") as mock_get_client,
            patch("src.pipelines.sources.player_overview.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.return_value = {"name": "No Id"}

            assert list(player_season_overview_resource(player_seasons=[(2024, "1")])) == []

    def test_400_response_skips_pair_and_continues(self):
        from src.pipelines.sources.player_overview import player_season_overview_resource

        fixture = _load_fixture("player_season_overview.json")

        with (
            patch("src.pipelines.sources.player_overview.get_client") as mock_get_client,
            patch("src.pipelines.sources.player_overview.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = [_http_error(400), fixture]

            results = list(
                player_season_overview_resource(player_seasons=[(2024, "nope"), (2024, "5083552")])
            )

            assert len(results) == 1
            assert results[0]["id"] == "5083552"

    def test_404_response_skips_pair_and_continues(self):
        from src.pipelines.sources.player_overview import player_season_overview_resource

        fixture = _load_fixture("player_season_overview.json")

        with (
            patch("src.pipelines.sources.player_overview.get_client") as mock_get_client,
            patch("src.pipelines.sources.player_overview.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = [_http_error(404), fixture]

            results = list(
                player_season_overview_resource(player_seasons=[(2024, "nope"), (2024, "5083552")])
            )

            assert len(results) == 1

    def test_other_status_errors_are_not_swallowed(self):
        from dlt.extract.exceptions import ResourceExtractionError

        from src.pipelines.sources.player_overview import player_season_overview_resource

        with (
            patch("src.pipelines.sources.player_overview.get_client") as mock_get_client,
            patch("src.pipelines.sources.player_overview.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = _http_error(500)

            with pytest.raises(ResourceExtractionError) as exc_info:
                list(player_season_overview_resource(player_seasons=[(2024, "1")]))

            assert isinstance(exc_info.value.__cause__, httpx.HTTPStatusError)
            assert exc_info.value.__cause__.response.status_code == 500

    def test_400_appends_key_and_status_code_to_misses(self):
        """PR #75 review finding A: without this collection, a terminal
        400/404 was silently dropped and re-requested by the drainer every
        run forever."""
        from src.pipelines.sources.player_overview import player_season_overview_resource

        fixture = _load_fixture("player_season_overview.json")

        with (
            patch("src.pipelines.sources.player_overview.get_client") as mock_get_client,
            patch("src.pipelines.sources.player_overview.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = [_http_error(400), fixture]

            misses: list[tuple[str, int]] = []
            list(
                player_season_overview_resource(
                    player_seasons=[(2024, "nope"), (2024, "5083552")], misses=misses
                )
            )

            assert misses == [("2024:nope", 400)]

    def test_404_appends_key_and_status_code_to_misses(self):
        from src.pipelines.sources.player_overview import player_season_overview_resource

        with (
            patch("src.pipelines.sources.player_overview.get_client") as mock_get_client,
            patch("src.pipelines.sources.player_overview.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = _http_error(404)

            misses: list[tuple[str, int]] = []
            list(player_season_overview_resource(player_seasons=[(2023, "999")], misses=misses))

            assert misses == [("2023:999", 404)]

    def test_miss_key_format_is_season_colon_player_id(self):
        """meta.fanout_misses.key format for this source is
        '{season}:{player_id}' -- distinct from coach_profiles' bare
        str(coach_id)."""
        from src.pipelines.sources.player_overview import player_season_overview_resource

        with (
            patch("src.pipelines.sources.player_overview.get_client") as mock_get_client,
            patch("src.pipelines.sources.player_overview.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = _http_error(400)

            misses: list[tuple[str, int]] = []
            list(player_season_overview_resource(player_seasons=[(2025, "42")], misses=misses))

            assert misses == [("2025:42", 400)]

    def test_misses_none_is_safe(self):
        """The default -- no collector passed -- must not raise."""
        from src.pipelines.sources.player_overview import player_season_overview_resource

        with (
            patch("src.pipelines.sources.player_overview.get_client") as mock_get_client,
            patch("src.pipelines.sources.player_overview.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = [_http_error(400), _http_error(404)]

            results = list(
                player_season_overview_resource(
                    player_seasons=[(2024, "a"), (2024, "b")],
                )
            )

            assert results == []

    def test_merge_write_disposition_and_compound_primary_key(self):
        """team added 2026-08-30 pre-backfill for transfer safety and grain
        consistency with player_success_season/passing_player_season
        (cfb-app work-order task 1) -- CFBD normally returns one overview
        record per player-season with a single team attribution, so this is
        insurance against a per-team split, not an observed one."""
        from src.pipelines.sources.player_overview import player_season_overview_resource

        with patch("src.pipelines.sources.player_overview.get_client") as mock_get_client:
            mock_get_client.return_value = MagicMock()

            resource = player_season_overview_resource(player_seasons=[(2024, "1")])

            assert resource.write_disposition == "merge"
            schema = resource.compute_table_schema()
            pk_columns = {name for name, col in schema["columns"].items() if col.get("primary_key")}
            assert pk_columns == {"season", "id", "team"}


# ---------------------------------------------------------------------------
# _dedup_rows (src/pipelines/run.py) -- pure, no DB
# ---------------------------------------------------------------------------


class TestDedupRows:
    def test_union_of_disjoint_lists(self):
        from src.pipelines.run import _dedup_rows

        assert _dedup_rows([(2024, "1")], [(2024, "2")]) == [(2024, "1"), (2024, "2")]

    def test_overlap_is_deduplicated_keeping_first_occurrence_order(self):
        from src.pipelines.run import _dedup_rows

        result = _dedup_rows([(2024, "1"), (2024, "2")], [(2024, "2"), (2024, "3")])
        assert result == [(2024, "1"), (2024, "2"), (2024, "3")]

    def test_accepts_plain_lists_as_rows_not_just_tuples(self):
        """psycopg2 rows are tuples, but the helper normalizes via tuple()
        so a caller (or a test) may pass plain lists too."""
        from src.pipelines.run import _dedup_rows

        assert _dedup_rows([[2024, "1"]], [[2024, "1"]]) == [(2024, "1")]

    def test_empty_inputs(self):
        from src.pipelines.run import _dedup_rows

        assert _dedup_rows() == []
        assert _dedup_rows([], []) == []

    def test_more_than_two_lists(self):
        from src.pipelines.run import _dedup_rows

        result = _dedup_rows([(1, "a")], [(1, "b")], [(1, "a")])
        assert result == [(1, "a"), (1, "b")]


# ---------------------------------------------------------------------------
# _season_is_final (src/pipelines/run.py) -- mirrors
# scripts/load_season.py::season_is_final; same FakeConn/FakeCursor testing
# precedent as tests/test_load_season.py::TestSeasonIsFinal.
# ---------------------------------------------------------------------------


class _FakeCursor:
    def __init__(self, row):
        self._row = row

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, *a, **k):
        pass

    def fetchone(self):
        return self._row


class _FakeConn:
    def __init__(self, row):
        self._row = row

    def cursor(self):
        return _FakeCursor(self._row)


class TestSeasonIsFinalGate:
    """A season's box score/usage/PPA totals mutate weekly while games are
    still being played -- this gate is what keeps run_player_overview_pipeline
    from loading a season early and re-loading it every day."""

    def test_a_completed_season_is_final(self):
        from src.pipelines.run import _season_is_final

        assert _season_is_final(_FakeConn((900, 1.0)), 2025) is True

    def test_tolerance_allows_a_stray_uncompleted_game(self):
        from src.pipelines.run import _season_is_final

        assert _season_is_final(_FakeConn((900, 0.995)), 2025) is True

    def test_a_season_in_progress_is_not_final(self):
        from src.pipelines.run import _season_is_final

        assert _season_is_final(_FakeConn((3677, 0.0158)), 2026) is False

    def test_too_few_games_is_not_final(self):
        from src.pipelines.run import _MIN_GAMES_FOR_FINISHED_SEASON, _season_is_final

        assert _season_is_final(_FakeConn((3, 1.0)), 2026) is False
        assert _season_is_final(_FakeConn((_MIN_GAMES_FOR_FINISHED_SEASON - 1, 1.0)), 2026) is False

    def test_near_complete_season_is_final(self):
        from src.pipelines.run import _season_is_final

        assert _season_is_final(_FakeConn((3801, 0.9995)), 2024) is True

    def test_an_unloaded_season_is_not_final(self):
        from src.pipelines.run import _season_is_final

        assert _season_is_final(_FakeConn((0, None)), 2027) is False


# ---------------------------------------------------------------------------
# run_player_overview_pipeline (src/pipelines/run.py)
# ---------------------------------------------------------------------------


def _mock_conn(usage_rows, ppa_rows, existing_rows, recent_misses=None):
    """Build a MagicMock psycopg2 connection matching
    run_player_overview_pipeline's candidate resolution for a single
    explicitly-passed season (so the seasons=None discovery queries are
    skipped): the recent-fanout-misses query (PR #75 review finding A,
    fetched once before the per-season loop -- _fetch_recent_fanout_misses,
    reading meta.fanout_misses via _fetch_rows_or_empty) comes first,
    returning `recent_misses` (string keys, default none), followed by the
    three per-season `cur.execute(...); cur.fetchall()` calls (usage
    candidates, ppa candidates, existing rows), in that order."""
    conn = MagicMock()
    cur = conn.cursor.return_value.__enter__.return_value
    cur.fetchall.side_effect = [
        [(key,) for key in (recent_misses or [])],
        usage_rows,
        ppa_rows,
        existing_rows,
    ]
    return conn


class TestRunPlayerOverviewPipelineBatching:
    def test_chunks_missing_into_batch_size_groups(self):
        from src.pipelines.run import run_player_overview_pipeline

        usage_rows = [(2024, str(i)) for i in range(1, 121)]
        conn = _mock_conn(usage_rows, [], [])

        mock_pipeline = MagicMock()
        mock_pipeline.run.return_value = "load-info"

        with (
            patch("src.pipelines.run._metrics_wp_db_url", return_value="postgres://fake"),
            patch("psycopg2.connect", return_value=conn),
            patch("src.pipelines.run._season_is_final", return_value=True),
            patch("src.pipelines.run.dlt.pipeline", return_value=mock_pipeline),
            patch("src.pipelines.run.player_overview_source") as mock_source,
        ):
            result = run_player_overview_pipeline(seasons=[2024], batch_size=50)

        assert result["missing"] == 120
        assert result["batches"] == 3
        assert mock_pipeline.run.call_count == 3
        batch_sizes = [len(call.kwargs["player_seasons"]) for call in mock_source.call_args_list]
        assert batch_sizes == [50, 50, 20]

    def test_already_loaded_pairs_are_excluded(self):
        from src.pipelines.run import run_player_overview_pipeline

        usage_rows = [(2024, "1"), (2024, "2"), (2024, "3")]
        conn = _mock_conn(usage_rows, [], [(2024, "1"), (2024, "2")])

        mock_pipeline = MagicMock()

        with (
            patch("src.pipelines.run._metrics_wp_db_url", return_value="postgres://fake"),
            patch("psycopg2.connect", return_value=conn),
            patch("src.pipelines.run._season_is_final", return_value=True),
            patch("src.pipelines.run.dlt.pipeline", return_value=mock_pipeline),
            patch("src.pipelines.run.player_overview_source") as mock_source,
        ):
            result = run_player_overview_pipeline(seasons=[2024], batch_size=50)

        assert result["missing"] == 1
        mock_source.assert_called_once()
        assert mock_source.call_args.kwargs["player_seasons"] == [(2024, "3")]

    def test_union_of_usage_and_ppa_candidates(self):
        """A player present in only metrics.ppa_players_season (not
        stats.player_usage) must still be a candidate -- neither table
        alone is a complete 'every player who played this season' list."""
        from src.pipelines.run import run_player_overview_pipeline

        conn = _mock_conn([(2024, "1")], [(2024, "2")], [])

        mock_pipeline = MagicMock()

        with (
            patch("src.pipelines.run._metrics_wp_db_url", return_value="postgres://fake"),
            patch("psycopg2.connect", return_value=conn),
            patch("src.pipelines.run._season_is_final", return_value=True),
            patch("src.pipelines.run.dlt.pipeline", return_value=mock_pipeline),
            patch("src.pipelines.run.player_overview_source") as mock_source,
        ):
            result = run_player_overview_pipeline(seasons=[2024], batch_size=50)

        assert result["missing"] == 2
        assert set(mock_source.call_args.kwargs["player_seasons"]) == {(2024, "1"), (2024, "2")}

    def test_undefined_table_on_fresh_backfill_treated_as_empty(self):
        """Neither stats.player_usage/metrics.ppa_players_season nor
        stats.player_season_overview being absent should crash the run --
        each is guarded independently."""
        import psycopg2.errors

        from src.pipelines.run import run_player_overview_pipeline

        conn = MagicMock()
        cur = conn.cursor.return_value.__enter__.return_value
        cur.fetchall.side_effect = [
            [],  # recent fanout misses (meta.fanout_misses)
            [(2024, "1")],  # usage candidates
            psycopg2.errors.UndefinedTable(),  # ppa candidates: table absent
            psycopg2.errors.UndefinedTable(),  # existing: table absent
        ]

        mock_pipeline = MagicMock()

        with (
            patch("src.pipelines.run._metrics_wp_db_url", return_value="postgres://fake"),
            patch("psycopg2.connect", return_value=conn),
            patch("src.pipelines.run._season_is_final", return_value=True),
            patch("src.pipelines.run.dlt.pipeline", return_value=mock_pipeline),
            patch("src.pipelines.run.player_overview_source"),
        ):
            result = run_player_overview_pipeline(seasons=[2024], batch_size=50)

        assert result["missing"] == 1

    def test_no_missing_skips_pipeline_run(self):
        from src.pipelines.run import run_player_overview_pipeline

        conn = _mock_conn([(2024, "1")], [], [(2024, "1")])

        mock_pipeline = MagicMock()

        with (
            patch("src.pipelines.run._metrics_wp_db_url", return_value="postgres://fake"),
            patch("psycopg2.connect", return_value=conn),
            patch("src.pipelines.run._season_is_final", return_value=True),
            patch("src.pipelines.run.dlt.pipeline", return_value=mock_pipeline),
        ):
            result = run_player_overview_pipeline(seasons=[2024], batch_size=50)

        assert result["missing"] == 0
        assert result["batches"] == 0
        mock_pipeline.run.assert_not_called()

    def test_cap_defers_the_rest_of_the_backlog(self):
        from src.pipelines.run import run_player_overview_pipeline

        usage_rows = [(2024, str(i)) for i in range(1, 11)]
        conn = _mock_conn(usage_rows, [], [])

        mock_pipeline = MagicMock()

        with (
            patch("src.pipelines.run._metrics_wp_db_url", return_value="postgres://fake"),
            patch("psycopg2.connect", return_value=conn),
            patch("src.pipelines.run._season_is_final", return_value=True),
            patch("src.pipelines.run.dlt.pipeline", return_value=mock_pipeline),
            patch("src.pipelines.run.player_overview_source"),
        ):
            result = run_player_overview_pipeline(seasons=[2024], max_players=4, batch_size=50)

        assert result["missing"] == 10
        assert result["loaded_this_run"] == 4
        assert result["deferred"] == 6


class TestRunPlayerOverviewPipelineSeasonGate:
    def test_seasons_none_discovers_from_both_tables_newest_first(self):
        from src.pipelines.run import run_player_overview_pipeline

        conn = MagicMock()
        cur = conn.cursor.return_value.__enter__.return_value
        cur.fetchall.side_effect = [
            [(2023,), (2024,)],  # usage seasons
            [(2022,), (2024,)],  # ppa seasons
            [],  # recent fanout misses (meta.fanout_misses) -- fetched once
            [],
            [],
            [],  # 2024: usage / ppa / existing
            [],
            [],
            [],  # 2023: usage / ppa / existing
            [],
            [],
            [],  # 2022: usage / ppa / existing
        ]

        with (
            patch("src.pipelines.run._metrics_wp_db_url", return_value="postgres://fake"),
            patch("psycopg2.connect", return_value=conn),
            patch("src.pipelines.run._season_is_final", return_value=True),
        ):
            result = run_player_overview_pipeline()

        assert result["seasons"] == [2024, 2023, 2022]
        assert result["eligible_seasons"] == [2024, 2023, 2022]

    def test_in_progress_season_is_excluded_by_the_gate(self):
        """An explicitly-passed in-progress season yields zero candidates
        rather than bypassing the gate."""
        from src.pipelines.run import run_player_overview_pipeline

        conn = MagicMock()
        cur = conn.cursor.return_value.__enter__.return_value
        cur.fetchall.side_effect = [
            [],  # recent fanout misses (meta.fanout_misses)
            [],
            [],
            [],  # 2025 only (2026 is not eligible): usage / ppa / existing
        ]

        def fake_is_final(_conn, season):
            return season == 2025

        with (
            patch("src.pipelines.run._metrics_wp_db_url", return_value="postgres://fake"),
            patch("psycopg2.connect", return_value=conn),
            patch("src.pipelines.run._season_is_final", side_effect=fake_is_final),
        ):
            result = run_player_overview_pipeline(seasons=[2026, 2025])

        assert result["eligible_seasons"] == [2025]
        assert result["seasons"] == [2026, 2025]


class TestRunPlayerOverviewPipelineBudgetGuard:
    def test_insufficient_budget_refuses_without_running_pipeline(self):
        from src.pipelines.run import run_player_overview_pipeline

        usage_rows = [(2024, str(i)) for i in range(1, 11)]
        conn = _mock_conn(usage_rows, [], [])

        mock_rate_limiter = MagicMock()
        mock_rate_limiter.check_budget.return_value = False
        mock_rate_limiter.remaining = 3

        mock_pipeline = MagicMock()

        with (
            patch("src.pipelines.run._metrics_wp_db_url", return_value="postgres://fake"),
            patch("psycopg2.connect", return_value=conn),
            patch("src.pipelines.run._season_is_final", return_value=True),
            patch("src.pipelines.run.get_rate_limiter", return_value=mock_rate_limiter),
            patch("src.pipelines.run.dlt.pipeline", return_value=mock_pipeline),
        ):
            result = run_player_overview_pipeline(seasons=[2024], batch_size=50)

        mock_rate_limiter.check_budget.assert_called_once_with(10)
        mock_pipeline.run.assert_not_called()
        assert result["batches"] == 0
        assert "error" in result

    def test_sufficient_budget_proceeds(self):
        from src.pipelines.run import run_player_overview_pipeline

        conn = _mock_conn([(2024, "1")], [], [])

        mock_rate_limiter = MagicMock()
        mock_rate_limiter.check_budget.return_value = True

        mock_pipeline = MagicMock()

        with (
            patch("src.pipelines.run._metrics_wp_db_url", return_value="postgres://fake"),
            patch("psycopg2.connect", return_value=conn),
            patch("src.pipelines.run._season_is_final", return_value=True),
            patch("src.pipelines.run.get_rate_limiter", return_value=mock_rate_limiter),
            patch("src.pipelines.run.dlt.pipeline", return_value=mock_pipeline),
            patch("src.pipelines.run.player_overview_source"),
        ):
            result = run_player_overview_pipeline(seasons=[2024], batch_size=50)

        assert result["batches"] == 1
        mock_pipeline.run.assert_called_once()


# ---------------------------------------------------------------------------
# meta.fanout_misses integration (PR #75 review finding A): recent-miss
# exclusion, excluded-count reporting, per-batch recording, and UndefinedTable
# degradation on the read side (the write side is covered once in
# test_sources/test_coaches.py -- _record_fanout_misses is shared code).
# ---------------------------------------------------------------------------


class TestRunPlayerOverviewPipelineFanoutMisses:
    def test_recent_miss_excluded_from_missing(self):
        from src.pipelines.run import run_player_overview_pipeline

        usage_rows = [(2024, "1"), (2024, "2"), (2024, "3")]
        conn = _mock_conn(usage_rows, [], [], recent_misses=["2024:2"])

        mock_pipeline = MagicMock()

        with (
            patch("src.pipelines.run._metrics_wp_db_url", return_value="postgres://fake"),
            patch("psycopg2.connect", return_value=conn),
            patch("src.pipelines.run._season_is_final", return_value=True),
            patch("src.pipelines.run.dlt.pipeline", return_value=mock_pipeline),
            patch("src.pipelines.run.player_overview_source") as mock_source,
        ):
            result = run_player_overview_pipeline(seasons=[2024], batch_size=50)

        assert result["missing"] == 2
        assert set(mock_source.call_args.kwargs["player_seasons"]) == {(2024, "1"), (2024, "3")}

    def test_excluded_miss_count_is_reported(self):
        from src.pipelines.run import run_player_overview_pipeline

        usage_rows = [(2024, "1"), (2024, "2"), (2024, "3")]
        conn = _mock_conn(usage_rows, [], [], recent_misses=["2024:2", "2024:3"])

        mock_pipeline = MagicMock()

        with (
            patch("src.pipelines.run._metrics_wp_db_url", return_value="postgres://fake"),
            patch("psycopg2.connect", return_value=conn),
            patch("src.pipelines.run._season_is_final", return_value=True),
            patch("src.pipelines.run.dlt.pipeline", return_value=mock_pipeline),
            patch("src.pipelines.run.player_overview_source"),
        ):
            result = run_player_overview_pipeline(seasons=[2024], batch_size=50)

        assert result["excluded_misses"] == 2
        assert result["missing"] == 1

    def test_excluded_misses_reported_even_when_nothing_to_load(self):
        from src.pipelines.run import run_player_overview_pipeline

        conn = _mock_conn([(2024, "1")], [], [], recent_misses=["2024:1"])

        mock_pipeline = MagicMock()

        with (
            patch("src.pipelines.run._metrics_wp_db_url", return_value="postgres://fake"),
            patch("psycopg2.connect", return_value=conn),
            patch("src.pipelines.run._season_is_final", return_value=True),
            patch("src.pipelines.run.dlt.pipeline", return_value=mock_pipeline),
        ):
            result = run_player_overview_pipeline(seasons=[2024], batch_size=50)

        assert result["missing"] == 0
        assert result["excluded_misses"] == 1
        mock_pipeline.run.assert_not_called()

    def test_record_fanout_misses_called_once_per_batch(self):
        from src.pipelines.run import run_player_overview_pipeline

        usage_rows = [(2024, str(i)) for i in range(1, 121)]
        conn = _mock_conn(usage_rows, [], [])

        mock_pipeline = MagicMock()
        recorded = []

        def fake_source(player_seasons, *, misses=None):
            if misses is not None:
                season, player_id = player_seasons[-1]
                misses.append((f"{season}:{player_id}", 404))
            return MagicMock()

        with (
            patch("src.pipelines.run._metrics_wp_db_url", return_value="postgres://fake"),
            patch("psycopg2.connect", return_value=conn),
            patch("src.pipelines.run._season_is_final", return_value=True),
            patch("src.pipelines.run.dlt.pipeline", return_value=mock_pipeline),
            patch("src.pipelines.run.player_overview_source", side_effect=fake_source),
            patch(
                "src.pipelines.run._record_fanout_misses",
                side_effect=lambda source, misses: recorded.append((source, misses)),
            ) as mock_record,
        ):
            result = run_player_overview_pipeline(seasons=[2024], max_players=200, batch_size=50)

        assert result["batches"] == 3
        assert mock_record.call_count == 3
        assert recorded == [
            ("player_season_overview", [("2024:50", 404)]),
            ("player_season_overview", [("2024:100", 404)]),
            ("player_season_overview", [("2024:120", 404)]),
        ]

    def test_record_fanout_misses_not_called_when_batch_has_no_misses(self):
        from src.pipelines.run import run_player_overview_pipeline

        conn = _mock_conn([(2024, "1")], [], [])

        mock_pipeline = MagicMock()

        with (
            patch("src.pipelines.run._metrics_wp_db_url", return_value="postgres://fake"),
            patch("psycopg2.connect", return_value=conn),
            patch("src.pipelines.run._season_is_final", return_value=True),
            patch("src.pipelines.run.dlt.pipeline", return_value=mock_pipeline),
            patch("src.pipelines.run.player_overview_source"),
            patch("src.pipelines.run._record_fanout_misses") as mock_record,
        ):
            run_player_overview_pipeline(seasons=[2024], batch_size=50)

        mock_record.assert_not_called()

    def test_undefined_table_reading_fanout_misses_yields_no_exclusion(self):
        """meta.fanout_misses not yet created (migration 056 not applied)
        must degrade to 'no exclusion', not crash the drainer."""
        import psycopg2.errors

        from src.pipelines.run import run_player_overview_pipeline

        conn = MagicMock()
        cur = conn.cursor.return_value.__enter__.return_value
        cur.fetchall.side_effect = [
            psycopg2.errors.UndefinedTable(),  # recent fanout misses: table absent
            [(2024, "1")],  # usage candidates
            [],  # ppa candidates
            [],  # existing
        ]

        mock_pipeline = MagicMock()

        with (
            patch("src.pipelines.run._metrics_wp_db_url", return_value="postgres://fake"),
            patch("psycopg2.connect", return_value=conn),
            patch("src.pipelines.run._season_is_final", return_value=True),
            patch("src.pipelines.run.dlt.pipeline", return_value=mock_pipeline),
            patch("src.pipelines.run.player_overview_source") as mock_source,
        ):
            result = run_player_overview_pipeline(seasons=[2024], batch_size=50)

        assert result["missing"] == 1
        assert result["excluded_misses"] == 0
        mock_source.assert_called_once()
