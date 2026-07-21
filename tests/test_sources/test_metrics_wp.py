"""Tests for the in-game (per-play) win probability source and its runner.

Covers src/pipelines/sources/metrics.py::metrics_wp_source /
win_probability_resource (mocked CFBD client -- no network) and
src/pipelines/run.py::run_metrics_wp_pipeline (mocked psycopg2/rate-limiter/
dlt -- no DB, no network). See docs/pipeline-manifest.md row 47 for why this
source is game-id-driven instead of year-driven like every other metrics.py
resource.
"""

from unittest.mock import MagicMock, patch

import httpx
import pytest


def _http_error(status_code: int) -> httpx.HTTPStatusError:
    request = httpx.Request("GET", "https://api.collegefootballdata.com/metrics/wp")
    response = httpx.Response(status_code, request=request)
    return httpx.HTTPStatusError("error", request=request, response=response)


# ---------------------------------------------------------------------------
# win_probability_resource / metrics_wp_source
# ---------------------------------------------------------------------------


class TestWinProbabilityResource:
    def test_emits_one_request_per_game_with_game_id_param(self):
        """One /metrics/wp call per game_id, params={"gameId": <id>} -- the
        endpoint requires gameId; a year param 400s (docs/pipeline-manifest.md
        row 47), which is exactly the bug this rewrite fixes."""
        from src.pipelines.sources.metrics import win_probability_resource

        with (
            patch("src.pipelines.sources.metrics.get_client") as mock_get_client,
            patch("src.pipelines.sources.metrics.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.return_value = []

            list(win_probability_resource(game_ids=[401628455, 400756843]))

            assert mock_make_request.call_count == 2
            calls = mock_make_request.call_args_list
            assert calls[0].args[1] == "/metrics/wp"
            assert calls[0].kwargs["params"] == {"gameId": 401628455}
            assert calls[1].kwargs["params"] == {"gameId": 400756843}

    def test_stamps_game_id_and_season_onto_every_row(self):
        from src.pipelines.sources.metrics import win_probability_resource

        mock_response = [
            {"playId": 1, "homeWinProbability": 0.55},
            {"playId": 2, "homeWinProbability": 0.58},
        ]

        with (
            patch("src.pipelines.sources.metrics.get_client") as mock_get_client,
            patch("src.pipelines.sources.metrics.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.return_value = mock_response

            results = list(
                win_probability_resource(game_ids=[401628455], game_seasons={401628455: 2024})
            )

            assert len(results) == 2
            for row in results:
                assert row["game_id"] == 401628455
                assert row["season"] == 2024

    def test_missing_season_entry_leaves_season_unstamped(self):
        """A game_id absent from game_seasons gets game_id stamped but no
        season key added (rather than a misleading None)."""
        from src.pipelines.sources.metrics import win_probability_resource

        with (
            patch("src.pipelines.sources.metrics.get_client") as mock_get_client,
            patch("src.pipelines.sources.metrics.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.return_value = [{"playId": 1}]

            results = list(win_probability_resource(game_ids=[401628455], game_seasons=None))

            assert results[0]["game_id"] == 401628455
            assert "season" not in results[0]

    def test_400_response_skips_game_and_continues(self):
        from src.pipelines.sources.metrics import win_probability_resource

        with (
            patch("src.pipelines.sources.metrics.get_client") as mock_get_client,
            patch("src.pipelines.sources.metrics.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = [_http_error(400), [{"playId": 99}]]

            results = list(win_probability_resource(game_ids=[111, 222]))

            assert mock_make_request.call_count == 2
            assert len(results) == 1
            assert results[0]["game_id"] == 222

    def test_404_response_skips_game_and_continues(self):
        from src.pipelines.sources.metrics import win_probability_resource

        with (
            patch("src.pipelines.sources.metrics.get_client") as mock_get_client,
            patch("src.pipelines.sources.metrics.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = [_http_error(404), [{"playId": 99}]]

            results = list(win_probability_resource(game_ids=[111, 222]))

            assert len(results) == 1
            assert results[0]["game_id"] == 222

    def test_other_status_errors_are_not_swallowed(self):
        """Non-400/404 errors must propagate, not be skipped. Iterating a
        @dlt.resource-wrapped generator surfaces the original exception as
        __cause__ of dlt's ResourceExtractionError rather than raising it
        bare -- assert on the cause to test the resource's own behavior
        (raise, don't swallow) without coupling to dlt's wrapper type."""
        from dlt.extract.exceptions import ResourceExtractionError

        from src.pipelines.sources.metrics import win_probability_resource

        with (
            patch("src.pipelines.sources.metrics.get_client") as mock_get_client,
            patch("src.pipelines.sources.metrics.make_request") as mock_make_request,
        ):
            mock_get_client.return_value = MagicMock()
            mock_make_request.side_effect = _http_error(500)

            with pytest.raises(ResourceExtractionError) as exc_info:
                list(win_probability_resource(game_ids=[111]))

            assert isinstance(exc_info.value.__cause__, httpx.HTTPStatusError)
            assert exc_info.value.__cause__.response.status_code == 500

    def test_merge_write_disposition_and_compound_primary_key(self):
        """(game_id, play_id), not bare play_id -- CFBD's playId uniqueness
        scope (global vs. per-game) is unconfirmed; see the comment above
        win_probability_resource and scripts/probe_metrics_wp.py."""
        from src.pipelines.sources.metrics import win_probability_resource

        with patch("src.pipelines.sources.metrics.get_client") as mock_get_client:
            mock_get_client.return_value = MagicMock()

            resource = win_probability_resource(game_ids=[401628455])

            assert resource.write_disposition == "merge"
            schema = resource.compute_table_schema()
            pk_columns = {name for name, col in schema["columns"].items() if col.get("primary_key")}
            assert pk_columns == {"game_id", "play_id"}

    def test_metrics_wp_source_returns_win_probability_resource(self):
        from src.pipelines.sources.metrics import metrics_wp_source

        with patch("src.pipelines.sources.metrics.get_client") as mock_get_client:
            mock_get_client.return_value = MagicMock()

            source = metrics_wp_source(game_ids=[401628455])

            assert set(source.resources.keys()) == {"win_probability"}

    def test_metrics_source_no_longer_returns_win_probability(self):
        """Regression guard: the dead year-driven win_probability resource
        must stay out of metrics_source's default list (it always 400'd)."""
        from src.pipelines.sources.metrics import metrics_source

        with patch("src.pipelines.sources.metrics.get_client") as mock_get_client:
            mock_get_client.return_value = MagicMock()

            source = metrics_source(years=[2024])

            assert "win_probability" not in set(source.resources.keys())


# ---------------------------------------------------------------------------
# run_metrics_wp_pipeline (src/pipelines/run.py)
# ---------------------------------------------------------------------------


def _mock_conn(candidate_rows, existing_rows=None, existing_raises=None):
    """Build a MagicMock psycopg2 connection matching run_metrics_wp_pipeline's
    `with conn.cursor() as cur: cur.execute(...); cur.fetchall()` usage, with
    the games query returning `candidate_rows` and the existing-ids query
    either returning `existing_rows` or raising `existing_raises`."""
    conn = MagicMock()
    cur = conn.cursor.return_value.__enter__.return_value
    if existing_raises is not None:
        cur.fetchall.side_effect = [candidate_rows, existing_raises]
    else:
        cur.fetchall.side_effect = [candidate_rows, existing_rows or []]
        cur.execute.side_effect = None
    return conn


class TestRunMetricsWpPipelineBatching:
    def test_chunks_missing_games_into_batch_size_groups(self):
        """120 missing games at batch_size=50 -> 3 pipeline.run() calls
        (50, 50, 20), mirroring run_game_stats_weekly's proven batching."""
        from src.pipelines.run import run_metrics_wp_pipeline

        candidate_rows = [(i, 2024) for i in range(1, 121)]
        conn = _mock_conn(candidate_rows, existing_rows=[])

        mock_pipeline = MagicMock()
        mock_pipeline.run.return_value = "load-info"

        with (
            patch("src.pipelines.run._metrics_wp_db_url", return_value="postgres://fake"),
            patch("psycopg2.connect", return_value=conn),
            patch("src.pipelines.run.dlt.pipeline", return_value=mock_pipeline),
            patch("src.pipelines.run.metrics_wp_source") as mock_source,
        ):
            result = run_metrics_wp_pipeline(seasons=[2024], batch_size=50)

        assert result["missing"] == 120
        assert result["batches"] == 3
        assert mock_pipeline.run.call_count == 3
        batch_sizes = [len(call.kwargs["game_ids"]) for call in mock_source.call_args_list]
        assert batch_sizes == [50, 50, 20]

    def test_already_loaded_games_are_excluded(self):
        from src.pipelines.run import run_metrics_wp_pipeline

        candidate_rows = [(1, 2024), (2, 2024), (3, 2024)]
        conn = _mock_conn(candidate_rows, existing_rows=[(1,), (2,)])

        mock_pipeline = MagicMock()

        with (
            patch("src.pipelines.run._metrics_wp_db_url", return_value="postgres://fake"),
            patch("psycopg2.connect", return_value=conn),
            patch("src.pipelines.run.dlt.pipeline", return_value=mock_pipeline),
            patch("src.pipelines.run.metrics_wp_source") as mock_source,
        ):
            result = run_metrics_wp_pipeline(seasons=[2024], batch_size=50)

        assert result["candidates"] == 3
        assert result["missing"] == 1
        mock_source.assert_called_once()
        assert mock_source.call_args.kwargs["game_ids"] == [3]

    def test_undefined_table_on_fresh_backfill_treated_as_empty(self):
        """metrics.win_probability doesn't exist until the first successful
        load creates it (dlt table-on-first-write). A fresh backfill must
        treat that as 'nothing loaded yet', not crash."""
        import psycopg2.errors

        from src.pipelines.run import run_metrics_wp_pipeline

        candidate_rows = [(1, 2014), (2, 2014)]
        conn = _mock_conn(candidate_rows, existing_raises=psycopg2.errors.UndefinedTable())

        mock_pipeline = MagicMock()

        with (
            patch("src.pipelines.run._metrics_wp_db_url", return_value="postgres://fake"),
            patch("psycopg2.connect", return_value=conn),
            patch("src.pipelines.run.dlt.pipeline", return_value=mock_pipeline),
            patch("src.pipelines.run.metrics_wp_source") as mock_source,
        ):
            result = run_metrics_wp_pipeline(seasons=[2014], batch_size=50)

        assert result["missing"] == 2
        mock_source.assert_called_once()

    def test_no_missing_games_skips_pipeline_run(self):
        from src.pipelines.run import run_metrics_wp_pipeline

        candidate_rows = [(1, 2024)]
        conn = _mock_conn(candidate_rows, existing_rows=[(1,)])

        mock_pipeline = MagicMock()

        with (
            patch("src.pipelines.run._metrics_wp_db_url", return_value="postgres://fake"),
            patch("psycopg2.connect", return_value=conn),
            patch("src.pipelines.run.dlt.pipeline", return_value=mock_pipeline),
        ):
            result = run_metrics_wp_pipeline(seasons=[2024], batch_size=50)

        assert result["missing"] == 0
        assert result["batches"] == 0
        mock_pipeline.run.assert_not_called()


class TestRunMetricsWpPipelineBudgetGuard:
    def test_insufficient_budget_refuses_without_running_pipeline(self):
        from src.pipelines.run import run_metrics_wp_pipeline

        candidate_rows = [(i, 2024) for i in range(1, 11)]
        conn = _mock_conn(candidate_rows, existing_rows=[])

        mock_rate_limiter = MagicMock()
        mock_rate_limiter.check_budget.return_value = False
        mock_rate_limiter.remaining = 3

        mock_pipeline = MagicMock()

        with (
            patch("src.pipelines.run._metrics_wp_db_url", return_value="postgres://fake"),
            patch("psycopg2.connect", return_value=conn),
            patch("src.pipelines.run.get_rate_limiter", return_value=mock_rate_limiter),
            patch("src.pipelines.run.dlt.pipeline", return_value=mock_pipeline),
        ):
            result = run_metrics_wp_pipeline(seasons=[2024], batch_size=50)

        mock_rate_limiter.check_budget.assert_called_once_with(10)
        mock_pipeline.run.assert_not_called()
        assert result["batches"] == 0
        assert "error" in result

    def test_sufficient_budget_proceeds(self):
        from src.pipelines.run import run_metrics_wp_pipeline

        candidate_rows = [(1, 2024)]
        conn = _mock_conn(candidate_rows, existing_rows=[])

        mock_rate_limiter = MagicMock()
        mock_rate_limiter.check_budget.return_value = True

        mock_pipeline = MagicMock()

        with (
            patch("src.pipelines.run._metrics_wp_db_url", return_value="postgres://fake"),
            patch("psycopg2.connect", return_value=conn),
            patch("src.pipelines.run.get_rate_limiter", return_value=mock_rate_limiter),
            patch("src.pipelines.run.dlt.pipeline", return_value=mock_pipeline),
            patch("src.pipelines.run.metrics_wp_source"),
        ):
            result = run_metrics_wp_pipeline(seasons=[2024], batch_size=50)

        assert result["batches"] == 1
        mock_pipeline.run.assert_called_once()
