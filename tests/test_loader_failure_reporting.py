"""Exercise real dlt resource extraction through offline orchestration fakes."""

import json
from unittest.mock import Mock

import httpx
import pytest

from scripts import load_season as season_loader
from src.pipelines import run
from src.pipelines.sources import game_stats, rosters


class ExtractingPipeline:
    """Run dlt resource iterators, without normalizing or writing a database."""

    def __init__(self):
        self.completed = []
        self.failures = []

    def run(self, source):
        rows = []
        try:
            for resource in source.selected_resources.values():
                rows.extend(list(resource))
        except Exception as error:
            self.failures.append(error)
            raise RuntimeError("Pipeline extraction failed") from error
        self.completed.append(rows)
        return "extraction completed (test fake; no database writes)"


@pytest.fixture
def runtime(monkeypatch):
    pipeline = ExtractingPipeline()
    monkeypatch.setattr(run.dlt, "pipeline", Mock(return_value=pipeline))
    limiter = Mock()
    limiter.check_budget.return_value = True
    limiter.get_status.return_value = {"remaining": 100_000}
    monkeypatch.setattr(run, "get_rate_limiter", lambda: limiter)
    monkeypatch.setattr("src.pipelines.utils.rate_limiter.get_rate_limiter", lambda: limiter)
    monkeypatch.setattr(run, "show_status", Mock())
    monkeypatch.setattr(game_stats, "get_client", Mock())
    monkeypatch.setattr(rosters, "get_client", Mock())
    return pipeline


def failing_request(monkeypatch, source, *, year=None):
    """One good request, then a 401; a third request must never be attempted."""
    error = httpx.HTTPStatusError(
        "Unauthorized",
        request=httpx.Request("GET", "https://example.invalid/cfbd"),
        response=httpx.Response(401),
    )

    def request(client, endpoint, params):
        should_fail = (
            params["year"] == year
            if year is not None
            else params.get("week") == 2 or params.get("team") == "Georgia"
        )
        if should_fail:
            raise error
        return [{"id": 123, "teams": []}]

    fake = Mock(side_effect=request)
    monkeypatch.setattr(source, "make_request", fake)
    return fake


@pytest.mark.parametrize("source_name", ["game_stats", "rosters"])
def test_season_summary_keeps_failure_context_after_dlt_wraps_it(
    source_name, runtime, monkeypatch, capsys
):
    source = game_stats if source_name == "game_stats" else rosters
    request = failing_request(monkeypatch, source)
    if source_name == "rosters":
        actual_runner = run.run_rosters_pipeline
        monkeypatch.setattr(
            run,
            "run_rosters_pipeline",
            lambda years: actual_runner(teams=["Alabama", "Georgia", "Texas"], years=years),
        )

    summary = season_loader.load_season(2026, sources=[source_name], skip_refresh=True)

    assert summary["successes"] == 0
    assert summary["errors"] == 1
    result = summary["results"][source_name]
    assert result["status"] == "error"
    receipt = result["request_failure"]
    assert receipt["outcome"] == "failed"
    assert receipt["error_type"] == "HTTPStatusError"
    assert receipt["params"]["year"] == 2026
    assert receipt["counts"] == {
        "succeeded": 1,
        "expected_no_data": 0,
        "failed": 1,
        "deferred": 18 if source_name == "game_stats" else 1,
    }
    assert receipt["endpoint"] == ("/games/teams" if source_name == "game_stats" else "/roster")
    json.dumps(summary)  # The full summary remains suitable for a run artifact.
    assert request.call_count == 2
    assert runtime.completed == []
    assert "ResourceExtractionError" == type(runtime.failures[0]).__name__
    assert "[FAIL]" in capsys.readouterr().out


def test_season_continues_independent_sources_but_exits_nonzero(runtime, monkeypatch, capsys):
    failing_request(monkeypatch, game_stats)
    ratings = Mock(return_value="ratings completed")
    monkeypatch.setattr(run, "run_ratings_pipeline", ratings)
    monkeypatch.setattr(
        "sys.argv",
        ["load_season.py", "--season", "2026", "--sources", "game_stats,ratings", "--skip-refresh"],
    )

    with pytest.raises(SystemExit) as exit_info:
        season_loader.main()

    assert exit_info.value.code == 1
    ratings.assert_called_once_with(years=[2026])
    output = capsys.readouterr().out
    assert "1 succeeded, 1 failed" in output
    assert "[FAIL] game_stats" in output


@pytest.mark.parametrize("weekly", [False, True])
def test_pipeline_cli_returns_failure_with_request_context(runtime, monkeypatch, capsys, weekly):
    failing_request(monkeypatch, game_stats)
    args = ["cfb-pipeline", "--source", "game_stats", "--years", "2026"]
    if weekly:
        args.append("--weekly")
    monkeypatch.setattr("sys.argv", args)

    with pytest.raises(SystemExit) as exit_info:
        run.main()

    assert exit_info.value.code == 1
    output = capsys.readouterr().out
    assert "ERROR in game_stats" in output
    assert '"endpoint": "/games/teams"' in output
    assert '"week": 2' in output
    if weekly:
        # Week one finished both resources. Week two still fails the whole command.
        assert len(runtime.completed) == 1
        assert "Weekly loading complete" not in output


def test_all_sources_cli_carries_one_resource_failure_to_exit(runtime, monkeypatch, capsys):
    failing_request(monkeypatch, game_stats)
    other_runners = []
    for name in list(vars(run)):
        if name.startswith("run_") and name not in {
            "run_game_stats_pipeline",
            "run_game_stats_weekly",
        }:
            fake = Mock(return_value="independent source completed")
            monkeypatch.setattr(run, name, fake)
            other_runners.append(fake)
    monkeypatch.setattr("sys.argv", ["cfb-pipeline", "--source", "all", "--years", "2026"])

    with pytest.raises(SystemExit) as exit_info:
        run.main()

    assert exit_info.value.code == 1
    assert all(fake.called for fake in other_runners)
    output = capsys.readouterr().out
    assert "sources failed: game_stats" in output
    assert '"outcome": "failed"' in output


def test_batched_cli_cannot_hide_failure_after_an_earlier_batch(runtime, monkeypatch, capsys):
    failing_request(monkeypatch, game_stats, year=2025)
    monkeypatch.setattr(
        "sys.argv",
        ["cfb-pipeline", "--source", "game_stats", "--years", "2024", "2025", "--batch-size", "1"],
    )

    with pytest.raises(SystemExit) as exit_info:
        run.main()

    assert exit_info.value.code == 1
    assert len(runtime.completed) == 1
    output = capsys.readouterr().out
    assert '"year": 2025' in output
    assert "All 2 batches complete" not in output


def test_empty_responses_remain_successful_through_season_cli(runtime, monkeypatch, capsys):
    empty = Mock(return_value=[])
    monkeypatch.setattr(game_stats, "make_request", empty)
    monkeypatch.setattr(
        "sys.argv",
        ["load_season.py", "--season", "2026", "--sources", "game_stats", "--skip-refresh"],
    )

    assert season_loader.main() is None

    assert len(runtime.completed) == 1
    assert empty.call_count == 40  # Two resources, each with 15 regular + 5 postseason weeks.
    assert "1 succeeded, 0 failed" in capsys.readouterr().out


def test_unrelated_runner_errors_keep_existing_summary_shape(runtime, monkeypatch):
    monkeypatch.setattr(
        run, "run_ratings_pipeline", Mock(side_effect=ValueError("bad configuration"))
    )

    summary = season_loader.load_season(2026, sources=["ratings"], skip_refresh=True)

    result = summary["results"]["ratings"]
    assert result["status"] == "error"
    assert result["error"] == "bad configuration"
    assert "request_failure" not in result
    assert summary["errors"] == 1
