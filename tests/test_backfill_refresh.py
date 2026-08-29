"""Unit tests for scripts/backfill_refresh.py's pure helpers (no DB, no network).

Covers backlog resolution (ordering + set-difference against the ledger),
cross-task call allocation, the month-guard math, campaign-complete
detection, season/task spec parsing, the ledger-insert idempotency shape,
and the pipeline-identity guard that keeps game-id-driven writes merging
into the SAME tables the normal `stats` source writes.
"""

import inspect
import math

import pytest

from scripts.backfill_refresh import (
    STATS_DATASET_NAME,
    STATS_PIPELINE_NAME,
    TASK_ORDER,
    TASK_RESOURCES,
    allocate_calls,
    build_ledger_values,
    compute_task_backlog,
    eta_run_days,
    is_campaign_complete,
    parse_seasons,
    parse_tasks,
    plan_batches,
    resolve_task_order,
    sort_games_newest_first,
    would_exceed_monthly_cap,
)

# ---------------------------------------------------------------------------
# parse_seasons / parse_tasks
# ---------------------------------------------------------------------------


class TestParseSeasons:
    def test_range(self):
        assert parse_seasons("2014-2025") == list(range(2014, 2026))

    def test_single_year_range(self):
        assert parse_seasons("2020-2020") == [2020]

    def test_explicit_list(self):
        assert parse_seasons("2014,2016,2018") == [2014, 2016, 2018]

    def test_list_tolerates_whitespace(self):
        assert parse_seasons(" 2014, 2016 ") == [2014, 2016]

    def test_bare_year(self):
        assert parse_seasons("2020") == [2020]

    def test_empty_raises(self):
        with pytest.raises(ValueError, match="must not be empty"):
            parse_seasons("")

    def test_backwards_range_raises(self):
        with pytest.raises(ValueError, match="end before start"):
            parse_seasons("2025-2014")


class TestParseTasks:
    def test_known_tasks(self):
        assert parse_tasks("plays_stats,box_advanced") == ["plays_stats", "box_advanced"]

    def test_single_task(self):
        assert parse_tasks("plays_stats") == ["plays_stats"]

    def test_whitespace_tolerant(self):
        assert parse_tasks(" plays_stats , box_advanced ") == ["plays_stats", "box_advanced"]

    def test_unknown_task_raises(self):
        with pytest.raises(ValueError, match="Unknown task"):
            parse_tasks("plays_stats,not_a_task")


class TestResolveTaskOrder:
    def test_reorders_to_canonical_sequence(self):
        """Operator typed box_advanced first -- plays_stats must still drain
        first, per TASK_ORDER."""
        assert resolve_task_order(["box_advanced", "plays_stats"]) == [
            "plays_stats",
            "box_advanced",
        ]

    def test_subset(self):
        assert resolve_task_order(["box_advanced"]) == ["box_advanced"]
        assert resolve_task_order(["plays_stats"]) == ["plays_stats"]


# ---------------------------------------------------------------------------
# Backlog resolution: ordering + set-difference against the ledger
# ---------------------------------------------------------------------------


class TestSortGamesNewestFirst:
    def test_newest_season_first(self):
        games = [(1, 2020, 1), (2, 2024, 1), (3, 2022, 1)]
        ordered = sort_games_newest_first(games)
        assert [g[1] for g in ordered] == [2024, 2022, 2020]

    def test_newest_week_within_season(self):
        games = [(1, 2024, 3), (2, 2024, 10), (3, 2024, 1)]
        ordered = sort_games_newest_first(games)
        assert [g[2] for g in ordered] == [10, 3, 1]

    def test_season_beats_week(self):
        """A newer season with an early week must still sort ahead of an
        older season's late week."""
        games = [(1, 2023, 15), (2, 2024, 1)]
        ordered = sort_games_newest_first(games)
        assert ordered[0] == (2, 2024, 1)


class TestComputeTaskBacklog:
    def test_removes_already_done_games(self):
        candidates = [(1, 2024, 1), (2, 2024, 2), (3, 2024, 3)]
        backlog = compute_task_backlog(candidates, done_ids={2})
        assert backlog == [3, 1]  # newest week (3) first, then week 1; week 2 excluded

    def test_empty_done_ids_returns_everything_ordered(self):
        candidates = [(1, 2020, 1), (2, 2024, 1)]
        assert compute_task_backlog(candidates, done_ids=set()) == [2, 1]

    def test_fully_done_returns_empty(self):
        candidates = [(1, 2024, 1), (2, 2024, 2)]
        assert compute_task_backlog(candidates, done_ids={1, 2}) == []

    def test_accepts_unordered_input(self):
        """The candidate list from the DB isn't guaranteed pre-sorted --
        compute_task_backlog must sort it itself."""
        candidates = [(3, 2022, 5), (1, 2024, 1), (2, 2023, 9)]
        assert compute_task_backlog(candidates, done_ids=set()) == [1, 2, 3]


# ---------------------------------------------------------------------------
# Cross-task allocation: plays_stats drains before box_advanced starts
# ---------------------------------------------------------------------------


class TestAllocateCalls:
    def test_first_task_drains_before_second_starts(self):
        backlogs = {"plays_stats": [1, 2, 3, 4, 5], "box_advanced": [10, 11, 12]}
        allocation = allocate_calls(backlogs, TASK_ORDER, max_calls=3)
        assert allocation == {"plays_stats": [1, 2, 3], "box_advanced": []}

    def test_second_task_gets_remainder_after_first_drains(self):
        backlogs = {"plays_stats": [1, 2], "box_advanced": [10, 11, 12]}
        allocation = allocate_calls(backlogs, TASK_ORDER, max_calls=4)
        assert allocation == {"plays_stats": [1, 2], "box_advanced": [10, 11]}

    def test_budget_covers_everything(self):
        backlogs = {"plays_stats": [1, 2], "box_advanced": [10]}
        allocation = allocate_calls(backlogs, TASK_ORDER, max_calls=100)
        assert allocation == {"plays_stats": [1, 2], "box_advanced": [10]}

    def test_zero_budget_allocates_nothing(self):
        backlogs = {"plays_stats": [1, 2], "box_advanced": [10]}
        allocation = allocate_calls(backlogs, TASK_ORDER, max_calls=0)
        assert allocation == {"plays_stats": [], "box_advanced": []}

    def test_respects_requested_task_order_subset(self):
        """box_advanced-only run must not touch plays_stats even if it has
        backlog -- box_advanced isn't in task_order here."""
        backlogs = {"box_advanced": [10, 11, 12]}
        allocation = allocate_calls(backlogs, ["box_advanced"], max_calls=2)
        assert allocation == {"box_advanced": [10, 11]}


class TestPlanBatches:
    def test_chunks_each_task_and_preserves_task_order(self):
        allocation = {"plays_stats": [1, 2, 3, 4, 5], "box_advanced": [10, 11]}
        batches = plan_batches(allocation, TASK_ORDER, batch_size=2)
        assert batches == [
            ("plays_stats", [1, 2]),
            ("plays_stats", [3, 4]),
            ("plays_stats", [5]),
            ("box_advanced", [10, 11]),
        ]

    def test_empty_task_contributes_no_batches(self):
        allocation = {"plays_stats": [], "box_advanced": [10]}
        batches = plan_batches(allocation, TASK_ORDER, batch_size=50)
        assert batches == [("box_advanced", [10])]


# ---------------------------------------------------------------------------
# Month guard math
# ---------------------------------------------------------------------------


class TestWouldExceedMonthlyCap:
    def test_under_cap_is_allowed(self):
        assert would_exceed_monthly_cap(month_spend=100, batch_calls=50, monthly_cap=1000) is False

    def test_exactly_at_cap_is_allowed(self):
        """Landing exactly on the cap is not exceeding it."""
        assert would_exceed_monthly_cap(month_spend=950, batch_calls=50, monthly_cap=1000) is False

    def test_over_cap_is_refused(self):
        assert would_exceed_monthly_cap(month_spend=980, batch_calls=50, monthly_cap=1000) is True

    def test_already_over_cap_refuses_any_further_batch(self):
        assert would_exceed_monthly_cap(month_spend=1000, batch_calls=1, monthly_cap=1000) is True


# ---------------------------------------------------------------------------
# Campaign-complete detection
# ---------------------------------------------------------------------------


class TestIsCampaignComplete:
    def test_all_empty_is_complete(self):
        assert is_campaign_complete({"plays_stats": [], "box_advanced": []}) is True

    def test_any_nonempty_is_not_complete(self):
        assert is_campaign_complete({"plays_stats": [1], "box_advanced": []}) is False

    def test_no_tasks_is_vacuously_complete(self):
        assert is_campaign_complete({}) is True


class TestEtaRunDays:
    def test_zero_remaining_is_zero_runs(self):
        assert eta_run_days(0, 1000) == 0

    def test_exact_multiple(self):
        assert eta_run_days(2000, 1000) == 2

    def test_rounds_up_partial_run(self):
        assert eta_run_days(1500, 1000) == 2

    def test_zero_cap_never_drains(self):
        assert eta_run_days(100, 0) == math.inf


# ---------------------------------------------------------------------------
# Ledger insert idempotency shape
# ---------------------------------------------------------------------------


class TestBuildLedgerValues:
    def test_row_shape(self):
        values = build_ledger_values("camp-1", "plays_stats", [10, 20])
        assert values == [("camp-1", "plays_stats", 10, 1), ("camp-1", "plays_stats", 20, 1)]

    def test_empty_game_ids_yields_empty_values(self):
        assert build_ledger_values("camp-1", "plays_stats", []) == []


def test_ledger_insert_sql_uses_on_conflict_do_nothing():
    """The INSERT this script issues after every successful batch must be
    ON CONFLICT DO NOTHING against the (campaign, task, game_id) primary key
    -- that's what makes replaying an already-recorded batch (e.g. a batch
    that loaded successfully but whose commit was interrupted) a no-op
    instead of a duplicate-key error."""
    from scripts.backfill_refresh import _LEDGER_INSERT_SQL

    assert "ON CONFLICT (campaign, task, game_id) DO NOTHING" in _LEDGER_INSERT_SQL
    assert "INSERT INTO meta.refresh_progress" in _LEDGER_INSERT_SQL


def test_insert_ledger_rows_uses_execute_values_with_on_conflict(monkeypatch):
    """Exercise insert_ledger_rows against a fake cursor/connection to assert
    it calls execute_values with the ON-CONFLICT SQL and the expected row
    tuples, and commits -- without any real DB connection."""
    from scripts.backfill_refresh import insert_ledger_rows

    calls = []

    class FakeCursor:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    class FakeConn:
        def cursor(self):
            return FakeCursor()

        def commit(self):
            calls.append("commit")

    def fake_execute_values(cur, sql, values):
        calls.append(("execute_values", sql, values))

    import psycopg2.extras

    monkeypatch.setattr(psycopg2.extras, "execute_values", fake_execute_values)

    insert_ledger_rows(FakeConn(), "camp-1", "plays_stats", [10, 20])

    assert len(calls) == 2
    tag, sql, values = calls[0]
    assert tag == "execute_values"
    assert "ON CONFLICT (campaign, task, game_id) DO NOTHING" in sql
    assert values == [("camp-1", "plays_stats", 10, 1), ("camp-1", "plays_stats", 20, 1)]
    assert calls[1] == "commit"


def test_insert_ledger_rows_skips_db_entirely_for_empty_batch(monkeypatch):
    """No games -> no cursor touched, no commit -- a defensive no-op rather
    than an empty VALUES () statement."""
    from scripts.backfill_refresh import insert_ledger_rows

    class ExplodingConn:
        def cursor(self):
            raise AssertionError("cursor() should not be called for an empty batch")

        def commit(self):
            raise AssertionError("commit() should not be called for an empty batch")

    insert_ledger_rows(ExplodingConn(), "camp-1", "plays_stats", [])


# ---------------------------------------------------------------------------
# Pipeline-identity guarantee: game-id-driven writes MUST merge into the same
# tables the normal `stats` source writes, never a parallel dlt schema.
# ---------------------------------------------------------------------------


def test_task_resources_are_the_committed_stats_module_resources():
    from src.pipelines.sources.stats import advanced_game_stats_resource, play_stats_resource

    assert TASK_RESOURCES == {
        "plays_stats": play_stats_resource,
        "box_advanced": advanced_game_stats_resource,
    }


def test_pipeline_identity_matches_run_stats_pipeline():
    """Guards the single most important correctness property of this
    script: a mismatched pipeline_name/dataset_name would silently stand up
    a PARALLEL dlt schema instead of merging into stats.play_stats /
    stats.advanced_game_stats. Reads run_stats_pipeline's source directly
    (no DB, no dlt.pipeline() call) so this test needs neither a database
    nor network access.
    """
    from src.pipelines.run import run_stats_pipeline

    src = inspect.getsource(run_stats_pipeline)
    assert f'pipeline_name="{STATS_PIPELINE_NAME}"' in src
    assert f'dataset_name="{STATS_DATASET_NAME}"' in src


def test_resource_functions_accept_explicit_game_ids():
    """Both task resources must be callable with only `game_ids=` (the mode
    this script always uses) -- a signature check that would catch either
    resource losing its game-id-driven branch."""
    for resource_fn in TASK_RESOURCES.values():
        sig = inspect.signature(resource_fn)
        assert "game_ids" in sig.parameters
