"""Unit tests for scripts/backfill_refresh.py's pure helpers (no DB, no network).

Covers backlog resolution (ordering + set-difference against the ledger),
cross-task call allocation, the month-guard math, campaign-complete
detection, season/task spec parsing, the ledger-insert idempotency shape,
the pipeline-identity guard that keeps game-id-driven writes merging into
the SAME tables the normal `stats` source writes, and the PR #75 R1
additions: per-game miss tracking (F6), finalize semantics (F7/F1), and the
should_mark_complete/fetch_unfinalized/--requeue-no-data machinery that
backs them.
"""

import inspect
import math
from datetime import UTC, datetime

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
    fetch_last_refreshed_game_id,
    finalize_campaign,
    is_campaign_complete,
    parse_seasons,
    parse_tasks,
    plan_batches,
    report_partial_bad_data,
    resolve_task_order,
    run_batch,
    should_mark_complete,
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
        assert values == [
            ("camp-1", "plays_stats", 10, 1, "refreshed"),
            ("camp-1", "plays_stats", 20, 1, "refreshed"),
        ]

    def test_empty_game_ids_yields_empty_values(self):
        assert build_ledger_values("camp-1", "plays_stats", []) == []

    def test_empty_misses_are_all_refreshed(self):
        """Explicit empty misses behaves identically to the default."""
        values = build_ledger_values("camp-1", "plays_stats", [10, 20], misses=[])
        assert all(v[4] == "refreshed" for v in values)

    def test_mixed_statuses(self):
        """R1/F6: a game_id in `misses` gets status='no_data'; everything
        else in the batch is 'refreshed' -- both still land with calls=1
        (a suppressed 400 or empty response still spends the call)."""
        values = build_ledger_values("camp-1", "plays_stats", [10, 20, 30], misses=[20])
        assert values == [
            ("camp-1", "plays_stats", 10, 1, "refreshed"),
            ("camp-1", "plays_stats", 20, 1, "no_data"),
            ("camp-1", "plays_stats", 30, 1, "refreshed"),
        ]

    def test_miss_not_in_game_ids_is_ignored(self):
        """A miss id outside this batch's game_ids must not fabricate a row."""
        values = build_ledger_values("camp-1", "plays_stats", [10], misses=[999])
        assert values == [("camp-1", "plays_stats", 10, 1, "refreshed")]


def test_ledger_insert_sql_uses_on_conflict_do_nothing():
    """The INSERT this script issues after every successful batch must be
    ON CONFLICT DO NOTHING against the (campaign, task, game_id) primary key
    -- that's what makes replaying an already-recorded batch (e.g. a batch
    that loaded successfully but whose commit was interrupted) a no-op
    instead of a duplicate-key error."""
    from scripts.backfill_refresh import _LEDGER_INSERT_SQL

    assert "ON CONFLICT (campaign, task, game_id) DO NOTHING" in _LEDGER_INSERT_SQL
    assert "INSERT INTO meta.refresh_progress" in _LEDGER_INSERT_SQL
    assert "status" in _LEDGER_INSERT_SQL


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

    insert_ledger_rows(FakeConn(), "camp-1", "plays_stats", [10, 20], misses=[20])

    assert len(calls) == 2
    tag, sql, values = calls[0]
    assert tag == "execute_values"
    assert "ON CONFLICT (campaign, task, game_id) DO NOTHING" in sql
    assert values == [
        ("camp-1", "plays_stats", 10, 1, "refreshed"),
        ("camp-1", "plays_stats", 20, 1, "no_data"),
    ]
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


# ---------------------------------------------------------------------------
# R1 (PR #75 F6): run_batch forwards a `misses` list into the resource and
# returns it back to the caller.
# ---------------------------------------------------------------------------


class TestRunBatch:
    def test_returns_info_and_forwards_misses(self, monkeypatch):
        """run_batch must (1) create a fresh `misses` list, (2) pass it as
        `misses=` to the task's resource function, and (3) return
        (load_info, misses, report) so the caller can route it into
        insert_ledger_rows."""
        from scripts import backfill_refresh

        captured_kwargs = {}

        def fake_resource(game_ids, *, misses=None, report=None):
            captured_kwargs["game_ids"] = game_ids
            captured_kwargs["misses"] = misses
            captured_kwargs["report"] = report
            if misses is not None:
                misses.append(game_ids[0])  # simulate one suppressed 400
            return iter(["row"])

        class FakePipeline:
            def run(self, source):
                # Consume the resource the same way pipeline.run() would.
                return {"loaded": list(source)}

        monkeypatch.setattr(backfill_refresh, "TASK_RESOURCES", {"plays_stats": fake_resource})

        info, misses, report = run_batch(FakePipeline(), "plays_stats", [10, 20])

        assert captured_kwargs["game_ids"] == [10, 20]
        assert captured_kwargs["misses"] is misses
        assert captured_kwargs["report"] is None
        assert report is None
        assert misses == [10]
        assert info == {"loaded": ["row"]}

    def test_box_advanced_forwards_fallback_probe_game_id(self, monkeypatch):
        from scripts import backfill_refresh
        from src.pipelines.sources.stats import AdvancedBoxBatchReport

        captured = {}

        def fake_resource(game_ids, *, misses=None, report=None):
            captured["report"] = report
            return iter([])

        class FakePipeline:
            def run(self, source):
                list(source)
                return {"loaded": []}

        monkeypatch.setattr(backfill_refresh, "TASK_RESOURCES", {"box_advanced": fake_resource})

        _info, _misses, report = run_batch(
            FakePipeline(), "box_advanced", [1, 2], fallback_probe_game_id=401836219
        )

        assert isinstance(report, AdvancedBoxBatchReport)
        assert report.fallback_probe_game_id == 401836219
        assert captured["report"] is report


# ---------------------------------------------------------------------------
# R1 (PR #75 F7): fetch_unfinalized / finalize_campaign / should_mark_complete
# ---------------------------------------------------------------------------


class _FakeCursor:
    def __init__(self, rows):
        self._rows = rows
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None


class _FakeConn:
    def __init__(self, rows):
        self._rows = rows
        self.commits = 0
        self.cursors = []

    def cursor(self):
        cur = _FakeCursor(self._rows)
        self.cursors.append(cur)
        return cur

    def commit(self):
        self.commits += 1

    def close(self):
        return None


class TestUnfinalizedSeasonsQueryShape:
    def test_contains_status_refreshed_and_coalesce_negative_infinity(self):
        """Pins the two clauses that make this query correct: only
        status='refreshed' rows count toward "unfinalized" (a 'no_data' row
        never triggers a refit), and a NULL last_finalized_at (never
        finalized) reads as '-infinity' so a fresh campaign's entire history
        is picked up rather than silently excluded by a NULL comparison."""
        from scripts.backfill_refresh import _UNFINALIZED_SEASONS_QUERY

        assert "status = 'refreshed'" in _UNFINALIZED_SEASONS_QUERY
        assert "'-infinity'" in _UNFINALIZED_SEASONS_QUERY
        assert "last_finalized_at" in _UNFINALIZED_SEASONS_QUERY


class TestFetchUnfinalized:
    def test_rows_yield_seasons_and_max_watermark(self):
        from scripts.backfill_refresh import fetch_unfinalized

        t1 = datetime(2026, 8, 1, tzinfo=UTC)
        t2 = datetime(2026, 8, 15, tzinfo=UTC)
        conn = _FakeConn(rows=[(2024, t1), (2025, t2)])

        seasons, watermark = fetch_unfinalized(conn, "camp-1")

        assert seasons == [2024, 2025]
        assert watermark == t2

    def test_empty_is_none_watermark(self):
        from scripts.backfill_refresh import fetch_unfinalized

        conn = _FakeConn(rows=[])

        seasons, watermark = fetch_unfinalized(conn, "camp-1")

        assert seasons == []
        assert watermark is None


class TestFinalizeCampaign:
    def test_all_zero_advances_watermark_and_commits(self, monkeypatch):
        from scripts import compute_adjusted_epa, compute_adjusted_epa_week, refresh_marts

        monkeypatch.setattr(compute_adjusted_epa, "compute_seasons", lambda seasons: 0)
        monkeypatch.setattr(compute_adjusted_epa_week, "compute_seasons", lambda seasons: 0)
        monkeypatch.setattr(refresh_marts, "refresh_marts", lambda **kwargs: 0)

        conn = _FakeConn(rows=[])
        watermark = datetime(2026, 8, 15, tzinfo=UTC)

        result = finalize_campaign(conn, "camp-1", [2024, 2025], watermark)

        assert result is True
        assert conn.commits == 1
        sql, params = conn.cursors[0].executed[0]
        assert "UPDATE meta.refresh_campaigns" in sql
        assert "last_finalized_at" in sql
        assert params == (watermark, "camp-1")

    def test_any_nonzero_failure_skips_update_and_returns_false(self, monkeypatch):
        from scripts import compute_adjusted_epa, compute_adjusted_epa_week, refresh_marts

        monkeypatch.setattr(compute_adjusted_epa, "compute_seasons", lambda seasons: 0)
        monkeypatch.setattr(compute_adjusted_epa_week, "compute_seasons", lambda seasons: 1)
        monkeypatch.setattr(refresh_marts, "refresh_marts", lambda **kwargs: 0)

        conn = _FakeConn(rows=[])
        watermark = datetime(2026, 8, 15, tzinfo=UTC)

        result = finalize_campaign(conn, "camp-1", [2024], watermark)

        assert result is False
        assert conn.commits == 0
        assert conn.cursors == []

    def test_mart_refresh_failure_also_fails(self, monkeypatch):
        from scripts import compute_adjusted_epa, compute_adjusted_epa_week, refresh_marts

        monkeypatch.setattr(compute_adjusted_epa, "compute_seasons", lambda seasons: 0)
        monkeypatch.setattr(compute_adjusted_epa_week, "compute_seasons", lambda seasons: 0)
        monkeypatch.setattr(refresh_marts, "refresh_marts", lambda **kwargs: 2)

        conn = _FakeConn(rows=[])

        result = finalize_campaign(conn, "camp-1", [2024], datetime.now(tz=UTC))

        assert result is False
        assert conn.commits == 0


class TestShouldMarkComplete:
    def test_empty_backlog_and_no_unfinalized_is_complete(self):
        assert should_mark_complete({"plays_stats": [], "box_advanced": []}, []) is True

    def test_empty_backlog_but_unfinalized_seasons_is_not_complete(self):
        """F7: an empty per-game backlog alone must NOT be enough to
        complete a campaign while a finalize is still outstanding."""
        assert should_mark_complete({"plays_stats": [], "box_advanced": []}, [2025]) is False

    def test_nonempty_backlog_is_not_complete_regardless_of_finalize(self):
        assert should_mark_complete({"plays_stats": [1], "box_advanced": []}, []) is False


# ---------------------------------------------------------------------------
# R1: --requeue-no-data
# ---------------------------------------------------------------------------


class TestRequeueNoData:
    def test_without_task_filter(self):
        from scripts.backfill_refresh import requeue_no_data

        conn = _FakeConn(rows=[])
        conn._rows = []

        class _CountingCursor(_FakeCursor):
            rowcount = 7

        conn.cursor = lambda: _CountingCursor([])

        rowcount = requeue_no_data(conn, "camp-1")

        assert rowcount == 7
        assert conn.commits == 1

    def test_with_task_filter_uses_the_scoped_sql(self):
        from scripts.backfill_refresh import (
            _REQUEUE_NO_DATA_SQL_WITH_TASKS,
            requeue_no_data,
        )

        captured = {}

        class _Cursor(_FakeCursor):
            rowcount = 3

            def execute(self, sql, params=None):
                captured["sql"] = sql
                captured["params"] = params

        conn = _FakeConn(rows=[])
        conn.cursor = lambda: _Cursor([])

        rowcount = requeue_no_data(conn, "camp-1", tasks=["box_advanced"])

        assert rowcount == 3
        assert captured["sql"] == _REQUEUE_NO_DATA_SQL_WITH_TASKS
        assert captured["params"] == ("camp-1", ["box_advanced"])

    def test_sql_predicates_scope_to_campaign_and_no_data_status(self):
        from scripts.backfill_refresh import _REQUEUE_NO_DATA_SQL, _REQUEUE_NO_DATA_SQL_WITH_TASKS

        assert "DELETE FROM meta.refresh_progress" in _REQUEUE_NO_DATA_SQL
        assert "status = 'no_data'" in _REQUEUE_NO_DATA_SQL
        assert "campaign = %s" in _REQUEUE_NO_DATA_SQL
        assert _REQUEUE_NO_DATA_SQL_WITH_TASKS.startswith(_REQUEUE_NO_DATA_SQL)
        assert "task = ANY(%s)" in _REQUEUE_NO_DATA_SQL_WITH_TASKS


class TestFetchLastRefreshedGameId:
    def test_query_shape_and_returns_int(self):
        from scripts.backfill_refresh import _LAST_REFRESHED_GAME_QUERY

        assert "status = 'refreshed'" in _LAST_REFRESHED_GAME_QUERY
        assert "ORDER BY refreshed_at DESC" in _LAST_REFRESHED_GAME_QUERY
        assert "LIMIT 1" in _LAST_REFRESHED_GAME_QUERY

        conn = _FakeConn(rows=[(401836219,)])
        gid = fetch_last_refreshed_game_id(conn, "camp-1", "box_advanced")
        assert gid == 401836219
        sql, params = conn.cursors[0].executed[0]
        assert params == ("camp-1", "box_advanced")

    def test_empty_returns_none(self):
        conn = _FakeConn(rows=[])
        assert fetch_last_refreshed_game_id(conn, "camp-1", "box_advanced") is None


class TestReportPartialBadData:
    def test_writes_warning_output_and_summary(self, tmp_path, monkeypatch, capsys):
        out_file = tmp_path / "github_output"
        summary_file = tmp_path / "summary.md"
        monkeypatch.setenv("GITHUB_OUTPUT", str(out_file))
        monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary_file))

        report_partial_bad_data({"status": "ok", "bad_data_game_ids": [10, 20]})

        captured = capsys.readouterr()
        assert "::warning::" in captured.out
        assert "10,20" in captured.out
        assert out_file.read_text(encoding="utf-8") == (
            "partial_bad_data=true\nbad_data_count=2\nbad_data_game_ids=10,20\n"
        )
        assert "Partial bad data" in summary_file.read_text(encoding="utf-8")

    def test_silent_when_ids_empty_or_exit_one(self, tmp_path, monkeypatch, capsys):
        out_file = tmp_path / "github_output"
        monkeypatch.setenv("GITHUB_OUTPUT", str(out_file))

        report_partial_bad_data({"status": "ok", "bad_data_game_ids": []})
        report_partial_bad_data({"status": "outage", "bad_data_game_ids": [1]})

        assert capsys.readouterr().out == ""
        assert not out_file.exists()


class TestRunCampaignOutageAndBadData:
    def test_outage_mid_batch_ledgers_attempted_prefix(self, monkeypatch):
        from scripts import backfill_refresh

        ledger_calls = []

        def fake_run_batch(pipeline, task, game_ids, *, fallback_probe_game_id=None):
            from src.pipelines.sources.stats import AdvancedBoxBatchReport

            if task == "box_advanced":
                report = AdvancedBoxBatchReport(
                    attempted=game_ids[:2],
                    server_error_game_ids=[game_ids[1]],
                    outage="CFBD down",
                )
                return {"ok": True}, [game_ids[1]], report
            return {"ok": True}, [], None

        monkeypatch.setattr(backfill_refresh, "run_batch", fake_run_batch)
        monkeypatch.setattr(
            backfill_refresh,
            "insert_ledger_rows",
            lambda conn, campaign, task, game_ids, misses=(): ledger_calls.append(
                (task, list(game_ids), list(misses))
            ),
        )
        monkeypatch.setattr(backfill_refresh, "get_month_spend", lambda conn: 0)
        monkeypatch.setattr(backfill_refresh, "fetch_last_refreshed_game_id", lambda *a: None)
        monkeypatch.setattr(
            backfill_refresh, "fetch_unfinalized", lambda conn, campaign: ([], None)
        )

        class Limiter:
            def check_budget(self, n):
                return True

        monkeypatch.setattr("src.pipelines.utils.rate_limiter.get_rate_limiter", lambda: Limiter())

        row = {
            "campaign": "camp-1",
            "description": None,
            "seasons": [2024],
            "tasks": ["box_advanced"],
            "created_at": None,
            "completed_at": None,
            "last_finalized_at": None,
        }
        monkeypatch.setattr(backfill_refresh, "get_or_create_campaign", lambda *a, **k: row)
        monkeypatch.setattr(
            backfill_refresh,
            "fetch_candidate_games",
            lambda conn, seasons: [(1, 2024, 1), (2, 2024, 1), (3, 2024, 1)],
        )
        monkeypatch.setattr(backfill_refresh, "fetch_done_ids", lambda conn, campaign, task: set())
        monkeypatch.setattr(backfill_refresh, "build_stats_pipeline", lambda: object())

        result = backfill_refresh.run_campaign(
            object(),
            "camp-1",
            tasks_spec="box_advanced",
            seasons_spec="2024",
            max_calls=10,
            batch_size=2,
        )

        assert result["status"] == "outage"
        assert len(ledger_calls) == 1
        task, attempted, misses = ledger_calls[0]
        assert task == "box_advanced"
        assert attempted == [3, 2]
        assert misses == [2]
        assert result["bad_data_game_ids"] == [2]

    def test_bad_data_run_populates_ids_and_exits_ok_path(self, monkeypatch):
        from scripts import backfill_refresh
        from src.pipelines.sources.stats import AdvancedBoxBatchReport

        batches_run = []

        def fake_run_batch(pipeline, task, game_ids, *, fallback_probe_game_id=None):
            batches_run.append(list(game_ids))
            report = AdvancedBoxBatchReport(
                attempted=game_ids,
                server_error_game_ids=[game_ids[0]],
            )
            return {"ok": True}, [game_ids[0]], report

        monkeypatch.setattr(backfill_refresh, "run_batch", fake_run_batch)
        monkeypatch.setattr(backfill_refresh, "insert_ledger_rows", lambda *a, **k: None)
        monkeypatch.setattr(backfill_refresh, "get_month_spend", lambda conn: 0)
        monkeypatch.setattr(backfill_refresh, "fetch_last_refreshed_game_id", lambda *a: None)
        monkeypatch.setattr(
            backfill_refresh, "fetch_unfinalized", lambda conn, campaign: ([], None)
        )

        class Limiter:
            def check_budget(self, n):
                return True

        monkeypatch.setattr("src.pipelines.utils.rate_limiter.get_rate_limiter", lambda: Limiter())

        row = {
            "campaign": "camp-1",
            "description": None,
            "seasons": [2024],
            "tasks": ["box_advanced"],
            "created_at": None,
            "completed_at": None,
            "last_finalized_at": None,
        }
        monkeypatch.setattr(backfill_refresh, "get_or_create_campaign", lambda *a, **k: row)
        monkeypatch.setattr(
            backfill_refresh,
            "fetch_candidate_games",
            lambda conn, seasons: [(10, 2024, 1), (20, 2024, 1)],
        )
        monkeypatch.setattr(backfill_refresh, "fetch_done_ids", lambda conn, campaign, task: set())
        monkeypatch.setattr(backfill_refresh, "build_stats_pipeline", lambda: object())

        result = backfill_refresh.run_campaign(
            object(),
            "camp-1",
            tasks_spec="box_advanced",
            seasons_spec="2024",
            max_calls=10,
            batch_size=1,
        )

        assert result["status"] == "ok"
        assert result["bad_data_game_ids"] == [20, 10]
        assert batches_run == [[20], [10]]


def test_main_exit_code_maps_outage(monkeypatch):
    import psycopg2

    from scripts import backfill_refresh

    exits = []

    def fake_exit(code):
        exits.append(code)
        raise SystemExit(code)

    monkeypatch.setattr(backfill_refresh.sys, "exit", fake_exit)
    monkeypatch.setattr(
        backfill_refresh,
        "run_campaign",
        lambda *a, **k: {"status": "outage", "bad_data_game_ids": [1]},
    )
    monkeypatch.setattr(backfill_refresh, "get_db_url", lambda: "postgresql://local/test")
    monkeypatch.setattr(psycopg2, "connect", lambda url: _FakeConn(rows=[]))

    class Op:
        outcome = None

    class Ctx:
        def __enter__(self):
            return Op()

        def __exit__(self, *exc):
            return False

    monkeypatch.setattr(
        "src.pipelines.utils.quota_admission.quota_operation", lambda *a, **k: Ctx()
    )

    class Args:
        requeue_no_data = False
        status = False
        dry_run = False
        campaign = "c"
        create = False
        seasons = None
        tasks = "plays_stats,box_advanced"
        max_calls = 1000
        monthly_cap = 30000
        batch_size = 50
        description = None

    class Parser:
        def parse_args(self):
            return Args()

    monkeypatch.setattr(backfill_refresh, "create_parser", lambda: Parser())

    with pytest.raises(SystemExit):
        backfill_refresh.main()

    assert exits == [1]
