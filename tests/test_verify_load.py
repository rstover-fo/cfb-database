"""Unit tests for verify_load's pure grading helpers (no DB, no API)."""

from datetime import UTC, date, datetime, timedelta

from scripts.verify_load import (
    FAIL,
    PASS,
    WARN,
    evaluate_game_counts,
    evaluate_missing,
    evaluate_snapshot_freshness,
    evaluate_snapshot_team_count,
    evaluate_staleness,
    is_in_season,
)


class TestIsInSeason:
    def test_season_months(self):
        assert all(is_in_season(m) for m in (9, 10, 11, 12, 1))

    def test_off_season_months(self):
        assert not any(is_in_season(m) for m in (2, 3, 4, 5, 6, 7, 8))


class TestEvaluateMissing:
    def test_zero_missing_passes(self):
        assert evaluate_missing(0) == PASS

    def test_within_tolerance_warns(self):
        assert evaluate_missing(1) == WARN
        assert evaluate_missing(10) == WARN

    def test_beyond_tolerance_fails(self):
        assert evaluate_missing(11) == FAIL


class TestEvaluateGameCounts:
    def test_db_matches_api(self):
        assert evaluate_game_counts(api_count=800, db_count=800) == PASS

    def test_db_exceeds_api(self):
        # DB keeps games the API has since dropped; not a load failure
        assert evaluate_game_counts(api_count=800, db_count=805) == PASS

    def test_db_behind_api_fails(self):
        assert evaluate_game_counts(api_count=800, db_count=750) == FAIL

    def test_db_empty_with_api_data_fails(self):
        assert evaluate_game_counts(api_count=800, db_count=0) == FAIL

    def test_both_empty_passes(self):
        assert evaluate_game_counts(api_count=0, db_count=0) == PASS


class TestEvaluateStaleness:
    def test_weekly_in_season_fails(self):
        assert evaluate_staleness("weekly", in_season=True, strict=False) == FAIL

    def test_weekly_off_season_warns(self):
        assert evaluate_staleness("weekly", in_season=False, strict=False) == WARN

    def test_weekly_off_season_strict_fails(self):
        assert evaluate_staleness("weekly", in_season=False, strict=True) == FAIL

    def test_seasonal_always_warns(self):
        assert evaluate_staleness("seasonal", in_season=True, strict=False) == WARN
        assert evaluate_staleness("seasonal", in_season=False, strict=True) == WARN


class TestEvaluateSnapshotFreshness:
    def test_off_season_always_passes(self):
        assert evaluate_snapshot_freshness(None, in_season=False, warn_days=8, fail_days=14) == PASS
        assert evaluate_snapshot_freshness(999, in_season=False, warn_days=8, fail_days=14) == PASS

    def test_in_season_no_snapshots_warns(self):
        assert evaluate_snapshot_freshness(None, in_season=True, warn_days=8, fail_days=14) == WARN

    def test_in_season_within_warn_days_passes(self):
        assert evaluate_snapshot_freshness(0, in_season=True, warn_days=8, fail_days=14) == PASS
        assert evaluate_snapshot_freshness(8, in_season=True, warn_days=8, fail_days=14) == PASS

    def test_in_season_between_warn_and_fail_days_warns(self):
        assert evaluate_snapshot_freshness(9, in_season=True, warn_days=8, fail_days=14) == WARN
        assert evaluate_snapshot_freshness(14, in_season=True, warn_days=8, fail_days=14) == WARN

    def test_in_season_beyond_fail_days_fails(self):
        assert evaluate_snapshot_freshness(15, in_season=True, warn_days=8, fail_days=14) == FAIL
        assert evaluate_snapshot_freshness(30, in_season=True, warn_days=8, fail_days=14) == FAIL


class TestEvaluateSnapshotTeamCount:
    def test_off_season_always_passes(self):
        assert evaluate_snapshot_team_count(None, in_season=False) == PASS
        assert evaluate_snapshot_team_count(0, in_season=False) == PASS

    def test_in_season_none_passes(self):
        # No snapshot at all is graded by evaluate_snapshot_freshness instead;
        # this grader shouldn't double-penalize an absent snapshot.
        assert evaluate_snapshot_team_count(None, in_season=True) == PASS

    def test_in_season_full_coverage_passes(self):
        assert evaluate_snapshot_team_count(120, in_season=True) == PASS
        assert evaluate_snapshot_team_count(135, in_season=True) == PASS

    def test_in_season_partial_coverage_warns(self):
        assert evaluate_snapshot_team_count(100, in_season=True) == WARN
        assert evaluate_snapshot_team_count(119, in_season=True) == WARN

    def test_in_season_low_coverage_fails(self):
        assert evaluate_snapshot_team_count(99, in_season=True) == FAIL
        assert evaluate_snapshot_team_count(0, in_season=True) == FAIL


class _SequencedCursor:
    """Cursor stub returning canned fetchone() results in call order.

    Unlike _RecordingCursor (which always returns (0,)), the new checks issue
    a variable number of queries (a to_regclass guard, then 0-2 more), so
    results must be sequenced per-call rather than fixed.
    """

    def __init__(self, results):
        self._results = list(results)
        self.queries = []

    def execute(self, sql, params=None):
        self.queries.append((sql, params))

    def fetchone(self):
        return self._results.pop(0)


class TestCheckMasseyComposite:
    def test_table_absent_warns_and_stops(self):
        from scripts.verify_load import Report, check_massey_composite

        cur = _SequencedCursor([(None,)])
        report = Report()
        check_massey_composite(cur, 2025, report)

        assert len(cur.queries) == 1
        assert "to_regclass" in cur.queries[0][0]
        assert report.failures == 0

    def test_table_present_no_snapshots_warns_in_season(self, monkeypatch):
        from scripts.verify_load import Report, check_massey_composite

        monkeypatch.setattr("scripts.verify_load._current_in_season", lambda: True)
        # to_regclass hit, MAX(snapshot_date) -> None; team count query skipped
        cur = _SequencedCursor([("oid",), (None,)])
        report = Report()
        check_massey_composite(cur, 2025, report)

        assert len(cur.queries) == 2
        assert report.failures == 0

    def test_fresh_full_snapshot_passes_in_season(self, monkeypatch):
        from scripts.verify_load import Report, check_massey_composite

        monkeypatch.setattr("scripts.verify_load._current_in_season", lambda: True)
        today = date.today()
        cur = _SequencedCursor([("oid",), (today,), (130,)])
        report = Report()
        check_massey_composite(cur, 2025, report)

        assert len(cur.queries) == 3
        assert "snapshot_date = %s" in cur.queries[2][0]
        assert cur.queries[2][1] == (2025, today)
        assert report.failures == 0

    def test_stale_partial_snapshot_fails_in_season(self, monkeypatch):
        from scripts.verify_load import Report, check_massey_composite

        monkeypatch.setattr("scripts.verify_load._current_in_season", lambda: True)
        stale = date.today() - timedelta(days=20)
        cur = _SequencedCursor([("oid",), (stale,), (90,)])
        report = Report()
        check_massey_composite(cur, 2025, report)

        # freshness FAIL (>14 days) + team count FAIL (<100) = 2 failures
        assert report.failures == 2

    def test_stale_snapshot_off_season_passes(self, monkeypatch):
        from scripts.verify_load import Report, check_massey_composite

        monkeypatch.setattr("scripts.verify_load._current_in_season", lambda: False)
        stale = date.today() - timedelta(days=200)
        cur = _SequencedCursor([("oid",), (stale,), (50,)])
        report = Report()
        check_massey_composite(cur, 2025, report)

        assert report.failures == 0

    def test_scoped_to_season(self, monkeypatch):
        from scripts.verify_load import Report, check_massey_composite

        monkeypatch.setattr("scripts.verify_load._current_in_season", lambda: True)
        cur = _SequencedCursor([("oid",), (None,)])
        check_massey_composite(cur, 2025, Report())

        assert cur.queries[1][1] == (2025,)


class TestCheckAvailabilityArchive:
    def test_table_absent_warns_and_stops(self):
        from scripts.verify_load import Report, check_availability_archive

        cur = _SequencedCursor([(None,)])
        report = Report()
        check_availability_archive(cur, 2025, report)

        assert len(cur.queries) == 1
        assert report.failures == 0

    def test_off_season_passes_even_when_absent(self, monkeypatch):
        from scripts.verify_load import Report, check_availability_archive

        monkeypatch.setattr("scripts.verify_load._current_in_season", lambda: False)
        cur = _SequencedCursor([("oid",), (None,)])
        report = Report()
        check_availability_archive(cur, 2025, report)

        assert report.failures == 0

    def test_in_season_absent_warns_never_fails(self, monkeypatch):
        from scripts.verify_load import Report, check_availability_archive

        monkeypatch.setattr("scripts.verify_load._current_in_season", lambda: True)
        cur = _SequencedCursor([("oid",), (None,)])
        report = Report()
        check_availability_archive(cur, 2025, report)

        assert report.failures == 0

    def test_in_season_recent_load_passes(self, monkeypatch):
        from scripts.verify_load import Report, check_availability_archive

        monkeypatch.setattr("scripts.verify_load._current_in_season", lambda: True)
        recent = datetime.now(UTC) - timedelta(days=1)
        cur = _SequencedCursor([("oid",), (recent,)])
        report = Report()
        check_availability_archive(cur, 2025, report)

        assert report.failures == 0

    def test_in_season_stale_load_warns_never_fails(self, monkeypatch):
        from scripts.verify_load import Report, check_availability_archive

        monkeypatch.setattr("scripts.verify_load._current_in_season", lambda: True)
        stale = datetime.now(UTC) - timedelta(days=30)
        cur = _SequencedCursor([("oid",), (stale,)])
        report = Report()
        check_availability_archive(cur, 2025, report)

        assert report.failures == 0

    def test_queries_correct_source_and_status_filter(self, monkeypatch):
        from scripts.verify_load import Report, check_availability_archive

        monkeypatch.setattr("scripts.verify_load._current_in_season", lambda: False)
        cur = _SequencedCursor([("oid",), (None,)])
        check_availability_archive(cur, 2025, Report())

        assert "to_regclass" in cur.queries[0][0]
        assert "source = 'availability'" in cur.queries[1][0]
        assert "status = 'loaded'" in cur.queries[1][0]


class TestCoverageChecksScopedToFbs:
    """Coverage checks must not count lower-division games (run 29866568883:
    a season-wide count reported 2,178 'missing' games -- every FCS/II/III
    game in core.games -- and can never clear the tolerance)."""

    class _RecordingCursor:
        def __init__(self):
            self.queries = []

        def execute(self, sql, params=None):
            self.queries.append(sql)

        def fetchone(self):
            return (0,)

    def _check_sql(self, check_fn):
        from scripts.verify_load import Report

        cur = self._RecordingCursor()
        check_fn(cur, 2025, Report())
        return cur.queries[0]

    def test_team_stats_check_scoped_to_fbs_involved_games(self):
        from scripts.verify_load import check_completed_have_team_stats

        sql = self._check_sql(check_completed_have_team_stats)
        assert "g.home_classification = 'fbs'" in sql
        assert "g.away_classification = 'fbs'" in sql

    def test_plays_check_scoped_to_fbs_involved_games(self):
        from scripts.verify_load import check_completed_have_plays

        sql = self._check_sql(check_completed_have_plays)
        assert "g.home_classification = 'fbs'" in sql
        assert "g.away_classification = 'fbs'" in sql


class TestEvaluateReturningProvisional:
    def test_nothing_evaluated_passes(self):
        from scripts.verify_load import evaluate_returning_provisional

        assert evaluate_returning_provisional(0, 0) == PASS

    def test_no_flagged_teams_passes(self):
        from scripts.verify_load import evaluate_returning_provisional

        assert evaluate_returning_provisional(136, 0) == PASS

    def test_flagged_teams_warn(self):
        from scripts.verify_load import evaluate_returning_provisional

        assert evaluate_returning_provisional(136, 1) == WARN
        assert evaluate_returning_provisional(136, 14) == WARN

    def test_never_fails(self):
        """The defect is CFBD's provisional roster snapshot, not our load; a
        FAIL would redline the daily job for weeks on something no re-run can
        fix (the availability_archive rationale)."""
        from scripts.verify_load import evaluate_returning_provisional

        assert evaluate_returning_provisional(136, 136) != FAIL


class TestCheckReturningProvisional:
    """Preseason returning production must be POSSIBLE, not merely present.

    2026-08-17 audit: CFBD published Washington State at 91% returning PPA
    for 2026 while its top two producers -- ~45% of team PPA, both out of
    eligibility -- could not return. The number merged cleanly, refreshed
    daily, and fed features/marts with no signal anything was wrong."""

    def test_source_tables_absent_warns_and_stops(self):
        from scripts.verify_load import Report, check_returning_provisional

        cur = _SequencedCursor([(None, "oid", "oid")])
        report = Report()
        check_returning_provisional(cur, report)

        assert len(cur.queries) == 1
        assert "to_regclass" in cur.queries[0][0]
        assert report.failures == 0

    def test_no_returning_rows_passes(self):
        from scripts.verify_load import Report, check_returning_provisional

        cur = _SequencedCursor([("oid", "oid", "oid"), (None,)])
        report = Report()
        check_returning_provisional(cur, report)

        assert len(cur.queries) == 2
        assert report.failures == 0

    def test_season_underway_passes_without_grading(self):
        """After kickoff rosters have landed and the metric is settled
        history; grading it would re-warn all season on stale evidence."""
        from scripts.verify_load import Report, check_returning_provisional

        cur = _SequencedCursor([("oid", "oid", "oid"), (2026,), (12,)])
        report = Report()
        check_returning_provisional(cur, report)

        assert len(cur.queries) == 3
        assert cur.queries[2][1] == (2026,)
        assert report.failures == 0

    def test_preseason_flagged_teams_warn_but_never_fail(self):
        from scripts.verify_load import Report, check_returning_provisional

        cur = _SequencedCursor(
            [
                ("oid", "oid", "oid"),
                (2026,),
                (0,),
                (134, 14, "Washington State claims 0.91 vs eligibility cap 0.55"),
            ]
        )
        report = Report()
        check_returning_provisional(cur, report)

        assert len(cur.queries) == 4
        assert report.failures == 0

    def test_grading_query_refutes_by_eligibility(self):
        """The check must compare the CLAIMED percent against what
        still-eligible players produced, with the COVID cohort excluded from
        the refutation set (a pre-2021 first season may hold a free year)."""
        from scripts.verify_load import Report, check_returning_provisional

        cur = _SequencedCursor([("oid", "oid", "oid"), (2026,), (0,), (134, 0, None)])
        check_returning_provisional(cur, Report())

        sql, params = cur.queries[3]
        assert "COUNT(DISTINCT r.year) >= %(max_seasons)s" in sql
        assert "MIN(r.year) >= %(covid_cutoff)s" in sql
        assert "percent_ppa > max_possible + %(slack)s" in sql
        assert params["season"] == 2026

    def test_eligibility_constants_are_sane(self):
        """5 = redshirt + 4 playing seasons; 2021 = first cohort with no
        possible 2020 COVID year. Neither is tunable without re-deriving."""
        from scripts.verify_load import (
            RETURNING_COVID_CUTOFF,
            RETURNING_MAX_SEASONS,
            RETURNING_SLACK,
        )

        assert RETURNING_MAX_SEASONS == 5
        assert RETURNING_COVID_CUTOFF == 2021
        assert 0 < RETURNING_SLACK <= 0.1

    def test_the_gate_is_wired_into_the_run(self):
        """A gate that is never called is a comment."""
        import inspect

        import scripts.verify_load as vl

        assert "check_returning_provisional(cur, report)" in inspect.getsource(vl)


class TestBacktestFreshnessGate:
    """The honesty numbers must be CURRENT, not merely present.

    cfb-app dropped its hardcoded win-MAE constant in favour of reading
    api.model_backtest, and its staleness check is run_date. That check is only
    meaningful if something advances run_date -- otherwise a consumer reads a
    plausible row describing a model that no longer exists and nothing fails.

    This gate exists because the failure already happened: migration 045
    shipped the view before any run had written to it, so api.model_backtest
    returned zero rows for a day while the handoff told cfb-app to depend on
    it. The deploy verified the view existed; nobody verified it had rows."""

    def test_an_empty_table_fails_rather_than_skips(self):
        """'Never backtested' and 'backtested last week' are different
        problems, but neither is a working state -- and an empty table is the
        one that already shipped."""
        import inspect

        from scripts.verify_load import check_backtest_freshness

        src = inspect.getsource(check_backtest_freshness)
        assert "total == 0" in src
        assert "FAIL" in src.split("total == 0")[1][:400], "no row must FAIL, not pass quietly"

    def test_the_gate_is_wired_into_the_run(self):
        """A gate that is never called is a comment."""
        import inspect

        import scripts.verify_load as vl

        assert "check_backtest_freshness(cur, report)" in inspect.getsource(vl)

    def test_threshold_absorbs_one_failed_night(self):
        """The workflow runs daily, so a 1-day threshold would fail on any
        single bad night. Tight enough to catch the step silently dying,
        loose enough not to cry wolf."""
        from scripts.verify_load import MAX_BACKTEST_AGE_DAYS

        assert 2 <= MAX_BACKTEST_AGE_DAYS <= 7

    def test_it_scopes_to_the_published_configuration(self):
        """api.model_backtest keys on (model_version, scope, season_start,
        season_end, strength_share) because those are different measurements.

        Filtering on model+scope alone is not enough: a one-off exploratory run
        over a narrower season range writes its own row with today's date, and
        a MAX(run_date) across all FBS rows would be refreshed by that
        experiment while the published row sat stale."""
        import inspect

        from scripts.verify_load import check_backtest_freshness

        src = inspect.getsource(check_backtest_freshness)
        for predicate in (
            "model_version = 'fitted_v1'",
            "scope = 'fbs'",
            "season_start = %(start)s",
            "season_end = %(end)s",
            "strength_share = %(share)s",
        ):
            assert predicate in src, f"gate must pin {predicate}"

    def test_the_canonical_bounds_are_imported_not_restated(self):
        """The daily workflow runs backtest_preseason.py with NO arguments, so
        the script defaults ARE the canonical configuration. Importing them is
        what keeps the gate and the workflow from drifting apart.

        Restating them is exactly how the row first published to cfb-app ended
        up on --start 2019 against a DEFAULT_START of 2018 -- a configuration
        the daily job would never reproduce, leaving two FBS rows and no way to
        tell which one consumers should read."""
        import inspect

        from scripts.verify_load import check_backtest_freshness

        src = inspect.getsource(check_backtest_freshness)
        assert "from scripts.backtest_preseason import DEFAULT_END, DEFAULT_START" in src
        assert "from scripts.simulate_season import DEFAULT_STRENGTH_SHARE" in src
        # The bounds must reach the query as bound parameters, never literals.
        assert "%(start)s" in src and "%(end)s" in src

    def test_the_workflow_runs_the_canonical_configuration(self):
        """The gate checks the defaults, so the workflow must USE the defaults.
        Any explicit --start/--end here would write a row the gate never looks
        at, and the gate would then fail every night on a missing canonical
        row while a perfectly good non-canonical one sat beside it."""
        import pathlib

        wf = pathlib.Path(".github/workflows/daily-load.yml").read_text()
        bare = "run: python scripts/backtest_preseason.py"
        assert bare in wf, "the daily workflow must run the backtest"
        invoked = next(ln for ln in wf.splitlines() if bare in ln)
        assert invoked.strip() == bare, (
            "the daily backtest must run BARE so the script defaults stay "
            f"canonical; found: {invoked.strip()!r}"
        )
