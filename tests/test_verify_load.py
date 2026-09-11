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


class TestCheckVariantTwins:
    """KTD7 tripwire wired into the daily verifier: check_variant_twins must
    FAIL on an unexpected __v_double twin, WARN (never FAIL) on a missing
    expected twin, and WARN (never crash) if the finder itself errors."""

    def test_no_unexpected_no_missing_passes(self, monkeypatch, capsys):
        from scripts.verify_load import Report, check_variant_twins

        monkeypatch.setattr(
            "src.pipelines.utils.variant_twins.find_unexpected_twins", lambda cur: {}
        )
        monkeypatch.setattr("src.pipelines.utils.variant_twins.find_missing_twins", lambda cur: {})

        report = Report()
        check_variant_twins(cur=object(), report=report)

        assert report.failures == 0
        assert "[PASS] variant_twins: no unreviewed __v_double twins" in capsys.readouterr().out

    def test_unexpected_twin_fails(self, monkeypatch, capsys):
        from scripts.verify_load import Report, check_variant_twins

        monkeypatch.setattr(
            "src.pipelines.utils.variant_twins.find_unexpected_twins",
            lambda cur: {"stats.rushing_player_season": ["new_metric__v_double"]},
        )
        monkeypatch.setattr("src.pipelines.utils.variant_twins.find_missing_twins", lambda cur: {})

        report = Report()
        check_variant_twins(cur=object(), report=report)

        assert report.failures == 1
        out = capsys.readouterr().out
        assert "[FAIL] variant_twins:" in out
        assert "stats.rushing_player_season" in out
        assert "new_metric__v_double" in out
        assert "review maintained consumers before choosing a remedy" in out
        assert "EXPECTED_VARIANT_TWINS" in out
        assert "applicable deploy-time validation" in out
        assert "REVIEWED_RAW_ONLY_VARIANT_TWINS" in out
        assert "with its rationale" in out

    def test_missing_twin_warns_not_fails(self, monkeypatch, capsys):
        from scripts.verify_load import Report, check_variant_twins

        monkeypatch.setattr(
            "src.pipelines.utils.variant_twins.find_unexpected_twins", lambda cur: {}
        )
        monkeypatch.setattr(
            "src.pipelines.utils.variant_twins.find_missing_twins",
            lambda cur: {"stats.passing_player_season": ["average_yards_after_catch__v_double"]},
        )

        report = Report()
        check_variant_twins(cur=object(), report=report)

        assert report.failures == 0
        out = capsys.readouterr().out
        assert "[WARN] variant_twins_missing:" in out
        assert "stats.passing_player_season" in out

    def test_finder_exception_warns_never_crashes(self, monkeypatch, capsys):
        from scripts.verify_load import Report, check_variant_twins

        def _boom(cur):
            raise RuntimeError("relation does not exist")

        monkeypatch.setattr("src.pipelines.utils.variant_twins.find_unexpected_twins", _boom)

        report = Report()
        check_variant_twins(cur=object(), report=report)  # must not raise

        assert report.failures == 0
        out = capsys.readouterr().out
        assert "[WARN] variant_twins:" in out
        assert "could not run" in out

    def test_docstring_explains_the_savepoint(self):
        from scripts.verify_load import check_variant_twins

        assert "SAVEPOINT" in (check_variant_twins.__doc__ or "")

    def test_the_check_is_wired_into_the_run_after_freshness(self):
        """A check that is never called is a comment -- and this one must run
        after check_freshness per the module docstring's numbered list."""
        import inspect

        import scripts.verify_load as vl

        src = inspect.getsource(vl.verify)
        assert "check_freshness(cur, in_season, strict, report)" in src
        assert "check_variant_twins(cur, report)" in src
        assert src.index("check_freshness(cur, in_season, strict, report)") < src.index(
            "check_variant_twins(cur, report)"
        )

    def test_docstring_lists_the_check(self):
        import scripts.verify_load as vl

        assert "variant" in vl.__doc__.lower()


class _FakeConnection:
    """Bare psycopg2-connection stand-in: check_variant_twins only reads
    `.autocommit` off of `cur.connection`."""

    def __init__(self, autocommit: bool = False):
        self.autocommit = autocommit


class _SavepointCursor:
    """Fake cursor exercising the real (non-monkeypatched)
    find_unexpected_twins/find_missing_twins against a real EXPECTED_
    VARIANT_TWINS -- unlike the tests above, which stub the finder
    functions out entirely. Records every SQL statement issued so the
    SAVEPOINT/ROLLBACK TO SAVEPOINT/RELEASE SAVEPOINT bracket around the
    two catalog queries can be asserted on directly. `fail_catalog=True`
    raises the first time an information_schema catalog query runs,
    simulating the PostgreSQL-level failure (e.g. a malformed query) that
    would otherwise leave the shared connection's transaction aborted."""

    def __init__(self, fail_catalog: bool, autocommit: bool = False):
        self.connection = _FakeConnection(autocommit=autocommit)
        self.statements: list[str] = []
        self._fail_catalog = fail_catalog

    def execute(self, sql, params=None):
        self.statements.append(sql)
        if "information_schema" in sql and self._fail_catalog:
            raise RuntimeError("relation does not exist")

    def fetchall(self):
        return []


class TestCheckVariantTwinsSavepoint:
    """Finding 2 (P2, post-#110 review): the two catalog queries run inside
    a SAVEPOINT so a PostgreSQL-level failure there can't leave verify()'s
    shared, non-autocommit connection in InFailedSqlTransaction for every
    check that runs afterward."""

    def test_catalog_failure_rolls_back_to_savepoint_and_warns(self, capsys):
        from scripts.verify_load import Report, check_variant_twins

        cur = _SavepointCursor(fail_catalog=True)
        report = Report()

        check_variant_twins(cur, report)  # must not raise

        assert "SAVEPOINT variant_twins" in cur.statements
        assert "ROLLBACK TO SAVEPOINT variant_twins" in cur.statements
        assert "RELEASE SAVEPOINT variant_twins" not in cur.statements
        assert report.failures == 0
        out = capsys.readouterr().out
        assert "[WARN] variant_twins:" in out
        assert "could not run" in out

    def test_catalog_success_releases_savepoint(self):
        from scripts.verify_load import Report, check_variant_twins

        cur = _SavepointCursor(fail_catalog=False)
        report = Report()

        check_variant_twins(cur, report)

        assert "SAVEPOINT variant_twins" in cur.statements
        assert "RELEASE SAVEPOINT variant_twins" in cur.statements
        assert not any(s.startswith("ROLLBACK TO SAVEPOINT") for s in cur.statements)

    def test_autocommit_connection_skips_the_savepoint_dance(self):
        from scripts.verify_load import Report, check_variant_twins

        cur = _SavepointCursor(fail_catalog=False, autocommit=True)
        report = Report()

        check_variant_twins(cur, report)

        assert not any("SAVEPOINT" in s for s in cur.statements)

    def test_autocommit_connection_still_warns_not_crashes_on_catalog_failure(self, capsys):
        from scripts.verify_load import Report, check_variant_twins

        cur = _SavepointCursor(fail_catalog=True, autocommit=True)
        report = Report()

        check_variant_twins(cur, report)  # must not raise

        assert not any("SAVEPOINT" in s for s in cur.statements)
        assert report.failures == 0
        out = capsys.readouterr().out
        assert "[WARN] variant_twins:" in out


class ReceiptCursor:
    def __init__(self, rows, installed=True, error=None):
        self.rows = rows
        self.installed = installed
        self.error = error
        self.queries = []

    def execute(self, statement):
        self.queries.append(statement)
        if self.error and "to_jsonb" in statement:
            raise self.error

    def fetchone(self):
        return ("get_asset_freshness()" if self.installed else None,)

    def fetchall(self):
        return [(row,) for row in self.rows]


def receipt_rows(**changes):
    result = []
    for asset in ("analytics.house_elo_game", "marts.house_elo_game"):
        row = {
            "asset_key": asset,
            "coverage_key": "source-wide",
            "generation_id": "generation",
            "published_at": "2026-09-09T00:00:00Z",
            "age_seconds": 5,
            "expected_refresh_interval": None,
            "is_stale": None,
            "publication_state": "current",
            "current_outcome": "succeeded",
            "coverage": {"complete": True},
            "recorded_inputs_current": True if asset.startswith("marts.") else None,
            "input_closure_current": None,
            "latest_outcome": "succeeded",
        }
        row.update(changes)
        result.append(row)
    return result


def receipt_check(rows=None, *, installed=True, strict=False, in_season=True):
    from scripts.verify_load import Report, check_receipt_freshness

    report = Report()
    cur = ReceiptCursor(receipt_rows() if rows is None else rows, installed=installed)
    check_receipt_freshness(cur, in_season=in_season, strict=strict, report=report)
    return report, cur


def test_receipt_missing_migration_warns_without_querying_missing_rpc(capsys):
    report, cur = receipt_check(installed=False, strict=True)
    assert report.failures == 0
    assert len(cur.queries) == 1
    assert "[WARN]" in capsys.readouterr().out


def test_receipt_unknown_cadence_and_closure_never_claim_freshness(capsys):
    report, _ = receipt_check()
    assert report.failures == 0
    output = capsys.readouterr().out
    assert "[PASS]" not in output
    assert "policy is undeclared" in output
    assert "upstream closure unverified" in output


def test_receipt_unrecorded_stays_optional_under_strict(capsys):
    report, _ = receipt_check(
        receipt_rows(publication_state="unrecorded", generation_id=None), strict=True
    )
    assert report.failures == 0
    assert "optional publisher has no receipts" in capsys.readouterr().out


def test_receipt_missing_current_with_history_is_strict_failure(capsys):
    values = receipt_rows(publication_state="unpublished", generation_id=None)
    assert receipt_check(values)[0].failures == 0
    assert receipt_check(values, strict=True)[0].failures == 2
    assert "no valid current publication" in capsys.readouterr().out


def test_receipt_incomplete_or_failed_current_is_never_valid():
    assert receipt_check(receipt_rows(coverage={"complete": False}))[0].failures == 2
    assert receipt_check(receipt_rows(current_outcome="failed"))[0].failures == 2
    assert receipt_check(receipt_rows(published_at=None))[0].failures == 2


def test_receipt_expected_no_data_is_not_failure(capsys):
    assert receipt_check(receipt_rows(current_outcome="expected_no_data"))[0].failures == 0
    assert "expected_no_data" in capsys.readouterr().out


def test_receipt_broken_or_unverified_required_mart_edge_fails():
    assert receipt_check(receipt_rows(recorded_inputs_current=False))[0].failures == 2
    assert receipt_check(receipt_rows(recorded_inputs_current=None))[0].failures == 1
    assert receipt_check(receipt_rows(input_closure_current=False))[0].failures == 2


def test_receipt_declared_staleness_respects_existing_severity_rule():
    values = receipt_rows(is_stale=True, expected_refresh_interval="1 day")
    assert receipt_check(values, in_season=True)[0].failures == 2
    assert receipt_check(values, in_season=False)[0].failures == 0
    assert receipt_check(values, in_season=False, strict=True)[0].failures == 2


def test_receipt_failed_attempt_warns_without_replacing_current(capsys):
    for outcome in ("failed", "deferred", "blocked", "partial"):
        assert receipt_check(receipt_rows(latest_outcome=outcome))[0].failures == 0
        output = capsys.readouterr().out
        assert f"latest attempt {outcome}" in output
        assert "current succeeded publication" in output


def test_receipt_missing_duplicate_or_wrong_grain_rows_fail():
    values = receipt_rows()
    for bad in ([], values[:1], [values[0], values[0]], receipt_rows(coverage_key="season")):
        assert receipt_check(bad)[0].failures == 1


def test_receipt_rpc_error_propagates_instead_of_silent_pass():
    import pytest

    from scripts.verify_load import Report, check_receipt_freshness

    cur = ReceiptCursor([], error=RuntimeError("permission denied"))
    with pytest.raises(RuntimeError, match="permission denied"):
        check_receipt_freshness(cur, in_season=True, strict=False, report=Report())


SOURCE_IDENTITIES = (
    ("sdv_ratings_weekly", "ratings.sdv_ratings_weekly"),
    ("sdv_fpi_weekly", "ratings.espn_fpi_weekly"),
    ("sdv_team_xwalk", "ref.team_id_xwalk"),
    ("sdv_game_xwalk", "ref.game_id_xwalk"),
)


class SourceReceiptCursor:
    def __init__(self, rows, installed=True, error=None):
        self.rows = rows
        self.installed = installed
        self.error = error
        self.queries = []

    def execute(self, statement, params=None):
        self.queries.append((statement, params))
        if self.error and "to_jsonb" in statement:
            raise self.error

    def fetchone(self):
        return ("get_source_freshness(bigint)" if self.installed else None,)

    def fetchall(self):
        return [(row,) for row in self.rows]


def source_receipt_rows(season=2025, **changes):
    rows = []
    for source_name, asset_key in SOURCE_IDENTITIES:
        origin = "registered_url"
        basis = (
            "artifact_field"
            if source_name in {"sdv_ratings_weekly", "sdv_fpi_weekly"}
            else "registered_artifact_name"
        )
        row = {
            "source_name": source_name,
            "asset_key": asset_key,
            "season": season,
            "coverage_key": f"season:{season}",
            "generation_id": "11111111-1111-4111-8111-111111111111",
            "published_at": "2026-09-09T00:00:00Z",
            "age_seconds": 5.0,
            "expected_refresh_interval": "8 days",
            "is_stale": False,
            "publication_state": "current",
            "current_outcome": "succeeded",
            "is_complete": True,
            "source_rows": 10,
            "published_rows": 10,
            "artifact_origin": origin,
            "season_basis": basis,
            "latest_outcome": "succeeded",
            "latest_recorded_at": "2026-09-09T00:00:00Z",
            "last_failure_outcome": None,
            "last_failure_at": None,
            "last_failure_category": None,
        }
        row.update(changes)
        rows.append(row)
    return rows


def noncurrent_source_rows(state, season=2025, **changes):
    rows = source_receipt_rows(season)
    for row in rows:
        row.update(
            publication_state=state,
            generation_id=None,
            published_at=None,
            age_seconds=None,
            is_stale=None,
            current_outcome=None,
            is_complete=None,
            source_rows=None,
            published_rows=None,
            artifact_origin=None,
            season_basis=None,
        )
        if state == "unrecorded":
            row.update(latest_outcome=None, latest_recorded_at=None)
        row.update(changes)
    return rows


def source_receipt_check(
    rows=None, *, season=2025, installed=True, strict=False, in_season=True, error=None
):
    from scripts.verify_load import Report, check_source_freshness

    report = Report()
    cur = SourceReceiptCursor(
        source_receipt_rows(season) if rows is None else rows,
        installed=installed,
        error=error,
    )
    check_source_freshness(cur, season=season, in_season=in_season, strict=strict, report=report)
    return report, cur


def test_source_receipt_query_is_season_scoped_and_fresh_rows_pass(capsys):
    report, cur = source_receipt_check(season=2025)
    assert report.failures == 0
    assert cur.queries == [
        ("SELECT to_regprocedure('public.get_source_freshness(bigint)')", None),
        ("SELECT to_jsonb(f) FROM public.get_source_freshness(%s) f", (2025,)),
    ]
    output = capsys.readouterr().out
    assert output.count("[PASS] source_freshness:") == 4


def test_source_receipt_missing_rpc_warns_under_strict_without_querying(capsys):
    report, cur = source_receipt_check(installed=False, strict=True)
    assert report.failures == 0
    assert len(cur.queries) == 1
    output = capsys.readouterr().out
    assert "[WARN] source_freshness:" in output
    assert "[PASS]" not in output


def test_source_receipt_scope_requires_exact_four_selected_season_rows():
    valid = source_receipt_rows()
    wrong_season = source_receipt_rows()
    wrong_season[0]["season"] = 2024
    wrong_coverage = source_receipt_rows()
    wrong_coverage[0]["coverage_key"] = "season:2024"
    wrong_source = source_receipt_rows()
    wrong_source[0]["source_name"] = "unknown"
    for rows in (
        [],
        valid[:3],
        [*valid, valid[0]],
        [valid[0], valid[0], *valid[2:]],
        wrong_season,
        wrong_coverage,
        wrong_source,
    ):
        assert source_receipt_check(rows)[0].failures == 1


def test_source_receipt_requires_exact_flat_rpc_shape():
    missing = source_receipt_rows()
    missing[0].pop("source_rows")
    extra = source_receipt_rows()
    extra[0]["coverage"] = {"complete": True}
    malformed_identity = source_receipt_rows()
    malformed_identity[0]["season"] = {"year": 2025}
    for rows in (missing, extra, malformed_identity):
        assert source_receipt_check(rows)[0].failures == 1


def test_source_receipt_unrecorded_is_optional_even_under_strict(capsys):
    report, _ = source_receipt_check(noncurrent_source_rows("unrecorded"), strict=True)
    assert report.failures == 0
    output = capsys.readouterr().out
    assert output.count("optional publisher has no receipts") == 4
    assert "[PASS]" not in output


def test_source_receipt_unpublished_history_fails_only_when_strict(capsys):
    rows = noncurrent_source_rows(
        "unpublished",
        latest_outcome="failed",
        last_failure_outcome="failed",
        last_failure_at="2026-09-09T00:00:00Z",
        last_failure_category="source_publication_failed",
    )
    assert source_receipt_check(rows, strict=False)[0].failures == 0
    assert source_receipt_check(rows, strict=True)[0].failures == 4
    assert "latest attempt failed" in capsys.readouterr().out


def test_source_receipt_invalid_pointer_always_fails():
    rows = noncurrent_source_rows("invalid")
    assert source_receipt_check(rows, in_season=False, strict=False)[0].failures == 4


def test_source_receipt_current_evidence_is_exact_and_complete():
    mutations = (
        {"current_outcome": "failed"},
        {"is_complete": False},
        {"source_rows": 0, "published_rows": 0},
        {"source_rows": 10, "published_rows": 9},
        {"artifact_origin": "unknown"},
        {"age_seconds": float("nan")},
    )
    for mutation in mutations:
        rows = source_receipt_rows()
        rows[0].update(mutation)
        assert source_receipt_check(rows)[0].failures == 1


def test_source_receipt_season_basis_matches_source_and_origin():
    wrong_fpi = source_receipt_rows()
    wrong_fpi[1]["season_basis"] = "caller_declared"
    wrong_local_xwalk = source_receipt_rows()
    wrong_local_xwalk[2].update(
        artifact_origin="local_file", season_basis="registered_artifact_name"
    )
    valid_local_xwalk = source_receipt_rows()
    valid_local_xwalk[2].update(artifact_origin="local_file", season_basis="caller_declared")
    assert source_receipt_check(wrong_fpi)[0].failures == 1
    assert source_receipt_check(wrong_local_xwalk)[0].failures == 1
    assert source_receipt_check(valid_local_xwalk)[0].failures == 0


def test_source_receipt_declared_staleness_uses_existing_severity_rule():
    rows = source_receipt_rows(is_stale=True)
    assert source_receipt_check(rows, in_season=True)[0].failures == 4
    assert source_receipt_check(rows, in_season=False)[0].failures == 0
    assert source_receipt_check(rows, in_season=False, strict=True)[0].failures == 4


def test_source_receipt_unknown_policy_warns_and_never_claims_freshness(capsys):
    rows = source_receipt_rows(expected_refresh_interval=None, is_stale=None)
    report, _ = source_receipt_check(rows, strict=True)
    assert report.failures == 0
    output = capsys.readouterr().out
    assert output.count("[WARN] source_freshness:") == 4
    assert "[PASS]" not in output


def test_source_receipt_latest_bad_attempt_does_not_replace_readable_current(capsys):
    for outcome in ("failed", "deferred", "blocked", "partial"):
        rows = source_receipt_rows(
            latest_outcome=outcome,
            last_failure_outcome=outcome,
            last_failure_at="2026-09-09T01:00:00Z",
            last_failure_category="source_publication_failed",
        )
        report, _ = source_receipt_check(rows)
        assert report.failures == 0
        output = capsys.readouterr().out
        assert f"latest attempt {outcome}" in output
        assert "current succeeded complete publication" in output


def test_source_receipt_expected_no_data_is_diagnostic_only(capsys):
    unpublished = noncurrent_source_rows("unpublished", latest_outcome="expected_no_data")
    assert source_receipt_check(unpublished, strict=False)[0].failures == 0
    assert source_receipt_check(unpublished, strict=True)[0].failures == 4

    current_diagnostic = source_receipt_rows(latest_outcome="expected_no_data")
    assert source_receipt_check(current_diagnostic)[0].failures == 0

    invalid_current = source_receipt_rows(current_outcome="expected_no_data")
    assert source_receipt_check(invalid_current)[0].failures == 4
    output = capsys.readouterr().out
    assert "latest attempt expected_no_data" not in output
    assert "invalid current publication evidence" in output


def test_source_receipt_historical_failure_after_recovery_does_not_warn(capsys):
    rows = source_receipt_rows(
        last_failure_outcome="failed",
        last_failure_at="2026-09-08T00:00:00Z",
        last_failure_category="source_publication_failed",
    )
    assert source_receipt_check(rows)[0].failures == 0
    output = capsys.readouterr().out
    assert "[WARN] source_receipt_attempt:" not in output
    assert output.count("[PASS] source_freshness:") == 4


def test_source_receipt_malformed_state_and_history_always_fail():
    cases = (
        ("publication_state", "mystery"),
        ("latest_recorded_at", None),
        ("last_failure_outcome", "failed"),
        ("expected_refresh_interval", 8),
    )
    for field, value in cases:
        rows = source_receipt_rows()
        rows[0][field] = value
        assert source_receipt_check(rows)[0].failures == 1


def test_source_receipt_rejects_arbitrary_failure_category(capsys):
    rows = source_receipt_rows(
        last_failure_outcome="failed",
        last_failure_at="2026-09-08T00:00:00Z",
        last_failure_category="raw-secret-error",
    )
    assert source_receipt_check(rows)[0].failures == 4
    assert "failure history is malformed" in capsys.readouterr().out


def test_source_receipt_rpc_error_propagates_instead_of_silent_pass():
    import pytest

    with pytest.raises(RuntimeError, match="permission denied"):
        source_receipt_check(error=RuntimeError("permission denied"))


def test_verify_includes_receipt_check_after_legacy_check(monkeypatch):
    from unittest.mock import MagicMock, call

    import psycopg2

    from scripts import refresh_marts, verify_load

    checks = [
        "check_partition",
        "check_game_counts",
        "check_completed_have_team_stats",
        "check_completed_have_plays",
        "check_fitted_coverage",
        "check_backtest_freshness",
        "check_freshness",
        "check_receipt_freshness",
        "check_source_freshness",
        "check_variant_twins",
        "check_massey_composite",
        "check_availability_archive",
    ]
    seen = []
    for name in checks:
        monkeypatch.setattr(verify_load, name, lambda *args, _name=name: seen.append(_name))
    conn = MagicMock()
    monkeypatch.setattr(psycopg2, "connect", lambda _: conn)
    monkeypatch.setattr(refresh_marts, "get_db_url", lambda: "unused")
    assert verify_load.verify(2026, strict=False) == 0
    assert seen == checks
    conn.set_session.assert_called_once_with(readonly=True)
    assert conn.mock_calls.index(call.set_session(readonly=True)) < conn.mock_calls.index(
        call.cursor()
    )
    conn.close.assert_called_once()


def test_verify_closes_connection_if_read_only_session_setup_fails(monkeypatch):
    from unittest.mock import MagicMock

    import psycopg2
    import pytest

    from scripts import refresh_marts, verify_load

    conn = MagicMock()
    conn.set_session.side_effect = RuntimeError("read-only session unavailable")
    monkeypatch.setattr(psycopg2, "connect", lambda _: conn)
    monkeypatch.setattr(refresh_marts, "get_db_url", lambda: "unused")

    with pytest.raises(RuntimeError, match="read-only session unavailable"):
        verify_load.verify(2026, strict=False)

    conn.close.assert_called_once()


def test_receipt_unversioned_inputs_prevent_pass_even_if_closure_claim_disagrees(capsys):
    values = receipt_rows(
        is_stale=False,
        expected_refresh_interval="1 day",
        input_closure_current=True,
        unversioned_input_assets=["core.games"],
    )
    assert receipt_check(values)[0].failures == 0
    output = capsys.readouterr().out
    assert "[PASS]" not in output
    assert "upstream closure unverified" in output
