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
