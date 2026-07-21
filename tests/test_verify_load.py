"""Unit tests for verify_load's pure grading helpers (no DB, no API)."""

from scripts.verify_load import (
    FAIL,
    PASS,
    WARN,
    evaluate_game_counts,
    evaluate_missing,
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
