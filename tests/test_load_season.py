"""Unit tests for load_season's season-selection helpers (no DB, no API)."""

from scripts.load_season import ESTIMATED_CALLS, SOURCE_ORDER, load_season, upcoming_schedule_season


class TestUpcomingScheduleSeason:
    def test_pre_august_months_refresh_next_schedule(self):
        # Jan-Jul: get_current_season() points at last calendar year's season,
        # so the upcoming season's schedule needs its own refresh (same month
        # boundary as get_current_season)
        for month in (1, 2, 3, 4, 5, 6, 7):
            assert upcoming_schedule_season(2025, month) == 2026

    def test_in_season_months_skip(self):
        for month in (8, 9, 10, 11, 12):
            assert upcoming_schedule_season(2026, month) is None


class TestMetricsWpWiring:
    """P3.2 Lane B: metrics_wp must be wired into the same places every other
    source is (SOURCE_ORDER, ESTIMATED_CALLS, runners, active-by-default),
    with zero workflow-file changes -- the daily workflow already runs
    load_season.py --weekly over all default-active sources."""

    def test_metrics_wp_in_source_order(self):
        assert "metrics_wp" in SOURCE_ORDER

    def test_metrics_wp_has_estimated_calls(self):
        assert "metrics_wp" in ESTIMATED_CALLS
        assert ESTIMATED_CALLS["metrics_wp"] == 70

    def test_metrics_wp_active_by_default(self):
        """Only "rosters" is excluded from the default active-source list
        (it requires --teams); metrics_wp must NOT be excluded the same way,
        or the daily workflow silently never runs it."""
        default_active = [s for s in SOURCE_ORDER if s != "rosters"]
        assert "metrics_wp" in default_active

    def test_dry_run_includes_metrics_wp_estimate(self, capsys):
        """load_season's dry-run path is pure printing (rate limiter reads a
        local state file only) -- no DB, no API -- so it's safe to exercise
        directly and confirm metrics_wp's estimate surfaces in the plan."""
        summary = load_season(season=2024, sources=["metrics_wp"], dry_run=True)

        assert summary["dry_run"] is True
        assert summary["estimated_calls"] == ESTIMATED_CALLS["metrics_wp"]

        captured = capsys.readouterr()
        assert "metrics_wp" in captured.out
