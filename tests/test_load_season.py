"""Unit tests for load_season's season-selection helpers (no DB, no API)."""

from scripts.load_season import upcoming_schedule_season


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
