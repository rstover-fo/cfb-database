"""Tests for year range configuration."""

from unittest.mock import patch

from src.pipelines.config.years import YEAR_RANGES, YearRange, get_current_season


class TestYearRange:
    def test_to_list_descending(self):
        yr = YearRange(start=2020, end=2023)
        assert yr.to_list() == [2023, 2022, 2021, 2020]

    def test_to_list_ascending(self):
        yr = YearRange(start=2020, end=2023)
        assert yr.to_list(descending=False) == [2020, 2021, 2022, 2023]

    def test_contains(self):
        yr = YearRange(start=2004, end=2026)
        assert 2010 in yr
        assert 2004 in yr
        assert 2026 in yr
        assert 2003 not in yr
        assert 2027 not in yr

    def test_iter_descending(self):
        yr = YearRange(start=2023, end=2025)
        assert list(yr) == [2025, 2024, 2023]

    def test_single_year_range(self):
        yr = YearRange(start=2024, end=2024)
        assert yr.to_list() == [2024]
        assert 2024 in yr


class TestYearRangesConfig:
    def test_all_categories_exist(self):
        expected = {
            "games",
            "games_modern",
            "plays",
            "stats",
            "ratings",
            "recruiting",
            "betting",
            "draft",
            "metrics",
        }
        assert set(YEAR_RANGES.keys()) == expected

    def test_plays_start_2004(self):
        assert YEAR_RANGES["plays"].start == 2004

    def test_ratings_start_2015(self):
        assert YEAR_RANGES["ratings"].start == 2015

    def test_all_ranges_end_2026(self):
        for name, yr in YEAR_RANGES.items():
            assert yr.end == 2026, f"{name} should end at 2026, got {yr.end}"


class TestGetCurrentSeason:
    def test_before_august_returns_previous_year(self):
        from datetime import datetime

        with patch("datetime.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2026, 3, 15)
            assert get_current_season() == 2025

    def test_august_returns_current_year(self):
        from datetime import datetime

        with patch("datetime.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2026, 8, 1)
            assert get_current_season() == 2026

    def test_december_returns_current_year(self):
        from datetime import datetime

        with patch("datetime.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2025, 12, 15)
            assert get_current_season() == 2025

    def test_january_returns_previous_year(self):
        from datetime import datetime

        with patch("datetime.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2026, 1, 27)
            assert get_current_season() == 2025
