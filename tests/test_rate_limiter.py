"""Tests for rate limiter budget tracking."""

import json
from pathlib import Path
from unittest.mock import patch

from src.pipelines.utils.rate_limiter import RateLimiter


class TestRateLimiter:
    def test_init_no_state_file(self, tmp_state_file: Path):
        limiter = RateLimiter(monthly_budget=1000, state_file=tmp_state_file)
        assert limiter.calls_used == 0
        assert limiter.monthly_budget == 1000

    def test_init_with_existing_state(self, mock_state_file: Path):
        with patch("src.pipelines.utils.rate_limiter.datetime") as mock_dt:
            mock_dt.now.return_value.strftime.return_value = "2026-01"
            limiter = RateLimiter(state_file=mock_state_file)
        assert limiter.calls_used == 500
        assert limiter.month == "2026-01"

    def test_monthly_reset(self, mock_state_file: Path):
        """State resets when month changes."""
        with patch("src.pipelines.utils.rate_limiter.datetime") as mock_dt:
            mock_dt.now.return_value.strftime.return_value = "2026-02"
            limiter = RateLimiter(state_file=mock_state_file)
        assert limiter.calls_used == 0
        assert limiter.month == "2026-02"

    def test_remaining(self, tmp_state_file: Path):
        limiter = RateLimiter(monthly_budget=100, state_file=tmp_state_file)
        limiter.calls_used = 30
        assert limiter.remaining == 70

    def test_usage_percent(self, tmp_state_file: Path):
        limiter = RateLimiter(monthly_budget=100, state_file=tmp_state_file)
        limiter.calls_used = 75
        assert limiter.usage_percent == 75.0

    def test_check_budget_sufficient(self, tmp_state_file: Path):
        limiter = RateLimiter(monthly_budget=100, state_file=tmp_state_file)
        limiter.calls_used = 50
        assert limiter.check_budget(10) is True

    def test_check_budget_insufficient(self, tmp_state_file: Path):
        limiter = RateLimiter(monthly_budget=100, state_file=tmp_state_file)
        limiter.calls_used = 95
        assert limiter.check_budget(10) is False

    def test_record_call(self, tmp_state_file: Path):
        limiter = RateLimiter(monthly_budget=100, state_file=tmp_state_file)
        limiter.record_call(5)
        assert limiter.calls_used == 5
        # Verify state persisted
        with open(tmp_state_file) as f:
            state = json.load(f)
        assert state["calls_used"] == 5

    def test_record_call_multiple(self, tmp_state_file: Path):
        limiter = RateLimiter(monthly_budget=100, state_file=tmp_state_file)
        limiter.record_call(3)
        limiter.record_call(7)
        assert limiter.calls_used == 10

    def test_get_status(self, tmp_state_file: Path):
        limiter = RateLimiter(monthly_budget=1000, state_file=tmp_state_file)
        limiter.calls_used = 250
        status = limiter.get_status()
        assert status["calls_used"] == 250
        assert status["remaining"] == 750
        assert status["monthly_budget"] == 1000
        assert status["usage_percent"] == 25.0

    def test_state_file_created_on_save(self, tmp_path: Path):
        state_file = tmp_path / "subdir" / "state.json"
        limiter = RateLimiter(monthly_budget=100, state_file=state_file)
        limiter.record_call(1)
        assert state_file.exists()
