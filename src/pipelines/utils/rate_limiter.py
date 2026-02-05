"""Rate limiter for CFBD API budget tracking."""

import json
import logging
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)


class RateLimiter:
    """Track API call budget across sessions.

    Persists state to a JSON file to track monthly usage.
    """

    def __init__(
        self,
        monthly_budget: int = 75000,
        state_file: Path | None = None,
    ):
        """Initialize the rate limiter.

        Args:
            monthly_budget: Maximum API calls per month (Tier 3 = 75,000)
            state_file: Path to state file. Defaults to .dlt/rate_limit_state.json
        """
        self.monthly_budget = monthly_budget
        self.state_file = state_file or Path(".dlt/rate_limit_state.json")
        self._load_state()

    def _load_state(self):
        """Load or initialize state from file."""
        if self.state_file.exists():
            with open(self.state_file) as f:
                state = json.load(f)
                state_month = state.get("month")
                current_month = datetime.now().strftime("%Y-%m")

                if state_month == current_month:
                    self.calls_used = state.get("calls_used", 0)
                    self.month = state_month
                else:
                    # New month, reset counter
                    self.calls_used = 0
                    self.month = current_month
        else:
            self.calls_used = 0
            self.month = datetime.now().strftime("%Y-%m")

    def _save_state(self):
        """Save current state to file."""
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        with open(self.state_file, "w") as f:
            json.dump(
                {
                    "month": self.month,
                    "calls_used": self.calls_used,
                    "monthly_budget": self.monthly_budget,
                    "last_updated": datetime.now().isoformat(),
                },
                f,
                indent=2,
            )

    @property
    def remaining(self) -> int:
        """Return remaining API calls this month."""
        return self.monthly_budget - self.calls_used

    @property
    def usage_percent(self) -> float:
        """Return percentage of budget used."""
        return (self.calls_used / self.monthly_budget) * 100

    def check_budget(self, calls_needed: int = 1) -> bool:
        """Check if we have budget for the specified number of calls.

        Args:
            calls_needed: Number of API calls we're about to make

        Returns:
            True if we have budget, False otherwise
        """
        return self.remaining >= calls_needed

    def record_call(self, count: int = 1):
        """Record API calls made.

        Args:
            count: Number of calls to record
        """
        self.calls_used += count
        self._save_state()
        pct = self.usage_percent
        logger.debug(f"API calls: {self.calls_used}/{self.monthly_budget} ({pct:.1f}%)")

    def get_status(self) -> dict:
        """Return current rate limit status."""
        return {
            "month": self.month,
            "calls_used": self.calls_used,
            "remaining": self.remaining,
            "monthly_budget": self.monthly_budget,
            "usage_percent": round(self.usage_percent, 2),
        }

    def warn_if_low(self, threshold: float = 90.0):
        """Log a warning if usage is above threshold."""
        if self.usage_percent >= threshold:
            logger.warning(
                f"API budget {self.usage_percent:.1f}% used. "
                f"{self.remaining} calls remaining this month."
            )


# Global rate limiter instance
_rate_limiter: RateLimiter | None = None


def get_rate_limiter() -> RateLimiter:
    """Get the global rate limiter instance."""
    global _rate_limiter
    if _rate_limiter is None:
        _rate_limiter = RateLimiter()
    return _rate_limiter
