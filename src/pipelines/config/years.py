"""Year range configuration for CFBD API endpoints.

Different data types have different available year ranges.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class YearRange:
    """Defines the available year range for a data category."""

    start: int
    end: int

    def __iter__(self):
        """Iterate through years from most recent to oldest."""
        return iter(range(self.end, self.start - 1, -1))

    def __contains__(self, year: int) -> bool:
        return self.start <= year <= self.end

    def to_list(self, descending: bool = True) -> list[int]:
        """Return list of years."""
        if descending:
            return list(range(self.end, self.start - 1, -1))
        return list(range(self.start, self.end + 1))


# Year ranges by data category
YEAR_RANGES = {
    # Games go back to 1869 but we'll start with modern era for most uses
    "games": YearRange(start=1869, end=2026),
    "games_modern": YearRange(start=2000, end=2026),

    # Play-by-play only available from 2004
    "plays": YearRange(start=2004, end=2026),

    # Most stats available from 2004
    "stats": YearRange(start=2004, end=2026),

    # Advanced ratings from 2004 (FPI starts 2005)
    "ratings": YearRange(start=2004, end=2026),

    # Recruiting from 2000
    "recruiting": YearRange(start=2000, end=2026),

    # Betting lines from 2013
    "betting": YearRange(start=2013, end=2026),

    # Draft from 2000
    "draft": YearRange(start=2000, end=2026),

    # Advanced metrics from 2014
    "metrics": YearRange(start=2014, end=2026),
}


def get_current_season() -> int:
    """Return the current CFB season year.

    CFB season runs Aug-Jan, so before August we're still in previous season.
    """
    from datetime import datetime

    now = datetime.now()
    if now.month < 8:  # Before August
        return now.year - 1
    return now.year
