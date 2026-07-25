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


def get_projection_seasons(conn) -> list[int]:
    """Seasons the compute chain must maintain, resolved from the data itself.

    `get_current_season()` is a *calendar* rule and is correct for ingest year
    windows, but it is wrong for the compute chain: it returns `year - 1` until
    August, so between January and July the "current season" is the one that
    already finished. Every `--incremental` step keyed on it spends the whole
    offseason rebuilding last season while the upcoming season -- which already
    has a published schedule and is exactly what predictions are wanted for --
    is never built at all.

    This resolves the target seasons from `core.games` instead: the most recent
    season with completed games, plus every later season that has scheduled
    games. In July 2026 that is `[2025, 2026]`; in October 2026 it is `[2026]`.
    Calendar-independent and self-healing at the season rollover.

    Mirrors the selection `scripts/compute_predictions.py`'s
    `TARGET_GAMES_QUERY` already uses (`season >= (SELECT MAX(season) FROM
    core.games WHERE completed)`) -- that script was unaffected by the blackout
    precisely because it never consulted the calendar.

    Takes an open psycopg2 connection so this module keeps no DB dependency of
    its own. Returns seasons in ascending order.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COALESCE(MAX(season), 0) FROM core.games WHERE COALESCE(completed, false)"
        )
        latest_completed = int(cur.fetchone()[0])

        # No completed games anywhere (fresh/partial DB): there is no "since
        # the last completed season" window to open, and `season >= 0` would
        # match all of 1869+. Fall back to the single latest season present.
        if latest_completed == 0:
            cur.execute("SELECT COALESCE(MAX(season), 0) FROM core.games")
            latest_any = int(cur.fetchone()[0])
            return [latest_any] if latest_any else []

        cur.execute(
            "SELECT DISTINCT season FROM core.games WHERE season >= %s ORDER BY season",
            (latest_completed,),
        )
        return [int(row[0]) for row in cur.fetchall()]
