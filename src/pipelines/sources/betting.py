"""Betting data sources - lines and spreads.

Game betting lines from various sportsbooks.

``line_snapshots_resource`` captures a point-in-time snapshot of betting
lines and is append-only: no primary key, ``write_disposition="append"``.
Each pipeline run stamps every row it yields with a single ``captured_at``
timestamp, so one run == one capture. Only pending games (no final score
yet) are snapshotted -- completed games' lines are immutable and already
covered by ``lines_resource``. Because ``betting.lines`` merge-overwrites on
``(game_id, provider)``, prior line values are destroyed on every load, so
snapshot history can only start accruing from the day this resource first
runs -- it cannot be backfilled from historical data.
"""

import hashlib
import logging
from collections.abc import Iterator
from datetime import UTC, datetime

import dlt
from dlt.sources import DltSource

from ..config.years import YEAR_RANGES, get_current_season
from ..utils.api_client import get_client
from .base import make_request

logger = logging.getLogger(__name__)


@dlt.source(name="cfbd_betting")
def betting_source(
    years: list[int] | None = None,
    mode: str = "incremental",
) -> DltSource:
    """Source for betting lines data.

    Args:
        years: Specific years to load. If None, uses mode to determine years.
        mode: "incremental" loads current season, "backfill" loads all historical.
    """
    if years is None:
        if mode == "incremental":
            years = [get_current_season()]
        else:  # backfill
            years = YEAR_RANGES["betting"].to_list()

    return [
        lines_resource(years),
        team_ats_resource(years),
        line_snapshots_resource(years),
    ]


@dlt.resource(
    name="lines",
    write_disposition="merge",
    primary_key=["game_id", "provider"],
)
def lines_resource(years: list[int]) -> Iterator[dict]:
    """Load betting lines for games.

    Args:
        years: List of years to load lines for
    """
    client = get_client()
    try:
        for year in years:
            logger.info(f"Loading betting lines for {year}...")

            data = make_request(client, "/lines", params={"year": year})

            for game in data:
                game_id = game.get("id")
                lines = game.get("lines", [])

                # Flatten - one row per game/provider combination
                for line in lines:
                    yield {
                        "game_id": game_id,
                        "season": game.get("season"),
                        "week": game.get("week"),
                        "home_team": game.get("homeTeam"),
                        "away_team": game.get("awayTeam"),
                        "home_score": game.get("homeScore"),
                        "away_score": game.get("awayScore"),
                        "provider": line.get("provider"),
                        "spread": line.get("spread"),
                        "formatted_spread": line.get("formattedSpread"),
                        "over_under": line.get("overUnder"),
                        "home_moneyline": line.get("homeMoneyline"),
                        "away_moneyline": line.get("awayMoneyline"),
                    }

    finally:
        client.close()


@dlt.resource(
    name="team_ats",
    write_disposition="merge",
    primary_key=["year", "team_id"],
)
def team_ats_resource(years: list[int]) -> Iterator[dict]:
    """Load team against-the-spread (ATS) records.

    Args:
        years: List of years to load ATS records for
    """
    client = get_client()
    try:
        for year in years:
            logger.info(f"Loading team ATS for {year}...")

            data = make_request(client, "/teams/ats", params={"year": year})

            for team in data:
                team["year"] = year
                yield team

    finally:
        client.close()


@dlt.resource(
    name="line_snapshots",
    write_disposition="append",
)
def line_snapshots_resource(years: list[int]) -> Iterator[dict]:
    """Capture a snapshot of betting lines for pending (not-yet-final) games.

    Append-only time series: no primary key, no merge/dedup. Every row
    yielded in a single run shares the same ``captured_at`` timestamp so
    consumers can group by run. Completed games (non-null home/away score)
    are skipped -- their lines are final and already captured by
    ``lines_resource``. Makes its own API calls rather than sharing a
    generator with ``lines_resource`` (a handful of extra calls/run).

    Args:
        years: List of years to snapshot lines for
    """
    captured_at = datetime.now(UTC)
    client = get_client()
    try:
        for year in years:
            logger.info(f"Capturing line snapshot for {year}...")

            data = make_request(client, "/lines", params={"year": year})

            for game in data:
                # Pending games only -- once a game has a final score its
                # lines are immutable and already captured via lines_resource.
                if game.get("homeScore") is not None or game.get("awayScore") is not None:
                    continue

                game_id = game.get("id")
                lines = game.get("lines", [])

                for line in lines:
                    spread = line.get("spread")
                    formatted_spread = line.get("formattedSpread")
                    over_under = line.get("overUnder")
                    home_moneyline = line.get("homeMoneyline")
                    away_moneyline = line.get("awayMoneyline")

                    hash_input = "|".join(
                        "" if value is None else str(value)
                        for value in (
                            spread,
                            formatted_spread,
                            over_under,
                            home_moneyline,
                            away_moneyline,
                        )
                    )
                    line_hash = hashlib.md5(hash_input.encode()).hexdigest()

                    yield {
                        "captured_at": captured_at,
                        "game_id": game_id,
                        "season": game.get("season"),
                        "week": game.get("week"),
                        "home_team": game.get("homeTeam"),
                        "away_team": game.get("awayTeam"),
                        "provider": line.get("provider"),
                        "spread": spread,
                        "formatted_spread": formatted_spread,
                        "over_under": over_under,
                        "home_moneyline": home_moneyline,
                        "away_moneyline": away_moneyline,
                        "line_hash": line_hash,
                    }

    finally:
        client.close()
