"""Betting data sources - lines and spreads.

Game betting lines from various sportsbooks.
"""

import logging
from typing import Iterator

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
