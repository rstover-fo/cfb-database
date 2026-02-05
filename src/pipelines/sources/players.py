"""Player data sources - player search and roster info.

Player biographical and roster data.
"""

import logging
from collections.abc import Iterator

import dlt
from dlt.sources import DltSource

from ..config.years import YEAR_RANGES, get_current_season
from ..utils.api_client import get_client
from .base import make_request

logger = logging.getLogger(__name__)


@dlt.source(name="cfbd_players")
def players_source(
    years: list[int] | None = None,
    mode: str = "incremental",
) -> DltSource:
    """Source for player data.

    Args:
        years: Specific years to load. If None, uses mode to determine years.
        mode: "incremental" loads current season, "backfill" loads all historical.
    """
    if years is None:
        if mode == "incremental":
            years = [get_current_season()]
        else:  # backfill
            years = YEAR_RANGES["stats"].to_list()

    return [
        player_search_resource(years),
    ]


@dlt.resource(
    name="player_search",
    write_disposition="merge",
    primary_key="id",
)
def player_search_resource(years: list[int]) -> Iterator[dict]:
    """Load player search data for specified years.

    Args:
        years: List of years to load players for
    """
    client = get_client()
    try:
        for year in years:
            logger.info(f"Loading player search for {year}...")

            data = make_request(client, "/player/search", params={"year": year})

            yield from data

    finally:
        client.close()
