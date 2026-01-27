"""Play-by-play data source - year-iterated loading.

This is the largest dataset - play-by-play data from 2004-present.
"""

import logging
from typing import Iterator

import dlt
from dlt.sources import DltSource

from ..config.years import YEAR_RANGES, get_current_season
from ..utils.api_client import get_client
from .base import make_request

logger = logging.getLogger(__name__)


@dlt.source(name="cfbd_plays")
def plays_source(
    years: list[int] | None = None,
    mode: str = "incremental",
) -> DltSource:
    """Source for play-by-play data.

    Args:
        years: Specific years to load. If None, uses mode to determine years.
        mode: "incremental" loads current season, "backfill" loads all historical.
    """
    if years is None:
        if mode == "incremental":
            years = [get_current_season()]
        else:  # backfill
            years = YEAR_RANGES["plays"].to_list()

    return [
        plays_resource(years),
    ]


@dlt.resource(
    name="plays",
    write_disposition="merge",
    primary_key="id",
)
def plays_resource(years: list[int]) -> Iterator[dict]:
    """Load play-by-play data for specified years.

    This is a large dataset - expect ~150k plays per season.
    CFBD requires week parameter for plays endpoint.

    Args:
        years: List of years to load plays for
    """
    client = get_client()
    try:
        for year in years:
            logger.info(f"Loading plays for {year}...")

            # CFBD plays endpoint requires week parameter
            # Regular season: weeks 1-15, Postseason: week 1
            for week in range(1, 16):
                logger.info(f"  Week {week}...")
                data = make_request(
                    client,
                    "/plays",
                    params={
                        "year": year,
                        "week": week,
                        "seasonType": "regular",
                    }
                )

                for play in data:
                    play["season"] = year
                    yield play

            # Postseason plays (CFBD requires week parameter)
            logger.info("  Postseason...")
            postseason_data = make_request(
                client,
                "/plays",
                params={
                    "year": year,
                    "seasonType": "postseason",
                    "week": 1,  # All postseason games are in "week 1"
                }
            )

            for play in postseason_data:
                play["season"] = year
                yield play

    finally:
        client.close()
