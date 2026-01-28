"""Rankings data sources - poll rankings by week.

AP, Coaches, CFP, and other poll rankings.
"""

import logging
from collections.abc import Iterator

import dlt
from dlt.sources import DltSource

from ..config.years import YEAR_RANGES, get_current_season
from ..utils.api_client import get_client
from .base import make_request

logger = logging.getLogger(__name__)


@dlt.source(name="cfbd_rankings")
def rankings_source(
    years: list[int] | None = None,
    mode: str = "incremental",
) -> DltSource:
    """Source for poll rankings data.

    Args:
        years: Specific years to load. If None, uses mode to determine years.
        mode: "incremental" loads current season, "backfill" loads all historical.
    """
    if years is None:
        if mode == "incremental":
            years = [get_current_season()]
        else:  # backfill
            years = YEAR_RANGES["games_modern"].to_list()

    return [
        rankings_resource(years),
    ]


@dlt.resource(
    name="rankings",
    write_disposition="merge",
    primary_key=["season", "week", "poll", "rank"],
)
def rankings_resource(years: list[int]) -> Iterator[dict]:
    """Load poll rankings for specified years and weeks.

    Iterates by year and week (1-15 regular season + postseason).

    Args:
        years: List of years to load rankings for
    """
    client = get_client()
    try:
        for year in years:
            logger.info(f"Loading rankings for {year}...")

            for week in range(1, 16):
                logger.info(f"  Week {week}...")
                data = make_request(
                    client,
                    "/rankings",
                    params={
                        "year": year,
                        "week": week,
                        "seasonType": "regular",
                    },
                )

                for week_data in data:
                    season = week_data.get("season", year)
                    week_num = week_data.get("week", week)
                    for poll in week_data.get("polls", []):
                        poll_name = poll.get("poll", "unknown")
                        for rank_entry in poll.get("ranks", []):
                            rank_entry["season"] = season
                            rank_entry["week"] = week_num
                            rank_entry["poll"] = poll_name
                            yield rank_entry

            # Postseason rankings
            logger.info("  Postseason...")
            postseason_data = make_request(
                client,
                "/rankings",
                params={
                    "year": year,
                    "seasonType": "postseason",
                    "week": 1,
                },
            )

            for week_data in postseason_data:
                season = week_data.get("season", year)
                week_num = week_data.get("week", "postseason")
                for poll in week_data.get("polls", []):
                    poll_name = poll.get("poll", "unknown")
                    for rank_entry in poll.get("ranks", []):
                        rank_entry["season"] = season
                        rank_entry["week"] = week_num
                        rank_entry["poll"] = poll_name
                        yield rank_entry

    finally:
        client.close()
