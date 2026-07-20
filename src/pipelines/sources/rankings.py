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
    primary_key=["season", "season_type", "week", "poll", "school"],
)
def rankings_resource(years: list[int]) -> Iterator[dict]:
    """Load poll rankings for specified years and weeks.

    Iterates by year and week (1-15 regular season + postseason).

    The merge key is [season, season_type, week, poll, school] -- NOT rank.
    Keying on rank loses data two ways (both confirmed live, 2026-07-20):
    tied teams share a rank (AP 2024 week 10 had two teams at #11, so one
    was silently dropped -- 55 AP weeks were short), and CFBD reports the
    postseason (final) poll as week 1, colliding with regular-season week 1
    unless season_type is part of the key. A team appears at most once per
    poll per week, so school is the collision-free choice.

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
                    season_type = week_data.get("seasonType", "regular")
                    for poll in week_data.get("polls", []):
                        poll_name = poll.get("poll", "unknown")
                        for rank_entry in poll.get("ranks", []):
                            rank_entry["season"] = season
                            rank_entry["season_type"] = season_type
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
                # CFBD reports the final poll as week 1; season_type (part of
                # the merge key) is what distinguishes it from regular week 1.
                week_num = week_data.get("week", 1)
                season_type = week_data.get("seasonType", "postseason")
                for poll in week_data.get("polls", []):
                    poll_name = poll.get("poll", "unknown")
                    for rank_entry in poll.get("ranks", []):
                        rank_entry["season"] = season
                        rank_entry["season_type"] = season_type
                        rank_entry["week"] = week_num
                        rank_entry["poll"] = poll_name
                        yield rank_entry

    finally:
        client.close()
