"""Stats data sources - team and player statistics.

Includes season stats, game stats for teams and players.
"""

import logging
from collections.abc import Iterator

import dlt
from dlt.sources import DltSource

from ..config.years import YEAR_RANGES, get_current_season
from ..utils.api_client import get_client
from .base import make_request

logger = logging.getLogger(__name__)


@dlt.source(name="cfbd_stats")
def stats_source(
    years: list[int] | None = None,
    mode: str = "incremental",
) -> DltSource:
    """Source for team and player statistics.

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
        team_season_stats_resource(years),
        player_season_stats_resource(years),
        advanced_team_stats_resource(years),
        advanced_game_stats_resource(years),
        player_usage_resource(years),
        player_returning_resource(years),
    ]


@dlt.resource(
    name="team_season_stats",
    write_disposition="merge",
    primary_key=["season", "team", "stat_name"],
)
def team_season_stats_resource(years: list[int]) -> Iterator[dict]:
    """Load team season statistics.

    Args:
        years: List of years to load stats for
    """
    client = get_client()
    try:
        for year in years:
            logger.info(f"Loading team season stats for {year}...")

            data = make_request(client, "/stats/season", params={"year": year})

            for stat in data:
                stat["season"] = year
                yield stat

    finally:
        client.close()


@dlt.resource(
    name="player_season_stats",
    write_disposition="merge",
    primary_key=["player_id", "season", "category", "stat_type"],
)
def player_season_stats_resource(years: list[int]) -> Iterator[dict]:
    """Load player season statistics.

    Args:
        years: List of years to load stats for
    """
    client = get_client()
    try:
        for year in years:
            logger.info(f"Loading player season stats for {year}...")

            # Player stats endpoint can return a lot of data
            # We'll load by category to manage memory
            categories = [
                "passing", "rushing", "receiving", "fumbles",
                "defensive", "interceptions", "punting", "kicking",
                "kickReturns", "puntReturns",
            ]

            for category in categories:
                logger.info(f"  Category: {category}...")
                data = make_request(
                    client,
                    "/stats/player/season",
                    params={"year": year, "category": category}
                )

                for stat in data:
                    stat["season"] = year
                    stat["category"] = category
                    # API returns statType (e.g., "YDS", "TD"); ensure it's present for PK
                    stat["stat_type"] = stat.get("statType", stat.get("stat_type", "unknown"))
                    yield stat

    finally:
        client.close()


@dlt.resource(
    name="advanced_team_stats",
    write_disposition="merge",
    primary_key=["season", "team"],
)
def advanced_team_stats_resource(years: list[int]) -> Iterator[dict]:
    """Load advanced team statistics (EPA, success rates, etc).

    Args:
        years: List of years to load stats for
    """
    client = get_client()
    try:
        for year in years:
            logger.info(f"Loading advanced team stats for {year}...")

            data = make_request(
                client,
                "/stats/season/advanced",
                params={"year": year}
            )

            for stat in data:
                stat["season"] = year
                yield stat

    finally:
        client.close()


@dlt.resource(
    name="advanced_game_stats",
    write_disposition="merge",
    primary_key=["game_id", "team"],
)
def advanced_game_stats_resource(years: list[int]) -> Iterator[dict]:
    """Load advanced game-level box score stats (EPA, success rates, etc).

    Args:
        years: List of years to load stats for
    """
    client = get_client()
    try:
        for year in years:
            logger.info(f"Loading advanced game stats for {year}...")

            data = make_request(
                client,
                "/game/box/advanced",
                params={"year": year},
            )

            yield from data

    finally:
        client.close()


@dlt.resource(
    name="player_usage",
    write_disposition="merge",
    primary_key=["season", "id"],
)
def player_usage_resource(years: list[int]) -> Iterator[dict]:
    """Load player usage metrics.

    Args:
        years: List of years to load usage for
    """
    client = get_client()
    try:
        for year in years:
            logger.info(f"Loading player usage for {year}...")

            data = make_request(
                client,
                "/player/usage",
                params={"year": year},
            )

            yield from data

    finally:
        client.close()


@dlt.resource(
    name="player_returning",
    write_disposition="merge",
    primary_key=["season", "team"],
)
def player_returning_resource(years: list[int]) -> Iterator[dict]:
    """Load returning player production data.

    Args:
        years: List of years to load returning production for
    """
    client = get_client()
    try:
        for year in years:
            logger.info(f"Loading player returning production for {year}...")

            data = make_request(
                client,
                "/player/returning",
                params={"year": year},
            )

            yield from data

    finally:
        client.close()
