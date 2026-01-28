"""WEPA (Wins-adjusted EPA) data sources - opponent-adjusted EPA metrics.

These metrics adjust EPA for opponent strength, providing more accurate
team and player efficiency comparisons.
"""

import logging
from collections.abc import Iterator

import dlt
from dlt.sources import DltSource

from ..config.years import YEAR_RANGES, get_current_season
from ..utils.api_client import get_client
from .base import make_request

logger = logging.getLogger(__name__)


@dlt.source(name="cfbd_wepa")
def wepa_source(
    years: list[int] | None = None,
    mode: str = "incremental",
) -> DltSource:
    """Source for WEPA (opponent-adjusted EPA) data.

    Args:
        years: Specific years to load. If None, uses mode to determine years.
        mode: "incremental" loads current season, "backfill" loads all historical.
    """
    if years is None:
        if mode == "incremental":
            years = [get_current_season()]
        else:  # backfill
            years = YEAR_RANGES["metrics"].to_list()

    return [
        wepa_team_season_resource(years),
        wepa_players_passing_resource(years),
        wepa_players_rushing_resource(years),
        wepa_players_kicking_resource(years),
    ]


@dlt.resource(
    name="wepa_team_season",
    write_disposition="merge",
    primary_key=["year", "team"],
)
def wepa_team_season_resource(years: list[int]) -> Iterator[dict]:
    """Load team season WEPA (opponent-adjusted EPA) data.

    Args:
        years: List of years to load WEPA for

    Yields:
        Team season WEPA records with offense/defense breakdown
    """
    client = get_client()
    try:
        for year in years:
            logger.info(f"Loading team WEPA for {year}...")

            data = make_request(
                client,
                "/wepa/team/season",
                params={"year": year}
            )

            for team in data:
                team["year"] = year
                yield team

    finally:
        client.close()


@dlt.resource(
    name="wepa_players_passing",
    write_disposition="merge",
    primary_key=["id", "year"],
)
def wepa_players_passing_resource(years: list[int]) -> Iterator[dict]:
    """Load player passing WEPA data.

    Args:
        years: List of years to load WEPA for

    Yields:
        Player passing WEPA records
    """
    client = get_client()
    try:
        for year in years:
            logger.info(f"Loading player passing WEPA for {year}...")

            data = make_request(
                client,
                "/wepa/players/passing",
                params={"year": year}
            )

            for player in data:
                player["year"] = year
                yield player

    finally:
        client.close()


@dlt.resource(
    name="wepa_players_rushing",
    write_disposition="merge",
    primary_key=["id", "year"],
)
def wepa_players_rushing_resource(years: list[int]) -> Iterator[dict]:
    """Load player rushing WEPA data.

    Args:
        years: List of years to load WEPA for

    Yields:
        Player rushing WEPA records
    """
    client = get_client()
    try:
        for year in years:
            logger.info(f"Loading player rushing WEPA for {year}...")

            data = make_request(
                client,
                "/wepa/players/rushing",
                params={"year": year}
            )

            for player in data:
                player["year"] = year
                yield player

    finally:
        client.close()


@dlt.resource(
    name="wepa_players_kicking",
    write_disposition="merge",
    primary_key=["id", "year"],
)
def wepa_players_kicking_resource(years: list[int]) -> Iterator[dict]:
    """Load player kicking WEPA data.

    Args:
        years: List of years to load WEPA for

    Yields:
        Player kicking WEPA records
    """
    client = get_client()
    try:
        for year in years:
            logger.info(f"Loading player kicking WEPA for {year}...")

            data = make_request(
                client,
                "/wepa/players/kicking",
                params={"year": year}
            )

            for player in data:
                player["year"] = year
                yield player

    finally:
        client.close()
