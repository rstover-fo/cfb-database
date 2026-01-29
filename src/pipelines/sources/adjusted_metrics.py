"""Adjusted metrics data sources - WEPA (opponent-adjusted stats).

WEPA (Win-Expected Points Added) adjusts performance metrics for opponent quality.
"""

import logging
from collections.abc import Iterator

import dlt
from dlt.sources import DltSource

from ..config.years import YEAR_RANGES, get_current_season
from ..utils.api_client import get_client
from .base import make_request

logger = logging.getLogger(__name__)


@dlt.source(name="cfbd_adjusted_metrics")
def adjusted_metrics_source(
    years: list[int] | None = None,
    mode: str = "incremental",
) -> DltSource:
    """Source for WEPA (opponent-adjusted) metrics.

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
    primary_key=["year", "team_id"],
)
def wepa_team_season_resource(years: list[int]) -> Iterator[dict]:
    """Load team WEPA (opponent-adjusted) season stats.

    Args:
        years: List of years to load WEPA for
    """
    client = get_client()
    try:
        for year in years:
            logger.info(f"Loading team WEPA for {year}...")

            data = make_request(
                client, "/wepa/team/season", params={"year": year}
            )

            for team in data:
                yield team

    finally:
        client.close()


@dlt.resource(
    name="wepa_players_passing",
    write_disposition="merge",
    primary_key=["year", "athlete_id"],
)
def wepa_players_passing_resource(years: list[int]) -> Iterator[dict]:
    """Load player WEPA passing stats (opponent-adjusted).

    Args:
        years: List of years to load WEPA for
    """
    client = get_client()
    try:
        for year in years:
            logger.info(f"Loading player passing WEPA for {year}...")

            data = make_request(
                client, "/wepa/players/passing", params={"year": year}
            )

            for player in data:
                yield player

    finally:
        client.close()


@dlt.resource(
    name="wepa_players_rushing",
    write_disposition="merge",
    primary_key=["year", "athlete_id"],
)
def wepa_players_rushing_resource(years: list[int]) -> Iterator[dict]:
    """Load player WEPA rushing stats (opponent-adjusted).

    Args:
        years: List of years to load WEPA for
    """
    client = get_client()
    try:
        for year in years:
            logger.info(f"Loading player rushing WEPA for {year}...")

            data = make_request(
                client, "/wepa/players/rushing", params={"year": year}
            )

            for player in data:
                yield player

    finally:
        client.close()


@dlt.resource(
    name="wepa_players_kicking",
    write_disposition="merge",
    primary_key=["year", "athlete_id"],
)
def wepa_players_kicking_resource(years: list[int]) -> Iterator[dict]:
    """Load player WEPA kicking stats (PAAR - Points Added Above Replacement).

    Args:
        years: List of years to load WEPA for
    """
    client = get_client()
    try:
        for year in years:
            logger.info(f"Loading player kicking WEPA for {year}...")

            data = make_request(
                client, "/wepa/players/kicking", params={"year": year}
            )

            for player in data:
                yield player

    finally:
        client.close()
