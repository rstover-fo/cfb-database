"""Recruiting data sources - players, team rankings, transfer portal.

College football recruiting data.
"""

import logging
from typing import Iterator

import dlt
from dlt.sources import DltSource

from ..config.years import YEAR_RANGES, get_current_season
from ..utils.api_client import get_client
from .base import make_request

logger = logging.getLogger(__name__)


@dlt.source(name="cfbd_recruiting")
def recruiting_source(
    years: list[int] | None = None,
    mode: str = "incremental",
) -> DltSource:
    """Source for recruiting data.

    Args:
        years: Specific years to load. If None, uses mode to determine years.
        mode: "incremental" loads current season, "backfill" loads all historical.
    """
    if years is None:
        if mode == "incremental":
            years = [get_current_season()]
        else:  # backfill
            years = YEAR_RANGES["recruiting"].to_list()

    return [
        recruits_resource(years),
        team_recruiting_resource(years),
        transfer_portal_resource(years),
    ]


@dlt.resource(
    name="recruits",
    write_disposition="merge",
    primary_key="id",
)
def recruits_resource(years: list[int]) -> Iterator[dict]:
    """Load individual recruit data.

    Args:
        years: List of years to load recruits for
    """
    client = get_client()
    try:
        for year in years:
            logger.info(f"Loading recruits for {year}...")

            data = make_request(
                client, "/recruiting/players", params={"year": year}
            )

            for recruit in data:
                recruit["recruiting_year"] = year
                yield recruit

    finally:
        client.close()


@dlt.resource(
    name="team_recruiting",
    write_disposition="merge",
    primary_key=["year", "team"],
)
def team_recruiting_resource(years: list[int]) -> Iterator[dict]:
    """Load team recruiting rankings.

    Args:
        years: List of years to load rankings for
    """
    client = get_client()
    try:
        for year in years:
            logger.info(f"Loading team recruiting for {year}...")

            data = make_request(
                client, "/recruiting/teams", params={"year": year}
            )

            for team in data:
                yield team

    finally:
        client.close()


@dlt.resource(
    name="transfer_portal",
    write_disposition="merge",
    primary_key=["season", "first_name", "last_name"],
)
def transfer_portal_resource(years: list[int]) -> Iterator[dict]:
    """Load transfer portal entries.

    Args:
        years: List of years to load transfers for
    """
    client = get_client()
    try:
        for year in years:
            logger.info(f"Loading transfer portal for {year}...")

            data = make_request(
                client, "/player/portal", params={"year": year}
            )

            for transfer in data:
                yield transfer

    finally:
        client.close()
