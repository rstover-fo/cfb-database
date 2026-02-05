"""Roster data source - team rosters by season.

Player roster data with team and year context.
"""

import logging
from collections.abc import Iterator

import dlt
from dlt.sources import DltSource

from ..config.years import YEAR_RANGES, get_current_season
from ..utils.api_client import get_client
from .base import make_request

logger = logging.getLogger(__name__)


@dlt.source(name="cfbd_rosters")
def rosters_source(
    teams: list[str] | None = None,
    years: list[int] | None = None,
    mode: str = "incremental",
) -> DltSource:
    """Source for roster data.

    Args:
        teams: List of team names. If None, requires explicit team list.
        years: Specific years to load. If None, uses mode to determine years.
        mode: "incremental" loads current season, "backfill" loads all historical.
    """
    if teams is None:
        raise ValueError(
            "teams parameter is required. Provide a list of team names, "
            "e.g., teams=['Alabama', 'Georgia', 'Ohio State']"
        )

    if years is None:
        if mode == "incremental":
            years = [get_current_season()]
        else:  # backfill
            years = YEAR_RANGES["stats"].to_list()

    return [
        rosters_resource(teams=teams, years=years),
    ]


@dlt.resource(
    name="rosters",
    write_disposition="merge",
    primary_key=["id", "team", "year"],
)
def rosters_resource(
    teams: list[str],
    years: list[int],
) -> Iterator[dict]:
    """Load team rosters for specified teams and years.

    Args:
        teams: List of team names (e.g., ["Alabama", "Georgia"])
        years: List of seasons (e.g., [2023, 2024])

    Yields:
        Player roster records with team/year context added
    """
    client = get_client()
    try:
        for team in teams:
            for year in years:
                logger.info(f"Loading roster for {team} {year}...")

                try:
                    players = make_request(client, "/roster", params={"team": team, "year": year})

                    for player in players:
                        # Add context fields for PK and querying
                        player["team"] = team
                        player["year"] = year
                        yield player

                except Exception as e:
                    logger.warning(f"Error fetching roster for {team} {year}: {e}")
                    continue

    finally:
        client.close()
