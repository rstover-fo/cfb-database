"""Recruiting data sources - players, team rankings, transfer portal.

College football recruiting data.
"""

import logging
from collections.abc import Iterator

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
        team_talent_resource(years),
        recruiting_groups_resource(years),
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

            data = make_request(client, "/recruiting/players", params={"year": year})

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

            data = make_request(client, "/recruiting/teams", params={"year": year})

            yield from data

    finally:
        client.close()


@dlt.resource(
    name="transfer_portal",
    write_disposition="merge",
    primary_key=["first_name", "last_name", "origin", "season"],
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

            data = make_request(client, "/player/portal", params={"year": year})

            yield from data

    finally:
        client.close()


@dlt.resource(
    name="team_talent",
    write_disposition="merge",
    primary_key=["year", "team"],
)
def team_talent_resource(years: list[int]) -> Iterator[dict]:
    """Load team talent composite ratings.

    Args:
        years: List of years to load talent data for

    Note: CFBD v2 renamed the /talent response field from "school" to
    "team" -- the merge key follows that rename. See
    src/schemas/migrations/033_team_talent_reset.sql for the one-time
    table reset this required.
    """
    client = get_client()
    try:
        for year in years:
            logger.info(f"Loading team talent for {year}...")

            data = make_request(client, "/talent", params={"year": year})

            yield from data

    finally:
        client.close()


@dlt.resource(
    name="recruiting_groups",
    write_disposition="merge",
    primary_key=["year", "team", "position_group"],
)
def recruiting_groups_resource(years: list[int]) -> Iterator[dict]:
    """Load recruiting data by position group.

    CFBD v2 changed /recruiting/groups to an aggregate over a
    startYear/endYear range (the old year param is gone) and dropped the
    year field from the response entirely. Requesting one year at a time
    and stamping it locally preserves the (year, team, position_group)
    grain the table has always had. positionGroup is nullable in the v2
    schema; the merge key requires it, so a missing value gets the
    "All Positions" label CFBD uses for the cross-position aggregate.

    Args:
        years: List of years to load recruiting groups for
    """
    client = get_client()
    try:
        for year in years:
            logger.info(f"Loading recruiting groups for {year}...")

            data = make_request(
                client, "/recruiting/groups", params={"startYear": year, "endYear": year}
            )

            for row in data:
                row["year"] = year
                if not row.get("positionGroup") and not row.get("position_group"):
                    row["positionGroup"] = "All Positions"
                yield row

    finally:
        client.close()
