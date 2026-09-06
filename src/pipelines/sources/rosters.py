"""Roster data source - team rosters by season.

Player roster data with team and year context.
"""

import logging
from collections.abc import Iterator

import dlt
from dlt.sources import DltSource

from ..config.years import YEAR_RANGES, get_current_season
from ..utils.api_client import get_client
from ..utils.request_outcomes import RequestOutcomeTracker, validate_record_list
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
    # Every consumer (api.roster_lookup, marts 011/017/020/025/045/050, the
    # scouting player mart, public.player_search) reads core.roster
    # (singular). Without this, dlt derives the table from the resource
    # name and writes core.rosters -- a parallel table nobody reads, which
    # is where the 2026 rosters landed on 2026-09-03 while core.roster
    # stopped at 2025. Same 17 columns + roster__recruit_ids child.
    table_name="roster",
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
    requests = [{"team": team, "year": year} for team in teams for year in years]
    tracker = RequestOutcomeTracker("/roster", len(requests), logger)
    try:
        for params in requests:
            logger.info("Loading roster for %s %s...", params["team"], params["year"])
            try:
                players = make_request(client, "/roster", params=params)
                records = validate_record_list(players)
            except Exception as error:
                raise tracker.failure(params, error) from error

            tracker.record_response(params, records)
            for player in records:
                # Add context fields for PK and querying
                player["team"] = params["team"]
                player["year"] = params["year"]
                yield player

    finally:
        client.close()
