"""Conference membership data sources - affiliations and realignment changes.

Distinct from `ref.conferences` (reference.py's `/conferences`, conference
metadata only -- names/abbreviations). These two track *membership*: which
team belonged to which conference over what year range, and when a team
changed conferences.
"""

import logging
from collections.abc import Iterator

import dlt
import httpx
from dlt.sources import DltSource

from ..config.years import YEAR_RANGES, get_current_season
from ..utils.api_client import get_client
from .base import make_request

logger = logging.getLogger(__name__)


@dlt.source(name="cfbd_conferences")
def conferences_source(
    years: list[int] | None = None,
    mode: str = "incremental",
) -> DltSource:
    """Source for conference membership data.

    Args:
        years: Specific years to load conference changes for. If None, uses
            mode to determine years. `conference_affiliations` ignores this
            entirely -- it is always a single unfiltered bulk call.
        mode: "incremental" loads current season, "backfill" loads all historical.
    """
    if years is None:
        if mode == "incremental":
            years = [get_current_season()]
        else:  # backfill
            years = YEAR_RANGES["games"].to_list()

    return [
        conference_affiliations_resource(),
        conference_changes_resource(years),
    ]


@dlt.resource(
    name="conference_affiliations",
    write_disposition="merge",
    primary_key=["team_id", "conference_id", "start_year"],
)
def conference_affiliations_resource() -> Iterator[dict]:
    """Load the full team-conference affiliation history, one bulk call.

    `/conferences/affiliations` takes no required parameters and returns
    every team's conference membership range in a single request (3,604
    rows as of the 2026-08-29 probe) -- there is no year loop to run.
    """
    client = get_client()
    try:
        logger.info("Loading conference affiliations (bulk)...")

        data = make_request(client, "/conferences/affiliations")

        yield from data

    finally:
        client.close()


@dlt.resource(
    name="conference_changes",
    write_disposition="merge",
    primary_key=["effective_year", "team_id"],
)
def conference_changes_resource(years: list[int]) -> Iterator[dict]:
    """Load conference realignment changes, one call per year.

    `year` is required -- a parameterless call 400s. The response already
    carries `effectiveYear` (not the request `year` echoed back under a
    different name), so nothing needs to be stamped.

    Args:
        years: List of years to load conference changes for
    """
    client = get_client()
    try:
        for year in years:
            logger.info(f"Loading conference changes for {year}...")

            try:
                data = make_request(client, "/conferences/changes", params={"year": year})
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 400:
                    logger.warning(f"No conference changes for {year} (400 response), skipping")
                    continue
                raise

            yield from data

    finally:
        client.close()
