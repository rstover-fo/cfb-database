"""NFL Draft data sources.

Draft picks with college player information.
"""

import logging
from typing import Iterator

import dlt
from dlt.sources import DltSource

from ..config.years import YEAR_RANGES, get_current_season
from ..utils.api_client import get_client
from .base import make_request

logger = logging.getLogger(__name__)


@dlt.source(name="cfbd_draft")
def draft_source(
    years: list[int] | None = None,
    mode: str = "incremental",
) -> DltSource:
    """Source for NFL draft data.

    Args:
        years: Specific years to load. If None, uses mode to determine years.
        mode: "incremental" loads current season, "backfill" loads all historical.
    """
    if years is None:
        if mode == "incremental":
            years = [get_current_season()]
        else:  # backfill
            years = YEAR_RANGES["draft"].to_list()

    return [
        draft_picks_resource(years),
    ]


@dlt.resource(
    name="draft_picks",
    write_disposition="merge",
    primary_key=["year", "overall"],
)
def draft_picks_resource(years: list[int]) -> Iterator[dict]:
    """Load NFL draft picks.

    Args:
        years: List of years to load draft picks for
    """
    client = get_client()
    try:
        for year in years:
            logger.info(f"Loading draft picks for {year}...")

            data = make_request(client, "/draft/picks", params={"year": year})

            for pick in data:
                yield pick

    finally:
        client.close()
