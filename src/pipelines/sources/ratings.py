"""Ratings data sources - SP+, Elo, FPI, SRS.

Team ratings and rankings by various systems.
"""

import logging
from collections.abc import Iterator

import dlt
from dlt.sources import DltSource

from ..config.years import YEAR_RANGES, get_current_season
from ..utils.api_client import get_client
from .base import make_request

logger = logging.getLogger(__name__)


@dlt.source(name="cfbd_ratings")
def ratings_source(
    years: list[int] | None = None,
    mode: str = "incremental",
) -> DltSource:
    """Source for team ratings data.

    Args:
        years: Specific years to load. If None, uses mode to determine years.
        mode: "incremental" loads current season, "backfill" loads all historical.
    """
    if years is None:
        if mode == "incremental":
            years = [get_current_season()]
        else:  # backfill
            years = YEAR_RANGES["ratings"].to_list()

    return [
        sp_ratings_resource(years),
        elo_ratings_resource(years),
        fpi_ratings_resource(years),
        srs_ratings_resource(years),
        sp_conference_ratings_resource(years),
    ]


@dlt.resource(
    name="sp_ratings",
    write_disposition="merge",
    primary_key=["year", "team"],
)
def sp_ratings_resource(years: list[int]) -> Iterator[dict]:
    """Load SP+ ratings (Bill Connelly's system).

    Args:
        years: List of years to load ratings for
    """
    client = get_client()
    try:
        for year in years:
            logger.info(f"Loading SP+ ratings for {year}...")

            data = make_request(client, "/ratings/sp", params={"year": year})

            for rating in data:
                rating["year"] = year
                yield rating

    finally:
        client.close()


@dlt.resource(
    name="elo_ratings",
    write_disposition="merge",
    primary_key=["year", "team"],
)
def elo_ratings_resource(years: list[int]) -> Iterator[dict]:
    """Load Elo ratings.

    Args:
        years: List of years to load ratings for
    """
    client = get_client()
    try:
        for year in years:
            logger.info(f"Loading Elo ratings for {year}...")

            data = make_request(client, "/ratings/elo", params={"year": year})

            for rating in data:
                rating["year"] = year
                yield rating

    finally:
        client.close()


@dlt.resource(
    name="fpi_ratings",
    write_disposition="merge",
    primary_key=["year", "team"],
)
def fpi_ratings_resource(years: list[int]) -> Iterator[dict]:
    """Load ESPN FPI ratings.

    Args:
        years: List of years to load ratings for
    """
    client = get_client()
    try:
        for year in years:
            logger.info(f"Loading FPI ratings for {year}...")

            data = make_request(client, "/ratings/fpi", params={"year": year})

            for rating in data:
                rating["year"] = year
                yield rating

    finally:
        client.close()


@dlt.resource(
    name="srs_ratings",
    write_disposition="merge",
    primary_key=["year", "team"],
)
def srs_ratings_resource(years: list[int]) -> Iterator[dict]:
    """Load Simple Rating System (SRS) ratings.

    Args:
        years: List of years to load ratings for
    """
    client = get_client()
    try:
        for year in years:
            logger.info(f"Loading SRS ratings for {year}...")

            data = make_request(client, "/ratings/srs", params={"year": year})

            for rating in data:
                rating["year"] = year
                yield rating

    finally:
        client.close()


@dlt.resource(
    name="sp_conference_ratings",
    write_disposition="merge",
    primary_key=["year", "conference"],
)
def sp_conference_ratings_resource(years: list[int]) -> Iterator[dict]:
    """Load SP+ conference-level ratings.

    Args:
        years: List of years to load conference ratings for
    """
    client = get_client()
    try:
        for year in years:
            logger.info(f"Loading SP+ conference ratings for {year}...")

            data = make_request(
                client, "/ratings/sp/conferences", params={"year": year}
            )

            for rating in data:
                rating["year"] = year
                yield rating

    finally:
        client.close()
