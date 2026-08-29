"""Ratings data sources - SP+, Elo, FPI, SRS, CORE.

Team ratings and rankings by various systems.
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

# CFBD publishes retrospective CORE ratings from 2016 onward; earlier years
# return nothing, so a full backfill skips them instead of spending calls.
CORE_RATINGS_START = 2016


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
        core_ratings_resource(years),
        sp_conference_ratings_resource(years),
        srs_expanded_ratings_resource(years),
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
    name="core_ratings",
    write_disposition="merge",
    primary_key=["year", "team"],
)
def core_ratings_resource(years: list[int]) -> Iterator[dict]:
    """Load CORE (Context & Opponent-Relative Efficiency) ratings.

    One snapshot row per team per season; through_week/through_season_type
    advance in-season and merge keeps the latest.

    Args:
        years: List of years to load ratings for
    """
    client = get_client()
    try:
        for year in years:
            if year < CORE_RATINGS_START:
                logger.info(f"Skipping CORE ratings for {year} (published from 2016)")
                continue

            logger.info(f"Loading CORE ratings for {year}...")

            data = make_request(client, "/ratings/core", params={"year": year})

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

            data = make_request(client, "/ratings/sp/conferences", params={"year": year})

            for rating in data:
                rating["year"] = year
                yield rating

    finally:
        client.close()


@dlt.resource(
    name="srs_expanded",
    write_disposition="merge",
    primary_key=["year", "team"],
)
def srs_expanded_ratings_resource(years: list[int]) -> Iterator[dict]:
    """Load expanded SRS (Simple Rating System) ratings.

    Unlike `srs_ratings` (`/ratings/srs`), this includes classification,
    conference, and division alongside the rating -- useful for filtering to
    FBS/FCS or building conference rollups without a join. `year` is a
    required top-level field on every returned row, so nothing is stamped.

    A pre-2005 year has not been verified against the live API (the
    2026-08-29 probe only confirmed 2005 onward, 235 rows). Both a 400 and
    an empty-200 are treated as "nothing to load for this year" rather than
    an error, so an unverified early year degrades safely instead of
    failing the whole resource -- unlike CORE_RATINGS_START above, there is
    no hardcoded skip year here because the boundary is unconfirmed.

    Args:
        years: List of years to load SRS-expanded ratings for
    """
    client = get_client()
    try:
        for year in years:
            logger.info(f"Loading expanded SRS ratings for {year}...")

            try:
                data = make_request(client, "/ratings/srs/expanded", params={"year": year})
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 400:
                    logger.warning(f"No expanded SRS ratings for {year} (400 response), skipping")
                    continue
                raise

            if not data:
                logger.info(f"No expanded SRS ratings data for {year}, skipping")
                continue

            yield from data

    finally:
        client.close()
