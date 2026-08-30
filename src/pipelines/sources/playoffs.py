"""College Football Playoff data sources - bracket, games, participants.

The current CFP bracket format (`/playoffs/cfp*`) covers the four-team era
onward, starting with the 2014 season -- CFP_START below skips earlier
years so a full backfill doesn't spend calls that always come back empty.
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

# The College Football Playoff began with the 2014 season (four-team era,
# later expanded to twelve). Earlier years return nothing.
CFP_START = 2014


@dlt.source(name="cfbd_playoffs")
def playoffs_source(
    years: list[int] | None = None,
    mode: str = "incremental",
) -> DltSource:
    """Source for College Football Playoff bracket data.

    Args:
        years: Specific years to load. If None, uses mode to determine years.
        mode: "incremental" loads current season, "backfill" loads all historical.
    """
    if years is None:
        if mode == "incremental":
            years = [get_current_season()]
        else:  # backfill
            years = YEAR_RANGES["games"].to_list()

    return [
        cfp_bracket_resource(years),
        cfp_games_resource(years),
        cfp_participants_resource(years),
    ]


@dlt.resource(
    name="cfp_bracket",
    write_disposition="merge",
    primary_key=["season"],
)
def cfp_bracket_resource(years: list[int]) -> Iterator[dict]:
    """Load the CFP bracket document: format, participants, rounds, champion.

    One nested row per season. dlt child-tables the nested `participants`
    and `rounds` arrays (and `rounds`' nested `matchups`/`slots`) via the
    standard `_dlt_parent_id` mechanism -- no manual flattening needed here.

    Response shape is not stable across seasons: the 2024 response is a
    one-element JSON array wrapping the bracket document (with a top-level
    `season` field), while the 2025 response is a bare JSON object -- no
    wrapping array -- which also omits `season`. The array-vs-object
    difference first broke `yield from data` (iterating a dict yields its
    string keys, surfacing as `'str' object has no attribute 'get'` at
    extract time); the missing `season` field separately left the primary
    key unbound and failed normalize. Both are handled here: the response is
    normalized to a list of records (a dict is wrapped as a one-element
    list) before iterating, and `season` is stamped from the request year
    whenever a record doesn't already carry one -- so both shapes load
    correctly.

    Args:
        years: List of years to load the bracket for
    """
    client = get_client()
    try:
        for year in years:
            if year < CFP_START:
                logger.info(f"Skipping CFP bracket for {year} (playoff started {CFP_START})")
                continue

            logger.info(f"Loading CFP bracket for {year}...")

            try:
                data = make_request(client, "/playoffs/cfp", params={"year": year})
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 400:
                    logger.warning(f"No CFP bracket for {year} (400 response), skipping")
                    continue
                raise

            if not data:
                logger.info(f"No CFP bracket data for {year}, skipping")
                continue

            if isinstance(data, dict):
                records = [data]
            elif isinstance(data, list):
                records = data
            else:
                logger.warning(
                    f"Unexpected /playoffs/cfp response type {type(data).__name__} "
                    f"for {year}, skipping"
                )
                continue

            for record in records:
                if not isinstance(record, dict):
                    logger.warning(
                        f"Skipping non-dict CFP bracket record ({type(record).__name__}) for {year}"
                    )
                    continue
                if record.get("season") is None:
                    record["season"] = year
                yield record

    finally:
        client.close()


@dlt.resource(
    name="cfp_games",
    write_disposition="merge",
    primary_key=["season", "id"],
)
def cfp_games_resource(years: list[int]) -> Iterator[dict]:
    """Load CFP bracket-slot games (one row per matchup, e.g. 11 for 2024).

    Unlike `/playoffs/cfp`, this response carries no `season` field --
    stamped here from the request year.

    Args:
        years: List of years to load bracket games for
    """
    client = get_client()
    try:
        for year in years:
            if year < CFP_START:
                logger.info(f"Skipping CFP games for {year} (playoff started {CFP_START})")
                continue

            logger.info(f"Loading CFP games for {year}...")

            try:
                data = make_request(client, "/playoffs/cfp/games", params={"year": year})
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 400:
                    logger.warning(f"No CFP games for {year} (400 response), skipping")
                    continue
                raise

            for row in data:
                row["season"] = year
                yield row

    finally:
        client.close()


@dlt.resource(
    name="cfp_participants",
    write_disposition="merge",
    primary_key=["season", "team__id"],
)
def cfp_participants_resource(years: list[int]) -> Iterator[dict]:
    """Load the CFP participant records for each season (12 in the current format).

    Unlike `/playoffs/cfp`, this response carries no `season` field --
    stamped here from the request year. `team` is a nested object (id,
    school, conference); dlt flattens it into `team__id`, `team__school`,
    `team__conference`.

    Args:
        years: List of years to load participants for
    """
    client = get_client()
    try:
        for year in years:
            if year < CFP_START:
                logger.info(f"Skipping CFP participants for {year} (playoff started {CFP_START})")
                continue

            logger.info(f"Loading CFP participants for {year}...")

            try:
                data = make_request(client, "/playoffs/cfp/participants", params={"year": year})
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 400:
                    logger.warning(f"No CFP participants for {year} (400 response), skipping")
                    continue
                raise

            for row in data:
                row["season"] = year
                yield row

    finally:
        client.close()
