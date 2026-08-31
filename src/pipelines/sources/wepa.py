"""WEPA (Wins-adjusted EPA) data sources - opponent-adjusted EPA metrics.

These metrics adjust EPA for opponent strength, providing more accurate
team and player efficiency comparisons.
"""

import logging
from collections.abc import Iterator

import dlt
from dlt.sources import DltSource

from ..config.years import YEAR_RANGES, get_current_season
from ..utils.api_client import get_client
from .base import make_request

logger = logging.getLogger(__name__)


def _stamp_player_id(player: dict, resource_name: str) -> dict | None:
    """Ensure `player["id"]` is populated before dlt normalizes the row.

    CFBD renamed the player-id field upstream on at least one of the
    /wepa/players/* endpoints sometime after the existing 2014-2025 rows
    were loaded (observed 2026-08-30, backfill run 33333482499: dlt raised
    UnboundColumnException on wepa_players_passing -- "id ... did not
    receive any data" -- even though the endpoint returned HTTP 200). We did
    not probe the exact new field name before diagnosing this, so this
    coalesces across the plausible candidates instead of guessing one:
    `playerId`, `athleteId`, or a nested `athlete.id`. Coalescing (rather
    than a straight rename) keeps primary-key continuity with the existing
    `id` column, which is text, so the winning candidate is cast with
    str() to keep the column type stable no matter which candidate it came
    from. The original renamed field is left in the record too -- dlt just
    adds it as a new column, and it tells us after the run what CFBD
    actually renamed it to.

    Returns the record with `id` populated, or None if no candidate exists
    at all. A caller that gets None must skip the record (yield nothing for
    it) rather than let dlt's normalize step raise on the non-nullable PK
    column and kill the whole resource -- and, per the stats.py
    play_stats lesson, its sibling resources' already-fetched packages too.
    """
    if player.get("id") is not None:
        return player

    athlete = player.get("athlete")
    candidate = None
    for value in (
        player.get("playerId"),
        player.get("athleteId"),
        athlete.get("id") if isinstance(athlete, dict) else None,
    ):
        if value is not None:
            candidate = value
            break

    if candidate is None:
        logger.warning(
            "%s: no player id found on record (checked id/playerId/athleteId/"
            "athlete.id) -- skipping. keys=%s",
            resource_name,
            sorted(player.keys()),
        )
        return None

    player["id"] = str(candidate)
    return player


@dlt.source(name="cfbd_wepa")
def wepa_source(
    years: list[int] | None = None,
    mode: str = "incremental",
) -> DltSource:
    """Source for WEPA (opponent-adjusted EPA) data.

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
    primary_key=["year", "team"],
)
def wepa_team_season_resource(years: list[int]) -> Iterator[dict]:
    """Load team season WEPA (opponent-adjusted EPA) data.

    Args:
        years: List of years to load WEPA for

    Yields:
        Team season WEPA records with offense/defense breakdown
    """
    client = get_client()
    try:
        for year in years:
            logger.info(f"Loading team WEPA for {year}...")

            data = make_request(client, "/wepa/team/season", params={"year": year})

            for team in data:
                team["year"] = year
                yield team

    finally:
        client.close()


@dlt.resource(
    name="wepa_players_passing",
    write_disposition="merge",
    primary_key=["id", "year"],
)
def wepa_players_passing_resource(years: list[int]) -> Iterator[dict]:
    """Load player passing WEPA data.

    Args:
        years: List of years to load WEPA for

    Yields:
        Player passing WEPA records
    """
    client = get_client()
    try:
        for year in years:
            logger.info(f"Loading player passing WEPA for {year}...")

            data = make_request(client, "/wepa/players/passing", params={"year": year})

            for player in data:
                player["year"] = year
                player = _stamp_player_id(player, "wepa_players_passing")
                if player is None:
                    continue
                yield player

    finally:
        client.close()


@dlt.resource(
    name="wepa_players_rushing",
    write_disposition="merge",
    primary_key=["id", "year"],
)
def wepa_players_rushing_resource(years: list[int]) -> Iterator[dict]:
    """Load player rushing WEPA data.

    Args:
        years: List of years to load WEPA for

    Yields:
        Player rushing WEPA records
    """
    client = get_client()
    try:
        for year in years:
            logger.info(f"Loading player rushing WEPA for {year}...")

            data = make_request(client, "/wepa/players/rushing", params={"year": year})

            for player in data:
                player["year"] = year
                player = _stamp_player_id(player, "wepa_players_rushing")
                if player is None:
                    continue
                yield player

    finally:
        client.close()


@dlt.resource(
    name="wepa_players_kicking",
    write_disposition="merge",
    primary_key=["id", "year"],
)
def wepa_players_kicking_resource(years: list[int]) -> Iterator[dict]:
    """Load player kicking WEPA data.

    Args:
        years: List of years to load WEPA for

    Yields:
        Player kicking WEPA records
    """
    client = get_client()
    try:
        for year in years:
            logger.info(f"Loading player kicking WEPA for {year}...")

            data = make_request(client, "/wepa/players/kicking", params={"year": year})

            for player in data:
                player["year"] = year
                player = _stamp_player_id(player, "wepa_players_kicking")
                if player is None:
                    continue
                yield player

    finally:
        client.close()
