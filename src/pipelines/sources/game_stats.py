"""Game stats source - isolated loading for game_team_stats and game_player_stats.

This source bypasses FK constraints by loading only the stats tables,
not the games/drives tables they reference.
"""

import logging
from collections.abc import Iterator
from typing import Literal

import dlt
from dlt.sources import DltSource

from ..config.years import YEAR_RANGES, get_current_season
from ..utils.api_client import get_client
from .base import make_request

logger = logging.getLogger(__name__)

WriteDisposition = Literal["merge", "replace", "append"]


@dlt.source(name="cfbd_game_stats")
def game_stats_source(
    years: list[int] | None = None,
    mode: str = "incremental",
    disposition: WriteDisposition = "merge",
    season_type: str | None = None,
    weeks: list[int] | None = None,
) -> DltSource:
    """Source for game team/player stats only.

    Use this source for backfilling game stats without triggering
    FK constraint issues from the games/drives tables.

    Args:
        years: Specific years to load. If None, uses mode to determine years.
        mode: "incremental" loads current season, "backfill" loads all historical.
        disposition: Write disposition - "merge", "replace", or "append".
        season_type: "regular" or "postseason". If None, loads both.
        weeks: Specific weeks to load. If None, loads all weeks.
    """
    if years is None:
        if mode == "incremental":
            years = [get_current_season()]
        else:  # backfill
            years = YEAR_RANGES["games_modern"].to_list()

    return [
        game_team_stats_resource(years, disposition, season_type, weeks),
        game_player_stats_resource(years, disposition, season_type, weeks),
    ]


def game_team_stats_resource(
    years: list[int],
    disposition: WriteDisposition = "merge",
    season_type: str | None = None,
    weeks: list[int] | None = None,
):
    """Load team box scores per game for specified years.

    Args:
        years: List of years to load game team stats for
        disposition: Write disposition - "merge", "replace", or "append"
        season_type: "regular" or "postseason". If None, loads both.
        weeks: Specific weeks to load. If None, loads all weeks for each season type.
    """

    @dlt.resource(
        name="game_team_stats",
        write_disposition=disposition,
        primary_key="id",
    )
    def _inner() -> Iterator[dict]:
        client = get_client()
        season_types = [season_type] if season_type else ["regular", "postseason"]
        try:
            for year in years:
                for st in season_types:
                    if weeks:
                        week_range = weeks
                    else:
                        max_week = 15 if st == "regular" else 5
                        week_range = list(range(1, max_week + 1))

                    for week in week_range:
                        logger.info(f"Loading game team stats: {year} {st} week {week}")
                        try:
                            data = make_request(
                                client,
                                "/games/teams",
                                params={
                                    "year": year,
                                    "seasonType": st,
                                    "week": week,
                                },
                            )
                            yield from data
                        except Exception:
                            # Some weeks may not have games (esp postseason)
                            continue
        finally:
            client.close()

    return _inner()


def game_player_stats_resource(
    years: list[int],
    disposition: WriteDisposition = "merge",
    season_type: str | None = None,
    weeks: list[int] | None = None,
):
    """Load player box scores per game for specified years.

    Args:
        years: List of years to load game player stats for
        disposition: Write disposition - "merge", "replace", or "append"
        season_type: "regular" or "postseason". If None, loads both.
        weeks: Specific weeks to load. If None, loads all weeks for each season type.
    """

    @dlt.resource(
        name="game_player_stats",
        write_disposition=disposition,
        primary_key="id",
    )
    def _inner() -> Iterator[dict]:
        client = get_client()
        season_types = [season_type] if season_type else ["regular", "postseason"]
        try:
            for year in years:
                for st in season_types:
                    if weeks:
                        week_range = weeks
                    else:
                        max_week = 15 if st == "regular" else 5
                        week_range = list(range(1, max_week + 1))

                    for week in week_range:
                        logger.info(f"Loading game player stats: {year} {st} week {week}")
                        try:
                            data = make_request(
                                client,
                                "/games/players",
                                params={
                                    "year": year,
                                    "seasonType": st,
                                    "week": week,
                                },
                            )
                            yield from data
                        except Exception:
                            # Some weeks may not have games (esp postseason)
                            continue
        finally:
            client.close()

    return _inner()
