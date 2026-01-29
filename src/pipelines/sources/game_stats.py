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
) -> DltSource:
    """Source for game team/player stats only.

    Use this source for backfilling game stats without triggering
    FK constraint issues from the games/drives tables.

    Args:
        years: Specific years to load. If None, uses mode to determine years.
        mode: "incremental" loads current season, "backfill" loads all historical.
        disposition: Write disposition - "merge", "replace", or "append".
    """
    if years is None:
        if mode == "incremental":
            years = [get_current_season()]
        else:  # backfill
            years = YEAR_RANGES["games_modern"].to_list()

    return [
        game_team_stats_resource(years, disposition),
        game_player_stats_resource(years, disposition),
    ]


def game_team_stats_resource(years: list[int], disposition: WriteDisposition = "merge"):
    """Load team box scores per game for specified years.

    Args:
        years: List of years to load game team stats for
        disposition: Write disposition - "merge", "replace", or "append"
    """

    @dlt.resource(
        name="game_team_stats",
        write_disposition=disposition,
        primary_key="id",
    )
    def _inner() -> Iterator[dict]:
        client = get_client()
        try:
            for year in years:
                logger.info(f"Loading game team stats for {year}...")

                # CFBD games/teams endpoint requires week parameter
                # Regular season: weeks 1-15, postseason: weeks 1-5
                for season_type in ["regular", "postseason"]:
                    max_week = 15 if season_type == "regular" else 5
                    for week in range(1, max_week + 1):
                        try:
                            data = make_request(
                                client,
                                "/games/teams",
                                params={
                                    "year": year,
                                    "seasonType": season_type,
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


def game_player_stats_resource(years: list[int], disposition: WriteDisposition = "merge"):
    """Load player box scores per game for specified years.

    Args:
        years: List of years to load game player stats for
        disposition: Write disposition - "merge", "replace", or "append"
    """

    @dlt.resource(
        name="game_player_stats",
        write_disposition=disposition,
        primary_key="id",
    )
    def _inner() -> Iterator[dict]:
        client = get_client()
        try:
            for year in years:
                logger.info(f"Loading game player stats for {year}...")

                # CFBD games/players endpoint requires week parameter
                # Regular season: weeks 1-15, postseason: weeks 1-5
                for season_type in ["regular", "postseason"]:
                    max_week = 15 if season_type == "regular" else 5
                    for week in range(1, max_week + 1):
                        try:
                            data = make_request(
                                client,
                                "/games/players",
                                params={
                                    "year": year,
                                    "seasonType": season_type,
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
