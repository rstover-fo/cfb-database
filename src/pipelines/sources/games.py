"""Games and drives data sources - year-iterated loading.

These tables contain game event data and require year-based iteration.
"""

import logging
from collections.abc import Iterator

import dlt
from dlt.sources import DltSource

logger = logging.getLogger(__name__)

from ..config.years import YEAR_RANGES, get_current_season
from ..utils.api_client import get_client
from .base import make_request


@dlt.source(name="cfbd_games")
def games_source(
    years: list[int] | None = None,
    mode: str = "incremental",
) -> DltSource:
    """Source for games and drives data.

    Args:
        years: Specific years to load. If None, uses mode to determine years.
        mode: "incremental" loads current season, "backfill" loads all historical.
    """
    if years is None:
        if mode == "incremental":
            years = [get_current_season()]
        else:  # backfill
            years = YEAR_RANGES["games_modern"].to_list()

    return [
        games_resource(years),
        drives_resource(years),
        game_media_resource(years),
        game_weather_resource(years),
        records_resource(years),
        game_team_stats_resource(years),
        game_player_stats_resource(years),
    ]


@dlt.resource(
    name="games",
    write_disposition="merge",
    primary_key="id",
)
def games_resource(years: list[int]) -> Iterator[dict]:
    """Load games for specified years.

    Args:
        years: List of years to load games for
    """
    client = get_client()
    try:
        for year in years:
            logger.info(f"Loading games for {year}...")

            # Load FBS games
            data = make_request(client, "/games", params={"year": year})

            for game in data:
                # Add year for partitioning if needed
                game["season"] = year
                yield game

    finally:
        client.close()


@dlt.resource(
    name="drives",
    write_disposition="merge",
    primary_key="id",
)
def drives_resource(years: list[int]) -> Iterator[dict]:
    """Load drives for specified years.

    Args:
        years: List of years to load drives for
    """
    client = get_client()
    try:
        for year in years:
            logger.info(f"Loading drives for {year}...")

            # CFBD drives endpoint requires seasonType parameter
            data = make_request(
                client,
                "/drives",
                params={"year": year, "seasonType": "regular"}
            )

            for drive in data:
                drive["season"] = year
                yield drive

            # Also load postseason drives
            postseason_data = make_request(
                client,
                "/drives",
                params={"year": year, "seasonType": "postseason"}
            )

            for drive in postseason_data:
                drive["season"] = year
                yield drive

    finally:
        client.close()


@dlt.resource(
    name="game_media",
    write_disposition="merge",
    primary_key="id",
)
def game_media_resource(years: list[int]) -> Iterator[dict]:
    """Load game media (TV, streaming) for specified years.

    Args:
        years: List of years to load media for
    """
    client = get_client()
    try:
        for year in years:
            logger.info(f"Loading game media for {year}...")

            data = make_request(client, "/games/media", params={"year": year})

            for media in data:
                media["season"] = year
                yield media

    finally:
        client.close()


@dlt.resource(
    name="game_weather",
    write_disposition="merge",
    primary_key="id",
)
def game_weather_resource(years: list[int]) -> Iterator[dict]:
    """Load game weather data for specified years.

    Args:
        years: List of years to load weather for
    """
    client = get_client()
    try:
        for year in years:
            logger.info(f"Loading game weather for {year}...")

            data = make_request(client, "/games/weather", params={"year": year})

            yield from data

    finally:
        client.close()


@dlt.resource(
    name="records",
    write_disposition="merge",
    primary_key=["year", "team"],
)
def records_resource(years: list[int]) -> Iterator[dict]:
    """Load team records for specified years.

    Args:
        years: List of years to load records for
    """
    client = get_client()
    try:
        for year in years:
            logger.info(f"Loading records for {year}...")

            data = make_request(client, "/records", params={"year": year})

            yield from data

    finally:
        client.close()


@dlt.resource(
    name="game_team_stats",
    write_disposition="merge",
    primary_key="id",
)
def game_team_stats_resource(years: list[int]) -> Iterator[dict]:
    """Load team box scores per game for specified years.

    Args:
        years: List of years to load game team stats for
    """
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
                            params={"year": year, "seasonType": season_type, "week": week}
                        )
                        yield from data
                    except Exception:
                        # Some weeks may not have games (esp postseason)
                        continue

    finally:
        client.close()


@dlt.resource(
    name="game_player_stats",
    write_disposition="merge",
    primary_key="id",
)
def game_player_stats_resource(years: list[int]) -> Iterator[dict]:
    """Load player box scores per game for specified years.

    Args:
        years: List of years to load game player stats for
    """
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
                            params={"year": year, "seasonType": season_type, "week": week}
                        )
                        yield from data
                    except Exception:
                        # Some weeks may not have games (esp postseason)
                        continue

    finally:
        client.close()
