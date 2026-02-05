"""Stats data sources - team and player statistics.

Includes season stats, game stats for teams and players.
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


@dlt.source(name="cfbd_stats")
def stats_source(
    years: list[int] | None = None,
    mode: str = "incremental",
) -> DltSource:
    """Source for team and player statistics.

    Args:
        years: Specific years to load. If None, uses mode to determine years.
        mode: "incremental" loads current season, "backfill" loads all historical.
    """
    if years is None:
        if mode == "incremental":
            years = [get_current_season()]
        else:  # backfill
            years = YEAR_RANGES["stats"].to_list()

    return [
        team_season_stats_resource(years),
        player_season_stats_resource(years),
        advanced_team_stats_resource(years),
        advanced_game_stats_resource(years),
        player_usage_resource(years),
        player_returning_resource(years),
        play_stats_resource(years),
        game_havoc_resource(years),
    ]


@dlt.resource(
    name="team_season_stats",
    write_disposition="merge",
    primary_key=["season", "team", "stat_name"],
)
def team_season_stats_resource(years: list[int]) -> Iterator[dict]:
    """Load team season statistics.

    Args:
        years: List of years to load stats for
    """
    client = get_client()
    try:
        for year in years:
            logger.info(f"Loading team season stats for {year}...")

            data = make_request(client, "/stats/season", params={"year": year})

            for stat in data:
                stat["season"] = year
                yield stat

    finally:
        client.close()


@dlt.resource(
    name="player_season_stats",
    write_disposition="merge",
    primary_key=["player_id", "season", "team", "category", "stat_type"],
)
def player_season_stats_resource(years: list[int]) -> Iterator[dict]:
    """Load player season statistics.

    Args:
        years: List of years to load stats for
    """
    client = get_client()
    try:
        for year in years:
            logger.info(f"Loading player season stats for {year}...")

            # Player stats endpoint can return a lot of data
            # We'll load by category to manage memory
            categories = [
                "passing",
                "rushing",
                "receiving",
                "fumbles",
                "defensive",
                "interceptions",
                "punting",
                "kicking",
                "kickReturns",
                "puntReturns",
            ]

            for category in categories:
                logger.info(f"  Category: {category}...")
                data = make_request(
                    client, "/stats/player/season", params={"year": year, "category": category}
                )

                for stat in data:
                    stat["season"] = year
                    stat["category"] = category
                    # API returns statType (e.g., "YDS", "TD"); ensure it's present for PK
                    stat["stat_type"] = stat.get("statType", stat.get("stat_type", "unknown"))
                    yield stat

    finally:
        client.close()


@dlt.resource(
    name="advanced_team_stats",
    write_disposition="merge",
    primary_key=["season", "team"],
)
def advanced_team_stats_resource(years: list[int]) -> Iterator[dict]:
    """Load advanced team statistics (EPA, success rates, etc).

    Args:
        years: List of years to load stats for
    """
    client = get_client()
    try:
        for year in years:
            logger.info(f"Loading advanced team stats for {year}...")

            data = make_request(client, "/stats/season/advanced", params={"year": year})

            for stat in data:
                stat["season"] = year
                yield stat

    finally:
        client.close()


@dlt.resource(
    name="advanced_game_stats",
    write_disposition="merge",
    primary_key=["game_id", "team"],
)
def advanced_game_stats_resource(years: list[int]) -> Iterator[dict]:
    """Load advanced game-level box score stats (EPA, success rates, etc).

    Note: Data only available from ~2014+. Earlier years return 400 and are skipped.

    Args:
        years: List of years to load stats for
    """
    client = get_client()
    try:
        for year in years:
            logger.info(f"Loading advanced game stats for {year}...")

            try:
                data = make_request(
                    client,
                    "/game/box/advanced",
                    params={"year": year},
                )
                yield from data
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 400:
                    logger.warning(f"No advanced game stats for {year} (400 response), skipping")
                    continue
                raise

    finally:
        client.close()


@dlt.resource(
    name="player_usage",
    write_disposition="merge",
    primary_key=["season", "id"],
)
def player_usage_resource(years: list[int]) -> Iterator[dict]:
    """Load player usage metrics.

    Note: Data only available from ~2014+. Earlier years return 400 and are skipped.

    Args:
        years: List of years to load usage for
    """
    client = get_client()
    try:
        for year in years:
            logger.info(f"Loading player usage for {year}...")

            try:
                data = make_request(
                    client,
                    "/player/usage",
                    params={"year": year},
                )
                yield from data
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 400:
                    logger.warning(f"No player usage data for {year} (400 response), skipping")
                    continue
                raise

    finally:
        client.close()


@dlt.resource(
    name="player_returning",
    write_disposition="merge",
    primary_key=["season", "team"],
)
def player_returning_resource(years: list[int]) -> Iterator[dict]:
    """Load returning player production data.

    Note: Data only available from ~2014+. Earlier years return 400 and are skipped.

    Args:
        years: List of years to load returning production for
    """
    client = get_client()
    try:
        for year in years:
            logger.info(f"Loading player returning production for {year}...")

            try:
                data = make_request(
                    client,
                    "/player/returning",
                    params={"year": year},
                )
                yield from data
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 400:
                    logger.warning(f"No player returning data for {year} (400 response), skipping")
                    continue
                raise

    finally:
        client.close()


@dlt.resource(
    name="play_stats",
    write_disposition="merge",
    primary_key=["game_id", "play_id", "athlete_id", "stat_type"],
)
def play_stats_resource(
    years: list[int] | None = None,
    game_ids: list[int] | None = None,
) -> Iterator[dict]:
    """Load play-level statistics (player associations for each play).

    IMPORTANT: The API has a 2000 record limit per request. When loading by year,
    we iterate by gameId to ensure complete data extraction.

    Note: Data only available from ~2014+. Earlier years return 400 and are skipped.

    Args:
        years: List of years to load play stats for (will fetch game IDs from API)
        game_ids: Explicit list of game IDs to load (overrides years if provided)
    """
    if game_ids is None and years is None:
        raise ValueError("Must provide either years or game_ids")

    client = get_client()
    try:
        if game_ids is not None:
            # Direct game ID iteration
            total = 0
            for i, game_id in enumerate(game_ids):
                try:
                    data = make_request(
                        client,
                        "/plays/stats",
                        params={"gameId": game_id},
                    )
                    if data:
                        total += len(data)
                        yield from data
                    if (i + 1) % 100 == 0:
                        logger.info(f"  Processed {i + 1}/{len(game_ids)} games, {total} records")
                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 400:
                        continue
                    raise
            logger.info(f"Loaded {total} total records from {len(game_ids)} games")
        else:
            # Year-based loading: fetch games for each year, then iterate by gameId
            for year in years:
                logger.info(f"Loading play stats for {year}...")
                try:
                    # Get all games for this year
                    games = make_request(
                        client,
                        "/games",
                        params={"year": year, "seasonType": "regular"},
                    )
                    # Also get postseason games
                    postseason = make_request(
                        client,
                        "/games",
                        params={"year": year, "seasonType": "postseason"},
                    )
                    games.extend(postseason)

                    game_ids_for_year = [g["id"] for g in games if g.get("id")]
                    logger.info(f"  Found {len(game_ids_for_year)} games for {year}")

                    year_total = 0
                    for i, game_id in enumerate(game_ids_for_year):
                        try:
                            data = make_request(
                                client,
                                "/plays/stats",
                                params={"gameId": game_id},
                            )
                            if data:
                                year_total += len(data)
                                yield from data
                            if (i + 1) % 100 == 0:
                                logger.info(
                                    f"    {year}: {i + 1}/{len(game_ids_for_year)} games,"
                                    f" {year_total} records"
                                )
                        except httpx.HTTPStatusError as e:
                            if e.response.status_code == 400:
                                continue
                            raise

                    logger.info(
                        f"Loaded {year}: {year_total} records from {len(game_ids_for_year)} games"
                    )

                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 400:
                        logger.warning(f"No play stats data for {year} (400 response), skipping")
                        continue
                    raise

    finally:
        client.close()


@dlt.resource(
    name="game_havoc",
    write_disposition="merge",
    primary_key=["game_id", "team"],
)
def game_havoc_resource(years: list[int]) -> Iterator[dict]:
    """Load game-level havoc statistics (TFLs, passes broken up, etc).

    Note: Data only available from ~2014+. Earlier years return 400 and are skipped.

    Args:
        years: List of years to load havoc stats for
    """
    client = get_client()
    try:
        for year in years:
            logger.info(f"Loading game havoc stats for {year}...")

            try:
                data = make_request(
                    client,
                    "/stats/game/havoc",
                    params={"year": year},
                )
                yield from data
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 400:
                    logger.warning(f"No game havoc data for {year} (400 response), skipping")
                    continue
                raise

    finally:
        client.close()
