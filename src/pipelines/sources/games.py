"""Games and drives data sources - year-iterated loading.

These tables contain game event data and require year-based iteration.

Game box scores (game_team_stats, game_player_stats) load exclusively via
game_stats.py, whose week-by-week path keeps merge batches small enough for
Supabase statement timeouts.
"""

import logging
from collections.abc import Iterator

import dlt
from dlt.sources import DltSource

from ..config.years import YEAR_RANGES, get_current_season
from ..utils.api_client import get_client
from .base import make_request

logger = logging.getLogger(__name__)


@dlt.source(name="cfbd_games")
def games_source(
    years: list[int] | None = None,
    mode: str = "incremental",
    games_cache: dict[int, list[dict]] | None = None,
) -> DltSource:
    """Source for games and drives data.

    Args:
        years: Specific years to load. If None, uses mode to determine years.
        mode: "incremental" loads current season, "backfill" loads all historical.
        games_cache: Optional {year: /games response} cache shared between the
            games resource and the drives orphan filter (and across the two
            sequential source instances run_games_pipeline creates). CFBD sits
            behind a CDN and two /games fetches seconds apart can return
            different game sets (observed: deploy run 29845763420, id
            401754543 present in one response and absent from the other), so
            the drives filter must reuse the exact response the games merge
            loaded, never a second fetch.
    """
    if years is None:
        if mode == "incremental":
            years = [get_current_season()]
        else:  # backfill
            years = YEAR_RANGES["games_modern"].to_list()

    if games_cache is None:
        games_cache = {}

    return [
        games_resource(years, games_cache),
        drives_resource(years, games_cache),
        game_media_resource(years),
        game_weather_resource(years),
        records_resource(years),
    ]


def _fetch_year_games(client, year: int, games_cache: dict[int, list[dict]]) -> list[dict]:
    """Fetch /games for a year once, serving repeats from the shared cache."""
    if year not in games_cache:
        games_cache[year] = make_request(client, "/games", params={"year": year})
    return games_cache[year]


@dlt.resource(
    name="games",
    write_disposition="merge",
    primary_key="id",
)
def games_resource(
    years: list[int], games_cache: dict[int, list[dict]] | None = None
) -> Iterator[dict]:
    """Load games for specified years.

    Args:
        years: List of years to load games for
        games_cache: Shared {year: /games response} cache (see games_source)
    """
    if games_cache is None:
        games_cache = {}
    client = get_client()
    try:
        for year in years:
            logger.info(f"Loading games for {year}...")

            # Load FBS games
            data = _fetch_year_games(client, year, games_cache)

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
def drives_resource(
    years: list[int], games_cache: dict[int, list[dict]] | None = None
) -> Iterator[dict]:
    """Load drives for specified years.

    CFBD's /drives can return drives for game ids that /games no longer
    lists for the same year (e.g. 2025's 401754543 -- cancelled/delisted
    games whose partial drive data lingers). Such rows have no parent for
    fk_drives_game, so each year's drives are filtered against that year's
    /games id set; dropped drives are logged, never loaded. The id set MUST
    come from the same /games response the games resource loaded (the shared
    games_cache) -- a second fetch can disagree with the first (CDN cache
    variance, see games_source) and re-admit an orphan.

    Args:
        years: List of years to load drives for
        games_cache: Shared {year: /games response} cache (see games_source)
    """
    if games_cache is None:
        games_cache = {}
    client = get_client()
    try:
        for year in years:
            logger.info(f"Loading drives for {year}...")

            game_ids = {game["id"] for game in _fetch_year_games(client, year, games_cache)}

            year_drives: list[dict] = []
            orphan_ids: set[int | None] = set()
            dropped = 0

            # CFBD drives endpoint requires seasonType parameter
            for season_type in ("regular", "postseason"):
                data = make_request(
                    client, "/drives", params={"year": year, "seasonType": season_type}
                )
                for drive in data:
                    gid = drive.get("gameId", drive.get("game_id"))
                    if gid not in game_ids:
                        orphan_ids.add(gid)
                        dropped += 1
                        continue
                    drive["season"] = year
                    year_drives.append(drive)

            if orphan_ids:
                logger.warning(
                    f"Dropping {dropped} drive(s) for {len(orphan_ids)} game(s) absent "
                    f"from /games?year={year}: {sorted(map(str, orphan_ids))[:10]}"
                )
            total = dropped + len(year_drives)
            if total and dropped / total > 0.25:
                raise ValueError(
                    f"Orphan filter would drop {dropped}/{total} drives for {year} -- "
                    "that is field-rename/coverage breakage, not stray game ids"
                )

            yield from year_drives

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
