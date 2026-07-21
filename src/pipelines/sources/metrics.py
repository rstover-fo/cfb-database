"""Advanced metrics data sources - PPA, win probability.

Predicted Points Added and other advanced analytics.

``win_probability`` (in-game, per-play win probability) is loaded by
``metrics_wp_source`` / ``win_probability_resource`` below, NOT by
``metrics_source``. The endpoint requires a ``gameId`` parameter -- year-only
queries return 400 -- so it cannot share the year-iterated shape every other
resource in this module uses. See docs/pipeline-manifest.md row 47 and
``src/pipelines/run.py::run_metrics_wp_pipeline`` for the per-game loader that
drives it from ``core.games``.
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


@dlt.source(name="cfbd_metrics")
def metrics_source(
    years: list[int] | None = None,
    mode: str = "incremental",
) -> DltSource:
    """Source for advanced metrics data.

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
        ppa_teams_resource(years),
        ppa_players_season_resource(years),
        ppa_games_resource(years),
        ppa_players_games_resource(years),
        pregame_win_probability_resource(years),
        # in-game win_probability (per-play, requires gameId) intentionally
        # NOT returned here -- see metrics_wp_source() below and
        # src/pipelines/run.py::run_metrics_wp_pipeline for its game-id-driven
        # loader. Returning it from this year-driven source was dead code:
        # the endpoint requires gameId and always 400'd on a year-only query.
        ppa_predicted_resource(),
        fg_expected_points_resource(),
    ]


@dlt.resource(
    name="ppa_teams",
    write_disposition="merge",
    primary_key=["season", "team"],
)
def ppa_teams_resource(years: list[int]) -> Iterator[dict]:
    """Load team PPA (Predicted Points Added) data.

    Args:
        years: List of years to load PPA for
    """
    client = get_client()
    try:
        for year in years:
            logger.info(f"Loading team PPA for {year}...")

            data = make_request(client, "/ppa/teams", params={"year": year})

            yield from data

    finally:
        client.close()


@dlt.resource(
    name="ppa_players_season",
    write_disposition="merge",
    primary_key=["season", "id"],
)
def ppa_players_season_resource(years: list[int]) -> Iterator[dict]:
    """Load player season PPA data.

    Args:
        years: List of years to load PPA for
    """
    client = get_client()
    try:
        for year in years:
            logger.info(f"Loading player season PPA for {year}...")

            data = make_request(client, "/ppa/players/season", params={"year": year})

            yield from data

    finally:
        client.close()


@dlt.resource(
    name="ppa_games",
    write_disposition="merge",
    primary_key=["game_id", "team"],
)
def ppa_games_resource(years: list[int]) -> Iterator[dict]:
    """Load game-level PPA (Predicted Points Added) data.

    Note: Data availability varies by year. Years with no data return 400 and are skipped.

    Args:
        years: List of years to load PPA for
    """
    client = get_client()
    try:
        for year in years:
            logger.info(f"Loading game PPA for {year}...")

            try:
                data = make_request(client, "/ppa/games", params={"year": year})
                yield from data
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 400:
                    logger.warning(f"No game PPA data for {year} (400 response), skipping")
                    continue
                raise

    finally:
        client.close()


@dlt.resource(
    name="ppa_players_games",
    write_disposition="merge",
    primary_key="id",
)
def ppa_players_games_resource(years: list[int]) -> Iterator[dict]:
    """Load player game-level PPA data.

    Note: Data availability varies by year. Years with no data return 400 and are skipped.

    Args:
        years: List of years to load PPA for
    """
    client = get_client()
    try:
        for year in years:
            logger.info(f"Loading player game PPA for {year}...")

            try:
                data = make_request(client, "/ppa/players/games", params={"year": year})
                yield from data
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 400:
                    logger.warning(f"No player game PPA data for {year} (400 response), skipping")
                    continue
                raise

    finally:
        client.close()


@dlt.resource(
    name="pregame_win_probability",
    write_disposition="merge",
    primary_key=["season", "game_id"],
)
def pregame_win_probability_resource(years: list[int]) -> Iterator[dict]:
    """Load pregame win probability predictions.

    Args:
        years: List of years to load predictions for
    """
    client = get_client()
    try:
        for year in years:
            logger.info(f"Loading pregame win probabilities for {year}...")

            data = make_request(client, "/metrics/wp/pregame", params={"year": year})

            yield from data

    finally:
        client.close()


@dlt.source(name="cfbd_metrics_wp")
def metrics_wp_source(
    game_ids: list[int],
    game_seasons: dict[int, int] | None = None,
) -> DltSource:
    """Source for in-game (per-play) win probability, loaded one game at a time.

    ``/metrics/wp`` requires a ``gameId`` parameter -- there is no year-level
    query, which is why this is a separate source from ``metrics_source``
    rather than another year-iterated resource in it (see module docstring
    and docs/pipeline-manifest.md row 47). Callers are expected to be
    ``src.pipelines.run.run_metrics_wp_pipeline``, which resolves the game
    ids from ``core.games`` (only completed games not already present in
    ``metrics.win_probability``) and batches them into ~50-game
    ``pipeline.run()`` calls to stay under Supabase's statement timeout
    (~150+ rows/game -> ~8.5K rows/merge at batch_size=50, mirroring the
    proven ``run_game_stats_weekly`` pattern).

    Args:
        game_ids: CFBD game ids to fetch win probability for, one API call
            each.
        game_seasons: Optional {game_id: season} map used to stamp a
            ``season`` column onto every row (CFBD's per-game response may or
            may not echo the season back -- confirmed by the W1
            ``scripts/probe_metrics_wp.py`` probe). If a game_id has no entry,
            ``season`` is left as whatever (if anything) the API returned.
    """
    return [win_probability_resource(game_ids, game_seasons)]


@dlt.resource(
    name="win_probability",
    write_disposition="merge",
    # Compound key, not bare "play_id": CFBD's playId uniqueness scope
    # (globally unique vs. unique-per-game) is unconfirmed as of this
    # writing -- see docs/pipeline-manifest.md row 47 investigation note and
    # scripts/probe_metrics_wp.py, which was written specifically to check
    # this. (game_id, play_id) is correct either way, so it's the safe
    # default until the probe confirms play_id alone is sufficient.
    primary_key=["game_id", "play_id"],
)
def win_probability_resource(
    game_ids: list[int],
    game_seasons: dict[int, int] | None = None,
) -> Iterator[dict]:
    """Load in-game win probability by play, one API call per game_id.

    Each completed FBS game returns ~150+ per-play records (playId,
    playText, homeWinProbability, down, distance, yardLine, ...). Every row
    is stamped with ``game_id`` (defensively -- in case a given season's
    response omits it) and, when known, ``season``. A 400 or 404 for a given
    game_id (e.g. no WP data computed for that game) is logged and skipped
    rather than aborting the whole batch -- callers may pass game ids for
    games CFBD hasn't backfilled WP for yet.

    Args:
        game_ids: CFBD game ids to fetch win probability for.
        game_seasons: Optional {game_id: season} map for stamping season.
    """
    game_seasons = game_seasons or {}
    client = get_client()
    try:
        for game_id in game_ids:
            logger.info(f"Loading in-game win probability for game {game_id}...")

            try:
                data = make_request(client, "/metrics/wp", params={"gameId": game_id})
            except httpx.HTTPStatusError as e:
                if e.response.status_code in (400, 404):
                    logger.warning(
                        f"No win probability data for game {game_id} "
                        f"({e.response.status_code} response), skipping"
                    )
                    continue
                raise

            season = game_seasons.get(game_id)
            for play in data:
                play["game_id"] = game_id
                if season is not None:
                    play["season"] = season
                yield play

    finally:
        client.close()


@dlt.resource(
    name="ppa_predicted",
    write_disposition="merge",
    primary_key=["down", "distance"],
)
def ppa_predicted_resource() -> Iterator[dict]:
    """Load predicted PPA values (static lookup table).

    This is a reference/lookup endpoint that does not require year iteration.
    Note: This endpoint may require specific parameters. Returns empty if unavailable.
    """
    client = get_client()
    try:
        logger.info("Loading predicted PPA lookup data...")

        try:
            data = make_request(client, "/ppa/predicted")
            yield from data
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 400:
                logger.warning("PPA predicted endpoint returned 400, skipping")
                return
            raise

    finally:
        client.close()


@dlt.resource(
    name="fg_expected_points",
    write_disposition="merge",
    primary_key=["distance"],
)
def fg_expected_points_resource() -> Iterator[dict]:
    """Load field goal expected points by distance (static lookup table).

    This is a reference/lookup endpoint that does not require year iteration.
    Returns expected points value for each field goal distance.
    """
    client = get_client()
    try:
        logger.info("Loading field goal expected points lookup data...")

        try:
            data = make_request(client, "/metrics/fg/ep")
            yield from data
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 400:
                logger.warning("FG expected points endpoint returned 400, skipping")
                return
            raise

    finally:
        client.close()
