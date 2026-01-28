"""Advanced metrics data sources - PPA, win probability.

Predicted Points Added and other advanced analytics.
"""

import logging
from collections.abc import Iterator

import dlt
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
        win_probability_resource(years),
        ppa_predicted_resource(),
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

            for team in data:
                yield team

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

            data = make_request(
                client, "/ppa/players/season", params={"year": year}
            )

            for player in data:
                yield player

    finally:
        client.close()


@dlt.resource(
    name="ppa_games",
    write_disposition="merge",
    primary_key=["game_id", "team"],
)
def ppa_games_resource(years: list[int]) -> Iterator[dict]:
    """Load game-level PPA (Predicted Points Added) data.

    Args:
        years: List of years to load PPA for
    """
    client = get_client()
    try:
        for year in years:
            logger.info(f"Loading game PPA for {year}...")

            data = make_request(client, "/ppa/games", params={"year": year})

            for game in data:
                yield game

    finally:
        client.close()


@dlt.resource(
    name="ppa_players_games",
    write_disposition="merge",
    primary_key="id",
)
def ppa_players_games_resource(years: list[int]) -> Iterator[dict]:
    """Load player game-level PPA data.

    Args:
        years: List of years to load PPA for
    """
    client = get_client()
    try:
        for year in years:
            logger.info(f"Loading player game PPA for {year}...")

            data = make_request(
                client, "/ppa/players/games", params={"year": year}
            )

            for player in data:
                yield player

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

            data = make_request(
                client, "/metrics/wp/pregame", params={"year": year}
            )

            for game in data:
                yield game

    finally:
        client.close()


@dlt.resource(
    name="win_probability",
    write_disposition="merge",
    primary_key="play_id",
)
def win_probability_resource(years: list[int]) -> Iterator[dict]:
    """Load in-game win probability by play.

    Args:
        years: List of years to load win probability for
    """
    client = get_client()
    try:
        for year in years:
            logger.info(f"Loading in-game win probability for {year}...")

            data = make_request(
                client, "/metrics/wp", params={"year": year}
            )

            for play in data:
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
    """
    client = get_client()
    try:
        logger.info("Loading predicted PPA lookup data...")

        data = make_request(client, "/ppa/predicted")

        yield from data

    finally:
        client.close()
