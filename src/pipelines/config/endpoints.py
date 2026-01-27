"""CFBD API endpoint configuration.

Defines primary keys, table names, and metadata for each endpoint.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class EndpointConfig:
    """Configuration for a CFBD API endpoint."""

    path: str
    table_name: str
    primary_key: list[str]
    schema: str = "ref"
    write_disposition: str = "replace"
    params: dict | None = None


# Reference data endpoints (full refresh)
REFERENCE_ENDPOINTS = {
    "conferences": EndpointConfig(
        path="/conferences",
        table_name="conferences",
        primary_key=["id"],
        schema="ref",
    ),
    "teams": EndpointConfig(
        path="/teams",
        table_name="teams",
        primary_key=["id"],
        schema="ref",
    ),
    "venues": EndpointConfig(
        path="/venues",
        table_name="venues",
        primary_key=["id"],
        schema="ref",
    ),
    "coaches": EndpointConfig(
        path="/coaches",
        table_name="coaches",
        primary_key=["first_name", "last_name", "seasons"],
        schema="ref",
    ),
    "play_types": EndpointConfig(
        path="/plays/types",
        table_name="play_types",
        primary_key=["id"],
        schema="ref",
    ),
}

# Core game data endpoints (year-iterated, merge)
CORE_ENDPOINTS = {
    "games": EndpointConfig(
        path="/games",
        table_name="games",
        primary_key=["id"],
        schema="core",
        write_disposition="merge",
    ),
    "drives": EndpointConfig(
        path="/drives",
        table_name="drives",
        primary_key=["id"],
        schema="core",
        write_disposition="merge",
    ),
    "plays": EndpointConfig(
        path="/plays",
        table_name="plays",
        primary_key=["id"],
        schema="core",
        write_disposition="merge",
    ),
    "game_media": EndpointConfig(
        path="/games/media",
        table_name="game_media",
        primary_key=["id"],
        schema="core",
        write_disposition="merge",
    ),
}

# Stats endpoints (year-iterated, merge)
STATS_ENDPOINTS = {
    "team_season_stats": EndpointConfig(
        path="/stats/season",
        table_name="team_season_stats",
        primary_key=["season", "team", "stat_name"],
        schema="stats",
        write_disposition="merge",
    ),
    "player_season_stats": EndpointConfig(
        path="/stats/player/season",
        table_name="player_season_stats",
        primary_key=["player_id", "season", "stat_type"],
        schema="stats",
        write_disposition="merge",
    ),
    "game_team_stats": EndpointConfig(
        path="/games/teams",
        table_name="game_team_stats",
        primary_key=["id"],
        schema="stats",
        write_disposition="merge",
    ),
    "game_player_stats": EndpointConfig(
        path="/games/players",
        table_name="game_player_stats",
        primary_key=["id"],
        schema="stats",
        write_disposition="merge",
    ),
}

# Ratings endpoints (year-iterated, merge)
RATINGS_ENDPOINTS = {
    "sp_ratings": EndpointConfig(
        path="/ratings/sp",
        table_name="sp_ratings",
        primary_key=["year", "team"],
        schema="ratings",
        write_disposition="merge",
    ),
    "elo_ratings": EndpointConfig(
        path="/ratings/elo",
        table_name="elo_ratings",
        primary_key=["year", "team"],
        schema="ratings",
        write_disposition="merge",
    ),
    "fpi_ratings": EndpointConfig(
        path="/ratings/fpi",
        table_name="fpi_ratings",
        primary_key=["year", "team"],
        schema="ratings",
        write_disposition="merge",
    ),
    "srs_ratings": EndpointConfig(
        path="/ratings/srs",
        table_name="srs_ratings",
        primary_key=["year", "team"],
        schema="ratings",
        write_disposition="merge",
    ),
}

# Recruiting endpoints (year-iterated, merge)
RECRUITING_ENDPOINTS = {
    "recruits": EndpointConfig(
        path="/recruiting/players",
        table_name="recruits",
        primary_key=["id"],
        schema="recruiting",
        write_disposition="merge",
    ),
    "team_recruiting": EndpointConfig(
        path="/recruiting/teams",
        table_name="team_recruiting",
        primary_key=["year", "team"],
        schema="recruiting",
        write_disposition="merge",
    ),
    "transfer_portal": EndpointConfig(
        path="/player/portal",
        table_name="transfer_portal",
        primary_key=["player_id", "season"],
        schema="recruiting",
        write_disposition="merge",
    ),
}

# Betting endpoints (year-iterated, merge)
BETTING_ENDPOINTS = {
    "lines": EndpointConfig(
        path="/lines",
        table_name="lines",
        primary_key=["id"],
        schema="betting",
        write_disposition="merge",
    ),
}

# Draft endpoints (year-iterated, merge)
DRAFT_ENDPOINTS = {
    "picks": EndpointConfig(
        path="/draft/picks",
        table_name="picks",
        primary_key=["college_athlete_id", "year"],
        schema="draft",
        write_disposition="merge",
    ),
}

# Metrics endpoints (year-iterated, merge)
METRICS_ENDPOINTS = {
    "ppa_games": EndpointConfig(
        path="/ppa/games",
        table_name="ppa_games",
        primary_key=["game_id", "team"],
        schema="metrics",
        write_disposition="merge",
    ),
    "ppa_players_games": EndpointConfig(
        path="/ppa/players/games",
        table_name="ppa_players_games",
        primary_key=["id"],
        schema="metrics",
        write_disposition="merge",
    ),
    "ppa_players_season": EndpointConfig(
        path="/ppa/players/season",
        table_name="ppa_players_season",
        primary_key=["id", "season"],
        schema="metrics",
        write_disposition="merge",
    ),
    "win_probability": EndpointConfig(
        path="/metrics/wp",
        table_name="win_probability",
        primary_key=["play_id"],
        schema="metrics",
        write_disposition="merge",
    ),
}

# All endpoints grouped by source
ALL_ENDPOINTS = {
    "reference": REFERENCE_ENDPOINTS,
    "core": CORE_ENDPOINTS,
    "stats": STATS_ENDPOINTS,
    "ratings": RATINGS_ENDPOINTS,
    "recruiting": RECRUITING_ENDPOINTS,
    "betting": BETTING_ENDPOINTS,
    "draft": DRAFT_ENDPOINTS,
    "metrics": METRICS_ENDPOINTS,
}
