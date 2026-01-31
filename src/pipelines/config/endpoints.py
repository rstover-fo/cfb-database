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


# Reference data endpoints (merge to preserve indexes and FKs)
REFERENCE_ENDPOINTS = {
    "conferences": EndpointConfig(
        path="/conferences",
        table_name="conferences",
        primary_key=["id"],
        schema="ref",
        write_disposition="merge",
    ),
    "teams": EndpointConfig(
        path="/teams",
        table_name="teams",
        primary_key=["id"],
        schema="ref",
        write_disposition="merge",
    ),
    "venues": EndpointConfig(
        path="/venues",
        table_name="venues",
        primary_key=["id"],
        schema="ref",
        write_disposition="merge",
    ),
    "coaches": EndpointConfig(
        path="/coaches",
        table_name="coaches",
        primary_key=["first_name", "last_name"],
        schema="ref",
        write_disposition="merge",
    ),
    "play_types": EndpointConfig(
        path="/plays/types",
        table_name="play_types",
        primary_key=["id"],
        schema="ref",
        write_disposition="merge",
    ),
    "teams_fbs": EndpointConfig(
        path="/teams/fbs",
        table_name="teams_fbs",
        primary_key=["id"],
        schema="ref",
        write_disposition="merge",
    ),
    "draft_positions": EndpointConfig(
        path="/draft/positions",
        table_name="draft_positions",
        primary_key=["name"],
        schema="ref",
        write_disposition="merge",
    ),
    "draft_teams": EndpointConfig(
        path="/draft/teams",
        table_name="draft_teams",
        primary_key=["location", "nickname"],
        schema="ref",
        write_disposition="merge",
    ),
    "stat_categories": EndpointConfig(
        path="/stats/categories",
        table_name="stat_categories",
        primary_key=["name"],
        schema="ref",
        write_disposition="merge",
    ),
    "calendar": EndpointConfig(
        path="/calendar",
        table_name="calendar",
        primary_key=["season", "week"],
        schema="ref",
        write_disposition="merge",
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
    "roster": EndpointConfig(
        path="/roster",
        table_name="roster",
        primary_key=["id"],
        schema="core",
        write_disposition="merge",
    ),
    "records": EndpointConfig(
        path="/records",
        table_name="team_records",
        primary_key=["year", "team"],
        schema="core",
        write_disposition="merge",
    ),
    "game_weather": EndpointConfig(
        path="/games/weather",
        table_name="game_weather",
        primary_key=["id"],
        schema="core",
        write_disposition="merge",
    ),
    "team_matchup": EndpointConfig(
        path="/teams/matchup",
        table_name="team_matchups",
        primary_key=["team1", "team2"],
        schema="core",
        write_disposition="merge",
    ),
    "player_search": EndpointConfig(
        path="/player/search",
        table_name="players",
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
        primary_key=["player_id", "season", "team", "category", "stat_type"],
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
    "advanced_game_stats": EndpointConfig(
        path="/game/box/advanced",
        table_name="advanced_game_stats",
        primary_key=["game_id", "team"],
        schema="stats",
        write_disposition="merge",
    ),
    "player_usage": EndpointConfig(
        path="/player/usage",
        table_name="player_usage",
        primary_key=["season", "id"],
        schema="stats",
        write_disposition="merge",
    ),
    "player_returning": EndpointConfig(
        path="/player/returning",
        table_name="player_returning",
        primary_key=["season", "team"],
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
    "sp_conferences": EndpointConfig(
        path="/ratings/sp/conferences",
        table_name="sp_conference_ratings",
        primary_key=["year", "conference"],
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
        primary_key=["first_name", "last_name", "origin", "season"],
        schema="recruiting",
        write_disposition="merge",
    ),
    "team_talent": EndpointConfig(
        path="/talent",
        table_name="team_talent",
        primary_key=["year", "school"],
        schema="recruiting",
        write_disposition="merge",
    ),
    "recruiting_groups": EndpointConfig(
        path="/recruiting/groups",
        table_name="recruiting_groups",
        primary_key=["year", "team", "position_group"],
        schema="recruiting",
        write_disposition="merge",
    ),
}

# Betting endpoints (year-iterated, merge)
BETTING_ENDPOINTS = {
    "lines": EndpointConfig(
        path="/lines",
        table_name="lines",
        primary_key=["game_id", "provider"],
        schema="betting",
        write_disposition="merge",
    ),
}

# Draft endpoints (year-iterated, merge)
DRAFT_ENDPOINTS = {
    "picks": EndpointConfig(
        path="/draft/picks",
        table_name="picks",
        primary_key=["year", "overall"],
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
    "ppa_predicted": EndpointConfig(
        path="/ppa/predicted",
        table_name="ppa_predicted",
        primary_key=["down", "distance"],
        schema="metrics",
        write_disposition="merge",
    ),
}

# Rankings endpoints (year+week iterated, merge)
RANKINGS_ENDPOINTS = {
    "rankings": EndpointConfig(
        path="/rankings",
        table_name="rankings",
        primary_key=["season", "week", "poll", "rank"],
        schema="core",
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
    "rankings": RANKINGS_ENDPOINTS,
}
