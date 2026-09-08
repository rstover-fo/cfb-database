"""Declared SQL refresh assets, checked against their PostgreSQL parse trees.

This registry covers materialized-view refreshes and their relation inputs, not
provider work units or compute-job generations. Ordering is stable for unrelated
assets. Existing full-refresh order is retained as the tie-breaker.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class RefreshAsset:
    name: str
    depends_on: tuple[str, ...]
    definition: str
    coverage_grain: str = "whole_relation"


# Input relations are updated by separate ingestion/compute jobs. Naming one as
# changed asserts its writes have already committed; this planner does not run
# those jobs. Explicit names make typos/missing dependencies fail validation.
EXTERNAL_RELATIONS = frozenset(
    (
        "analytics.adjusted_epa_build",
        "analytics.adjusted_epa_week_build",
        "analytics.house_elo_game",
        "analytics.player_game_epa_build",
        "betting.lines",
        "betting.team_ats",
        "core.drives",
        "core.game_team_stats",
        "core.game_team_stats__teams",
        "core.game_team_stats__teams__stats",
        "core.games",
        "core.plays",
        "core.roster",
        "draft.draft_picks",
        "features.team_week",
        "metrics.ppa_players_season",
        "metrics.pregame_win_probability",
        "metrics.wepa_players_kicking",
        "metrics.wepa_players_passing",
        "metrics.wepa_players_rushing",
        "metrics.wepa_team_season",
        "predictions.game_predictions",
        "ratings.core_ratings",
        "ratings.elo_ratings",
        "ratings.espn_fpi_weekly",
        "ratings.fpi_ratings",
        "ratings.sdv_ratings_weekly",
        "ratings.sp_ratings",
        "recruiting.recruits",
        "recruiting.team_recruiting",
        "recruiting.transfer_portal",
        "ref.coach_seasons",
        "ref.coach_tenures",
        "ref.coaches",
        "ref.coaches__seasons",
        "ref.eras",
        "ref.teams",
        "stats.game_havoc",
        "stats.passing_player_season",
        "stats.passing_plays",
        "stats.passing_team_season",
        "stats.play_stats",
        "stats.player_returning",
        "stats.player_season_stats",
        "stats.player_usage",
        "stats.rushing_player_season",
        "stats.rushing_plays",
        "stats.rushing_team_season",
    )
)

REFRESH_ASSETS = (
    RefreshAsset(
        "marts._game_epa_calc", ("core.plays",), "src/schemas/marts/002_game_epa_calc.sql"
    ),
    RefreshAsset("marts.play_epa", ("core.plays",), "src/schemas/marts/010_play_epa.sql"),
    RefreshAsset(
        "marts.player_comparison",
        (
            "core.roster",
            "metrics.ppa_players_season",
            "recruiting.recruits",
            "stats.player_season_stats",
        ),
        "src/schemas/marts/020_player_comparison.sql",
    ),
    RefreshAsset(
        "marts.conference_head_to_head",
        ("core.games",),
        "src/schemas/marts/027_conference_head_to_head.sql",
    ),
    RefreshAsset(
        "marts.team_wepa_season",
        ("metrics.wepa_team_season",),
        "src/schemas/marts/029_team_wepa_season.sql",
    ),
    RefreshAsset(
        "marts.player_wepa_season",
        (
            "metrics.wepa_players_kicking",
            "metrics.wepa_players_passing",
            "metrics.wepa_players_rushing",
        ),
        "src/schemas/marts/030_player_wepa_season.sql",
    ),
    RefreshAsset(
        "marts.returning_production",
        ("stats.player_returning",),
        "src/schemas/marts/031_returning_production.sql",
    ),
    RefreshAsset(
        "marts.player_usage", ("stats.player_usage",), "src/schemas/marts/032_player_usage.sql"
    ),
    RefreshAsset(
        "marts.team_ats_records",
        ("betting.team_ats",),
        "src/schemas/marts/033_team_ats_records.sql",
    ),
    RefreshAsset(
        "marts.core_ratings", ("ratings.core_ratings",), "src/schemas/marts/043_core_ratings.sql"
    ),
    RefreshAsset(
        "marts.penalty_log",
        ("core.games", "core.plays", "ref.teams"),
        "src/schemas/marts/041_penalty_log.sql",
    ),
    RefreshAsset(
        "marts.team_penalty_box",
        (
            "core.game_team_stats",
            "core.game_team_stats__teams",
            "core.game_team_stats__teams__stats",
            "core.games",
        ),
        "src/schemas/marts/042_team_penalty_box.sql",
    ),
    RefreshAsset(
        "marts.passing_charting_player_season",
        ("core.roster", "stats.passing_player_season"),
        "src/schemas/marts/045_passing_charting_player_season.sql",
    ),
    RefreshAsset(
        "marts.passing_charting_target_season",
        ("ref.teams", "stats.passing_plays"),
        "src/schemas/marts/046_passing_charting_target_season.sql",
    ),
    RefreshAsset(
        "marts.passing_charting_team_season",
        ("stats.passing_plays", "stats.passing_team_season"),
        "src/schemas/marts/047_passing_charting_team_season.sql",
    ),
    RefreshAsset(
        "marts.coach_tenures",
        ("ref.coach_tenures", "ref.teams"),
        "src/schemas/marts/048_coach_tenures.sql",
    ),
    RefreshAsset(
        "marts.rushing_charting_player_season",
        ("core.roster", "stats.rushing_player_season"),
        "src/schemas/marts/050_rushing_charting_player_season.sql",
    ),
    RefreshAsset(
        "marts.rushing_charting_team_season",
        ("stats.rushing_plays", "stats.rushing_team_season"),
        "src/schemas/marts/051_rushing_charting_team_season.sql",
    ),
    RefreshAsset(
        "marts.rushing_charting_direction_season",
        ("stats.rushing_player_season", "stats.rushing_plays", "stats.rushing_team_season"),
        "src/schemas/marts/052_rushing_charting_direction_season.sql",
    ),
    RefreshAsset(
        "marts.team_epa_season",
        ("core.games", "marts._game_epa_calc"),
        "src/schemas/marts/003_team_epa_season.sql",
    ),
    RefreshAsset(
        "marts.team_season_summary",
        (
            "core.games",
            "ratings.core_ratings",
            "ratings.elo_ratings",
            "ratings.fpi_ratings",
            "ratings.sp_ratings",
            "recruiting.team_recruiting",
        ),
        "src/schemas/marts/001_team_season_summary.sql",
    ),
    RefreshAsset(
        "marts.player_game_epa",
        ("analytics.player_game_epa_build", "marts.play_epa", "stats.play_stats"),
        "src/schemas/marts/011_player_game_epa.sql",
    ),
    RefreshAsset(
        "marts.defensive_havoc",
        ("core.games", "core.plays", "stats.game_havoc"),
        "src/schemas/marts/005_defensive_havoc.sql",
    ),
    RefreshAsset(
        "marts.scoring_opportunities",
        ("core.drives",),
        "src/schemas/marts/006_scoring_opportunities.sql",
    ),
    RefreshAsset(
        "marts.team_playcalling_tendencies",
        ("core.plays", "marts.play_epa"),
        "src/schemas/marts/021_team_playcalling_tendencies.sql",
    ),
    RefreshAsset(
        "marts.team_situational_success",
        ("core.plays", "marts.play_epa"),
        "src/schemas/marts/022_team_situational_success.sql",
    ),
    RefreshAsset(
        "marts.situational_splits",
        ("core.games", "core.plays"),
        "src/schemas/marts/004_situational_splits.sql",
    ),
    RefreshAsset(
        "marts.player_season_epa",
        ("marts.player_game_epa",),
        "src/schemas/marts/012_player_season_epa.sql",
    ),
    RefreshAsset(
        "marts.coach_record",
        ("recruiting.team_recruiting", "ref.coaches", "ref.coaches__seasons"),
        "src/schemas/marts/009_coach_record.sql",
    ),
    RefreshAsset(
        "marts.matchup_history", ("core.games",), "src/schemas/marts/007_matchup_history.sql"
    ),
    RefreshAsset(
        "marts.recruiting_class",
        ("recruiting.recruits", "recruiting.team_recruiting"),
        "src/schemas/marts/008_recruiting_class.sql",
    ),
    RefreshAsset(
        "marts.team_talent_composite",
        ("core.roster", "recruiting.recruits", "recruiting.transfer_portal"),
        "src/schemas/marts/017_team_talent_composite.sql",
    ),
    RefreshAsset(
        "marts.team_tempo_metrics",
        ("core.games", "core.plays", "marts.team_epa_season"),
        "src/schemas/marts/019_team_tempo_metrics.sql",
    ),
    RefreshAsset(
        "marts.transfer_portal_impact",
        ("core.roster", "marts.team_season_summary", "recruiting.transfer_portal"),
        "src/schemas/marts/025_transfer_portal_impact.sql",
    ),
    RefreshAsset(
        "marts.team_season_trajectory",
        (
            "core.games",
            "marts.defensive_havoc",
            "marts.team_epa_season",
            "marts.team_season_summary",
            "recruiting.team_recruiting",
            "ref.eras",
            "ref.teams",
        ),
        "src/schemas/marts/013_team_season_trajectory.sql",
    ),
    RefreshAsset(
        "marts.conference_era_summary",
        ("core.games", "marts.team_epa_season", "ref.eras"),
        "src/schemas/marts/014_conference_era_summary.sql",
    ),
    RefreshAsset(
        "marts.team_style_profile",
        ("marts.play_epa",),
        "src/schemas/marts/015_team_style_profile.sql",
    ),
    RefreshAsset(
        "marts.coaching_tenure",
        (
            "core.games",
            "marts.recruiting_class",
            "marts.team_season_summary",
            "ref.coach_seasons",
            "ref.coaches",
            "ref.coaches__seasons",
        ),
        "src/schemas/marts/023_coaching_tenure.sql",
    ),
    RefreshAsset(
        "marts.recruiting_roi",
        (
            "draft.draft_picks",
            "marts.recruiting_class",
            "marts.team_epa_season",
            "marts.team_season_summary",
        ),
        "src/schemas/marts/024_recruiting_roi.sql",
    ),
    RefreshAsset(
        "marts.conference_comparison",
        (
            "core.games",
            "marts.recruiting_class",
            "marts.team_epa_season",
            "marts.team_season_summary",
            "ref.teams",
        ),
        "src/schemas/marts/026_conference_comparison.sql",
    ),
    RefreshAsset(
        "marts.matchup_edges",
        ("core.games", "marts.team_style_profile"),
        "src/schemas/marts/016_matchup_edges.sql",
    ),
    RefreshAsset("marts.data_freshness", (), "src/schemas/marts/028_data_freshness.sql"),
    RefreshAsset(
        "marts.house_elo",
        ("analytics.house_elo_game", "ratings.elo_ratings"),
        "src/schemas/marts/034_house_elo.sql",
    ),
    RefreshAsset(
        "marts.house_elo_game",
        ("analytics.house_elo_game",),
        "src/schemas/marts/035_house_elo_game.sql",
    ),
    RefreshAsset(
        "marts.team_adjusted_epa",
        ("analytics.adjusted_epa_build", "marts.team_wepa_season"),
        "src/schemas/marts/036_team_adjusted_epa.sql",
    ),
    RefreshAsset(
        "marts.scored_matchup_edges",
        ("core.games", "predictions.game_predictions"),
        "src/schemas/marts/037_scored_matchup_edges.sql",
    ),
    RefreshAsset(
        "marts.prediction_accuracy",
        ("core.games", "metrics.pregame_win_probability", "predictions.game_predictions"),
        "src/schemas/marts/038_prediction_accuracy.sql",
    ),
    RefreshAsset(
        "marts.team_week_features",
        ("features.team_week",),
        "src/schemas/marts/039_team_week_features.sql",
    ),
    RefreshAsset(
        "marts.adjusted_epa_week",
        ("analytics.adjusted_epa_week_build",),
        "src/schemas/marts/040_adjusted_epa_week.sql",
    ),
    RefreshAsset(
        "marts.epa_crossvalidation",
        (
            "marts.team_adjusted_epa",
            "marts.team_epa_season",
            "ratings.espn_fpi_weekly",
            "ratings.fpi_ratings",
            "ratings.sdv_ratings_weekly",
            "ref.teams",
        ),
        "src/schemas/marts/044_epa_crossvalidation.sql",
    ),
    RefreshAsset(
        "analytics.team_season_summary", ("core.games",), "src/schemas/013_analytics_views.sql"
    ),
    RefreshAsset(
        "analytics.player_career_stats",
        ("stats.player_season_stats",),
        "src/schemas/013_analytics_views.sql",
    ),
    RefreshAsset(
        "analytics.conference_standings",
        (
            "analytics.team_season_summary",
            "ratings.elo_ratings",
            "ratings.sp_ratings",
            "recruiting.team_recruiting",
        ),
        "src/schemas/013_analytics_views.sql",
    ),
    RefreshAsset(
        "analytics.team_recruiting_trend",
        ("recruiting.recruits", "recruiting.team_recruiting"),
        "src/schemas/013_analytics_views.sql",
    ),
    RefreshAsset(
        "analytics.game_results",
        ("betting.lines", "core.games", "metrics.pregame_win_probability"),
        "src/schemas/013_analytics_views.sql",
    ),
)

# Function bodies hide relation edges from the calling query's RangeVars.
# Reviewed source hashes force dependency review when these definitions change.
FUNCTION_RELATIONS = {"ref.get_era": ("ref.eras",)}
FUNCTION_DEFINITIONS = {
    "ref.get_era": (
        "src/schemas/functions/get_era.sql",
        "504a8d01b3134888852a5c45fbce06717e63609a4dc6f4b5302a6344645ee1a0",
    ),
}
