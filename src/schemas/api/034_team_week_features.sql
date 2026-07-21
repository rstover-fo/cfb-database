-- api.team_week_features
-- As-of feature vector entering each team's game: house Elo, opponent-
-- adjusted EPA, season-to-date production/havoc, and preseason-known
-- constants (returning production, prior-season SP+). This is the modeling
-- substrate behind fitted_v1 (docs/brainstorms/2026-07-21-team-week-feature-
-- design.md), exposed read-only for transparency.
-- Thin passthrough of marts.team_week_features (Tier 3 analytics,
-- docs/plans/2026-07-21-tier3-analytics-plan.md, Pillar C).
--
-- SIGN CONVENTION: adj_epa_off HIGHER = better offense; adj_epa_def LOWER /
-- more negative = better defense (EPA allowed above average); adj_epa_net =
-- adj_epa_off - adj_epa_def (HIGHER = better team overall).
--
-- AS-OF / LEAK-FREE: the row for week_index WI uses only data with
-- week_index < WI within the same season, plus preseason constants and
-- prior-season (S-1) adj-EPA fallbacks known before the season starts --
-- adj_epa_source ('week' | 'prior_season' | NULL) records which applies.
-- week_index = week for season_type = 'regular', 100 + week for
-- 'postseason' (CFBD restarts week numbering at 1 for bowls).
--
-- PostgREST usage:
--   GET /api/team_week_features?team=eq.Ohio State&season=eq.2024&order=week_index.asc
--   GET /api/team_week_features?season=eq.2026&week=eq.5&season_type=eq.regular
--
-- Column contract: docs/brainstorms/2026-07-21-team-week-feature-design.md
-- section 1 / migration 028.

CREATE OR REPLACE VIEW api.team_week_features AS
SELECT
    season,
    season_type,
    week,
    week_index,
    team,
    conference,
    game_id,
    games_played_to_date,
    elo_pregame,
    adj_epa_off,
    adj_epa_def,
    adj_epa_net,
    adj_epa_hfa,
    adj_epa_source,
    off_epa_per_play,
    off_success_rate,
    off_explosiveness_rate,
    off_plays_per_game,
    def_epa_per_play_allowed,
    def_success_rate_allowed,
    def_explosiveness_rate_allowed,
    havoc_rate_defense,
    havoc_rate_offense_allowed,
    returning_ppa_pct,
    returning_passing_ppa_pct,
    returning_rushing_ppa_pct,
    returning_usage,
    preseason_sp_rating,
    preseason_sp_offense,
    preseason_sp_defense,
    computed_at,
    feature_build_version
FROM marts.team_week_features;

GRANT SELECT ON api.team_week_features TO anon, authenticated;

COMMENT ON VIEW api.team_week_features IS 'As-of feature vector entering each team''s game -- house Elo, opponent-adjusted EPA, season-to-date production/havoc, and preseason-known constants. Columns: season, season_type, week, week_index, team, conference, game_id, games_played_to_date, elo_pregame, adj_epa_off, adj_epa_def, adj_epa_net, adj_epa_hfa, adj_epa_source, off_epa_per_play, off_success_rate, off_explosiveness_rate, off_plays_per_game, def_epa_per_play_allowed, def_success_rate_allowed, def_explosiveness_rate_allowed, havoc_rate_defense, havoc_rate_offense_allowed, returning_ppa_pct, returning_passing_ppa_pct, returning_rushing_ppa_pct, returning_usage, preseason_sp_rating, preseason_sp_offense, preseason_sp_defense, computed_at, feature_build_version. adj_epa_off higher = better offense; adj_epa_def lower/more negative = better defense. week_index = week for season_type=''regular'', 100 + week for ''postseason''. Backed by marts.team_week_features.';
