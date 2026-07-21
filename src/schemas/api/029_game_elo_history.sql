-- api.game_elo_history
-- Game-grain house Elo history: pregame/postgame Elo both sides, win prob,
-- expected vs actual margin, CFBD Elo copies for validation.
-- Thin passthrough of marts.house_elo_game (Tier 2 analytics,
-- docs/plans/2026-07-21-tier2-analytics-plan.md).
--
-- PostgREST usage:
--   GET /api/game_elo_history?game_id=eq.401628455
--   GET /api/game_elo_history?season=eq.2024&home_team=eq.Ohio State

CREATE OR REPLACE VIEW api.game_elo_history AS
SELECT
    game_id,
    season,
    week,
    season_type,
    start_date,
    neutral_site,
    home_team,
    away_team,
    home_pregame_elo,
    away_pregame_elo,
    home_postgame_elo,
    away_postgame_elo,
    home_win_prob,
    expected_home_margin,
    actual_home_margin,
    mov_multiplier,
    cfbd_home_pregame_elo,
    cfbd_away_pregame_elo,
    margin_error,
    abs_margin_error
FROM marts.house_elo_game;

GRANT SELECT ON api.game_elo_history TO anon, authenticated;

COMMENT ON VIEW api.game_elo_history IS 'Game-grain house Elo history. Columns: game_id, season, week, season_type, start_date, neutral_site, home_team, away_team, home_pregame_elo, away_pregame_elo, home_postgame_elo, away_postgame_elo, home_win_prob, expected_home_margin, actual_home_margin, mov_multiplier, cfbd_home_pregame_elo, cfbd_away_pregame_elo, margin_error, abs_margin_error. margin_error = expected_home_margin - actual_home_margin (positive = model overrated the home side). Backed by marts.house_elo_game.';
