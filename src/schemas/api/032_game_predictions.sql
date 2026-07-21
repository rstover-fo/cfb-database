-- api.game_predictions
-- Latest snapshot per (game, model) from the append-only prediction log:
-- house Elo + ridge-adjusted-EPA expected margin/win-prob vs the market
-- line, as of each game's most recent prediction_date.
-- Thin latest-snapshot view over predictions.game_predictions (Tier 2
-- analytics, docs/plans/2026-07-21-tier2-analytics-plan.md).
--
-- predictions.game_predictions is append-only across days (one immutable row
-- per game_id/model_version/UTC prediction_date -- see
-- migrations/024_predictions_schema.sql), so this view picks the most recent
-- prediction_date per (game_id, model_version) via DISTINCT ON. Query the
-- base table directly if the full day-by-day history is needed instead.
--
-- PostgREST usage:
--   GET /api/game_predictions?game_id=eq.401628455
--   GET /api/game_predictions?season=eq.2026&week=eq.5&model_version=eq.house_v1

CREATE OR REPLACE VIEW api.game_predictions AS
SELECT DISTINCT ON (game_id, model_version)
    prediction_id,
    computed_at,
    prediction_date,
    model_version,
    game_id,
    season,
    week,
    season_type,
    home_team,
    away_team,
    neutral_site,
    home_elo_pregame,
    away_elo_pregame,
    elo_margin,
    epa_margin,
    expected_home_margin,
    home_win_prob,
    market_provider,
    market_home_margin,
    market_spread,
    market_captured_at,
    edge,
    edge_pick
FROM predictions.game_predictions
ORDER BY game_id, model_version, prediction_date DESC;

GRANT SELECT ON api.game_predictions TO anon, authenticated;

COMMENT ON VIEW api.game_predictions IS 'Latest house prediction snapshot per (game_id, model_version), from the append-only predictions.game_predictions log. Columns: prediction_id, computed_at, prediction_date, model_version, game_id, season, week, season_type, home_team, away_team, neutral_site, home_elo_pregame, away_elo_pregame, elo_margin, epa_margin, expected_home_margin, home_win_prob, market_provider, market_home_margin, market_spread, market_captured_at, edge, edge_pick. DISTINCT ON (game_id, model_version) ORDER BY prediction_date DESC selects the most recent snapshot; query predictions.game_predictions directly for full day-by-day history.';
