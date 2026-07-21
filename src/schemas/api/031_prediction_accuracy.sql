-- api.prediction_accuracy
-- Retroactive scoring of house predictions by season/model/edge-threshold:
-- margin MAE/RMSE, ATS record, Brier score (house and CFBD), vs the market
-- and CFBD's own pregame win probability.
-- Thin passthrough of marts.prediction_accuracy (Tier 2 analytics,
-- docs/plans/2026-07-21-tier2-analytics-plan.md).
--
-- PostgREST usage:
--   GET /api/prediction_accuracy?season=eq.2024&model_version=eq.house_v1
--   GET /api/prediction_accuracy?edge_threshold=eq.6&order=ats_hit_rate.desc

CREATE OR REPLACE VIEW api.prediction_accuracy AS
SELECT
    model_version,
    season,
    edge_threshold,
    n_games,
    n_with_market,
    margin_mae,
    margin_rmse,
    ats_wins,
    ats_losses,
    ats_pushes,
    ats_hit_rate,
    brier,
    cfbd_brier,
    n_scored_win_prob
FROM marts.prediction_accuracy;

GRANT SELECT ON api.prediction_accuracy TO anon, authenticated;

COMMENT ON VIEW api.prediction_accuracy IS 'Retroactive scoring of house predictions by season/model/edge-threshold. Columns: model_version, season, edge_threshold, n_games, n_with_market, margin_mae, margin_rmse, ats_wins, ats_losses, ats_pushes, ats_hit_rate, brier, cfbd_brier, n_scored_win_prob. brier/cfbd_brier let the house win-prob model be benchmarked directly against CFBD''s pregame win probability. Backed by marts.prediction_accuracy.';
