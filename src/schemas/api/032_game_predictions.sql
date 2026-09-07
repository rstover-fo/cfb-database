-- Latest published forecast per game/model. Historical modes remain discoverable
-- through prediction_history. Neither prediction_date nor computed_at selects a row.
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
    edge_pick,
    evaluation_mode,
    created_at,
    published_at,
    simulated_as_of_at,
    experiment_label,
    fit_id,
    input_hash
FROM predictions.game_predictions
WHERE evaluation_mode = 'published_forecast'
ORDER BY game_id, model_version, published_at DESC, prediction_id DESC;

GRANT SELECT ON api.game_predictions TO anon, authenticated;

COMMENT ON VIEW api.game_predictions IS 'Latest published forecast by publication timestamp and immutable prediction_id. Published does not alone imply pre-kickoff; accuracy applies its strict kickoff cutoff.';

CREATE OR REPLACE VIEW api.prediction_history AS
SELECT
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
    edge_pick,
    evaluation_mode,
    created_at,
    published_at,
    simulated_as_of_at,
    experiment_label,
    fit_id,
    input_hash
FROM predictions.game_predictions;

GRANT SELECT ON api.prediction_history TO anon, authenticated;
COMMENT ON VIEW api.prediction_history IS 'All prediction modes and immutable IDs, including legacy_unknown. Simulated as-of timestamps are reconstruction intent, not original publication evidence.';

-- Preserve the API-only analyst role even when deployed by a different owner.
DO $grants$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'analyst_ro') THEN
        GRANT SELECT ON api.game_predictions, api.prediction_history TO analyst_ro;
    END IF;
END
$grants$;
