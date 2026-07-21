-- Predictions schema: append-only house prediction snapshots
-- =============================================================================
-- Tier 2 analytics (docs/plans/2026-07-21-tier2-analytics-plan.md), Phase 1.
--
-- predictions.game_predictions holds one immutable snapshot row per
-- (game, model_version, UTC prediction_date): house Elo + ridge-adjusted-EPA
-- expected margin/win-prob, compared against the market line, with the
-- resulting edge. Rows are append-only across days -- the same-day
-- ON CONFLICT DO UPDATE only lets a re-run *converge* today's snapshot
-- (e.g. a market line refresh later the same UTC day), it never overwrites
-- a prior day's row. That gives an auditable history of how the model's
-- read on a game evolved as kickoff approached, which marts.prediction_accuracy
-- (Phase 4) scores retroactively for MAE/ATS/Brier.
--
-- Writer: scripts/compute_predictions.py (Phase 5 of the plan). Nothing reads
-- this table yet -- it is created empty here so schema and grants are in
-- place before the compute script and marts land.
--
-- Not in MIGRATION_ORDER: applied via run_migrations.py --file (deploy
-- manifest), like 019-023. Idempotent (IF NOT EXISTS throughout).

CREATE SCHEMA IF NOT EXISTS predictions;

CREATE TABLE IF NOT EXISTS predictions.game_predictions (
    prediction_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    computed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    prediction_date DATE NOT NULL DEFAULT ((now() AT TIME ZONE 'utc')::date),
    model_version TEXT NOT NULL,

    game_id BIGINT NOT NULL,
    season BIGINT,
    week BIGINT,
    season_type VARCHAR,
    home_team VARCHAR NOT NULL,
    away_team VARCHAR NOT NULL,
    neutral_site BOOLEAN,

    home_elo_pregame NUMERIC(8, 2),
    away_elo_pregame NUMERIC(8, 2),
    elo_margin NUMERIC(6, 2),
    epa_margin NUMERIC(6, 2),
    expected_home_margin NUMERIC(6, 2),
    home_win_prob NUMERIC(5, 4),

    market_provider VARCHAR,
    market_home_margin NUMERIC(6, 2),
    market_spread NUMERIC(6, 2),
    market_captured_at TIMESTAMPTZ,

    edge NUMERIC(6, 2),
    edge_pick VARCHAR
);

CREATE UNIQUE INDEX IF NOT EXISTS game_predictions_daily_key
    ON predictions.game_predictions (game_id, model_version, prediction_date);

CREATE INDEX IF NOT EXISTS game_predictions_game_id_idx
    ON predictions.game_predictions (game_id);

CREATE INDEX IF NOT EXISTS game_predictions_season_week_idx
    ON predictions.game_predictions (season, week);

CREATE INDEX IF NOT EXISTS game_predictions_computed_at_idx
    ON predictions.game_predictions (computed_at);

COMMENT ON TABLE predictions.game_predictions IS
    'Append-only house prediction snapshots: one immutable row per (game_id, model_version, UTC prediction_date), same-day ON CONFLICT DO UPDATE for intra-day convergence only. Written by scripts/compute_predictions.py; scored by marts.prediction_accuracy.';

-- Grant USAGE + read-only SELECT per the repo's read-access pattern
-- (see grant_read_access_for_security_invoker.sql).
GRANT USAGE ON SCHEMA predictions TO anon, authenticated;
GRANT SELECT ON ALL TABLES IN SCHEMA predictions TO anon, authenticated;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA predictions FROM anon, authenticated;
