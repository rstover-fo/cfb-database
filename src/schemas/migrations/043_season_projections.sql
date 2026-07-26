-- Season projections: append-only Monte Carlo season-outcome snapshots
-- =============================================================================
-- Preseason outlook plan (docs/plans/2026-07-25-preseason-outlook-model-plan.md),
-- Phase 4.
--
-- predictions.season_projections holds one immutable snapshot row per
-- (season, team, model_version, UTC projection_date): the distribution of
-- season win totals implied by simulating every game on that team's schedule
-- with the per-game predictions in predictions.game_predictions.
--
-- WHY THIS TABLE EXISTS. Everything the warehouse predicted until now was a
-- per-game point estimate. Nothing represented a *season* as an object, so a
-- question like "what is this team's 2026 outlook" had no surface to read and
-- degraded to prose. Twelve independent game predictions do not compose into a
-- win total with an interval; this table is that composition, materialized.
--
-- Append-only across days, exactly like predictions.game_predictions
-- (migration 024): the same-day ON CONFLICT DO UPDATE only lets a re-run
-- *converge* today's snapshot, never overwrite a prior day's. The snapshot
-- history is the point -- it shows how the model's read on a season moved as
-- games resolved, which is what makes a preseason projection auditable after
-- the fact rather than quietly revised.
--
-- Writer: scripts/simulate_season.py. Created empty here so schema and grants
-- are in place before the compute script and the Phase 5 marts/api views land.
--
-- Not in MIGRATION_ORDER: applied via run_migrations.py --file (deploy
-- manifest), like 019-028 and 041. Idempotent (IF NOT EXISTS throughout).

CREATE SCHEMA IF NOT EXISTS predictions;

CREATE TABLE IF NOT EXISTS predictions.season_projections (
    projection_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    computed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    projection_date DATE NOT NULL DEFAULT ((now() AT TIME ZONE 'utc')::date),
    model_version TEXT NOT NULL,

    -- Identity
    season BIGINT NOT NULL,
    team VARCHAR NOT NULL,
    conference VARCHAR,

    -- Schedule state. games_scheduled counts games actually ON the schedule,
    -- never a hypothetical full slate: as of 2026-07-25 the 2026 schedule had
    -- 68 of 337 teams with fewer than 8 games published, and projecting those
    -- over 12 games would invent results. schedule_complete flags whether the
    -- slate is whole enough for the win total to mean what a reader assumes
    -- (>= 11 regular-season games).
    games_scheduled BIGINT NOT NULL,
    games_completed BIGINT NOT NULL,
    actual_wins BIGINT,
    schedule_complete BOOLEAN NOT NULL,

    -- Central tendency
    projected_wins NUMERIC(5, 2),
    projected_losses NUMERIC(5, 2),
    median_wins NUMERIC(5, 2),

    -- Distribution. The percentiles and p_win_dist are the reason this table
    -- exists rather than a single projected_wins column: a 9-3 projection with
    -- a [7, 11] interval and one with a [8.5, 9.5] interval are different
    -- claims, and only the distribution distinguishes them.
    wins_p10 NUMERIC(5, 2),
    wins_p25 NUMERIC(5, 2),
    wins_p75 NUMERIC(5, 2),
    wins_p90 NUMERIC(5, 2),
    p_win_dist JSONB,
    p_bowl_eligible NUMERIC(5, 4),
    p_ten_plus NUMERIC(5, 4),

    -- Schedule strength (average opponent rating over the team's slate).
    sos_rating NUMERIC(8, 3),
    sos_rank BIGINT,

    -- Conference title odds are v1-crude: highest conference win percentage
    -- per simulation with ties split evenly. Real tiebreakers and
    -- championship-game formats are not modeled.
    conf_title_prob NUMERIC(5, 4),

    -- playoff_prob ships NULL deliberately. The 12-team format's automatic
    -- bids and seeding are a rules-modeling project of their own, and a number
    -- nobody can defend is worse than an honest absence. The column exists now
    -- so adding it later needs no migration.
    playoff_prob NUMERIC(5, 4),

    -- Simulation provenance. residual_sigma is the per-model standard
    -- deviation of (actual - predicted) margin the draws used; it is stored
    -- per row rather than assumed, so a projection always carries the
    -- assumption that produced it.
    n_sims BIGINT NOT NULL,
    residual_sigma NUMERIC(6, 3)
);

-- One snapshot per team per model per UTC day (mirrors
-- game_predictions_daily_key in migration 024).
CREATE UNIQUE INDEX IF NOT EXISTS season_projections_daily_key
    ON predictions.season_projections (season, team, model_version, projection_date);

CREATE INDEX IF NOT EXISTS season_projections_season_idx
    ON predictions.season_projections (season);

CREATE INDEX IF NOT EXISTS season_projections_team_season_idx
    ON predictions.season_projections (team, season);

CREATE INDEX IF NOT EXISTS season_projections_computed_at_idx
    ON predictions.season_projections (computed_at);

COMMENT ON TABLE predictions.season_projections IS
    'Append-only Monte Carlo season-outcome snapshots: one immutable row per (season, team, model_version, UTC projection_date), same-day ON CONFLICT DO UPDATE for intra-day convergence only. Completed games contribute their actual result; remaining games are drawn from Normal(expected_home_margin, residual_sigma). Written by scripts/simulate_season.py.';

COMMENT ON COLUMN predictions.season_projections.p_win_dist IS
    'Full win-total distribution as {"0": p, "1": p, ...} over 0..games_scheduled; sums to 1.';

COMMENT ON COLUMN predictions.season_projections.games_scheduled IS
    'Games actually on the schedule. Projections are never extrapolated to a hypothetical full slate -- see schedule_complete.';

COMMENT ON COLUMN predictions.season_projections.n_sims IS
    'Simulation count. v1 draws each game INDEPENDENTLY, which understates both tails: real season outcomes are correlated (a team better than its rating beats everyone more often), so extreme records are underpredicted. Treat p10/p90 and p_ten_plus as conservative at the edges.';

COMMENT ON COLUMN predictions.season_projections.playoff_prob IS
    'NULL in v1 -- the 12-team format''s automatic bids and seeding are not modeled.';

-- Grant USAGE + read-only SELECT per the repo's read-access pattern
-- (see grant_read_access_for_security_invoker.sql), matching 024/028 -- no
-- write grants to anon/authenticated; writes come only from the compute
-- scripts via the direct connection owner.
GRANT USAGE ON SCHEMA predictions TO anon, authenticated;
GRANT SELECT ON ALL TABLES IN SCHEMA predictions TO anon, authenticated;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA predictions FROM anon, authenticated;
