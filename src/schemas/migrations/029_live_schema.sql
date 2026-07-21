-- Live schema: Saturday scoreboard polling + house live win-probability
-- =============================================================================
-- Tier 3 analytics (docs/plans/2026-07-21-tier3-analytics-plan.md), Pillar D,
-- Phase 1.
--
-- The `live` schema backs the in-game dashboard: append-only poll snapshots
-- of CFBD's /scoreboard endpoint (one row per game per poll tick) plus the
-- calibration parameters for a transparent, closed-form house live
-- win-probability model.
--
-- House live WP formula (computed by scripts/poll_scoreboard.py, stored in
-- house_live_home_wp -- documented here so the table is self-describing):
--
--   f          = clamp(seconds_remaining / 3600, eps, 1)
--   projected  = current_margin + pregame_expected_margin * f
--   home_wp    = Phi(projected / (sigma * sqrt(f)))
--
-- where current_margin = home_points - away_points, pregame_expected_margin
-- is the house Elo pregame expected home margin (analytics.house_elo_game),
-- sigma comes from live.wp_params, and Phi is the standard normal CDF
-- (stdlib erf). Boundary behavior: as f -> 0 (game ending) wp -> {0, 1} by
-- the sign of current_margin; at f = 1 (kickoff, current_margin = 0) wp
-- reduces to approximately the pregame Elo win probability. eps keeps the
-- denominator off zero at the final tick.
--
-- Writers: scripts/poll_scoreboard.py (scoreboard_snapshots, via the
-- .github/workflows/live-scoreboard.yml Saturday cron) and
-- scripts/calibrate_live_wp.py (wp_params, fits sigma against a 2015+
-- /metrics/wp backfill -- Phase 7 of the plan). Both tables are created
-- empty/seeded here so schema and grants are in place before the polling
-- workflow and calibration script land in later phases.
--
-- Not in MIGRATION_ORDER: applied via run_migrations.py --file (deploy
-- manifest), like 019-026. Idempotent (IF NOT EXISTS / ON CONFLICT DO
-- NOTHING throughout).

CREATE SCHEMA IF NOT EXISTS live;

-- -----------------------------------------------------------------------------
-- Append-only poll snapshots: one row per game per poll tick. No unique
-- constraint on (game_id, captured_at) -- a single poll batches every live
-- game under one workflow run, but ticks are not guaranteed to share an
-- identical timestamp, and duplicate ticks are harmless history, not a
-- correctness issue (the api layer's plain view takes DISTINCT ON latest
-- per game).
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS live.scoreboard_snapshots (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    captured_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    game_id BIGINT NOT NULL,
    season INTEGER,
    week INTEGER,
    season_type TEXT,

    status TEXT,
    period INTEGER,
    clock TEXT,
    seconds_remaining INTEGER,

    home_team TEXT,
    away_team TEXT,
    home_points INTEGER,
    away_points INTEGER,
    possession TEXT,

    spread NUMERIC,
    over_under NUMERIC,

    cfbd_home_wp NUMERIC,
    house_live_home_wp NUMERIC,
    pregame_expected_margin NUMERIC,

    snapshot_hash TEXT
);

-- Latest-per-game lookups (api/035_live_scoreboard.sql's DISTINCT ON) and
-- game timelines.
CREATE INDEX IF NOT EXISTS scoreboard_snapshots_game_captured_idx
    ON live.scoreboard_snapshots (game_id, captured_at DESC);

-- BRIN index: snapshots are appended in captured_at order (physically
-- sequential by insertion, like core.plays' season BRIN -- see 002_core.sql)
-- and this table is poll-tick append-only, so BRIN gives cheap time-range
-- pruning at a fraction of a B-tree's size.
CREATE INDEX IF NOT EXISTS scoreboard_snapshots_captured_at_brin
    ON live.scoreboard_snapshots USING brin (captured_at);

COMMENT ON TABLE live.scoreboard_snapshots IS
    'Append-only /scoreboard poll snapshots, one row per game per poll tick. house_live_home_wp is the closed-form house live win probability (see migration header for the formula); cfbd_home_wp is CFBD''s own live WP where available for comparison. Written by scripts/poll_scoreboard.py via .github/workflows/live-scoreboard.yml.';

-- -----------------------------------------------------------------------------
-- Single-row calibration parameters for the house live WP formula.
-- sigma seeds at ~16 as a pre-calibration guess (roughly the house Elo
-- margin-model scale); scripts/calibrate_live_wp.py (Phase 7) fits it
-- against a 2015+ /metrics/wp backfill and overwrites this row, along with
-- blend_weight, fit provenance, and the resulting Brier score.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS live.wp_params (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    sigma NUMERIC NOT NULL,
    blend_weight NUMERIC,
    fitted_through_season INTEGER,
    n_games INTEGER,
    brier NUMERIC,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE live.wp_params IS
    'Single-row calibration for the house live WP formula (see migration header). sigma seeds at ~16 pre-calibration; scripts/calibrate_live_wp.py (Tier 3 Phase 7) fits and replaces it against a 2015+ /metrics/wp backfill.';

-- Pre-calibration seed: sigma = 16.0, matching Pillar D's stated seed value.
-- Replaced in place by scripts/calibrate_live_wp.py once the Phase 7
-- calibration backfill and fit have run.
INSERT INTO live.wp_params (id, sigma)
VALUES (1, 16.0)
ON CONFLICT DO NOTHING;

-- Grant USAGE + read-only SELECT per the repo's read-access pattern
-- (see grant_read_access_for_security_invoker.sql). Writes come only from
-- the GitHub Actions role via the direct connection owner (poll_scoreboard.py,
-- calibrate_live_wp.py), matching 024/025 -- no write grants to anon/authenticated.
GRANT USAGE ON SCHEMA live TO anon, authenticated;
GRANT SELECT ON ALL TABLES IN SCHEMA live TO anon, authenticated;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA live FROM anon, authenticated;
