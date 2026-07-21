-- Tier 2 analytics staging tables: house Elo + ridge-adjusted EPA
-- =============================================================================
-- Tier 2 analytics (docs/plans/2026-07-21-tier2-analytics-plan.md), Phase 1.
--
-- Three analytics.* staging tables populated by the Python compute scripts:
--   - analytics.house_elo_game / analytics.house_elo_current, written by
--     scripts/compute_house_elo.py (Phase 2)
--   - analytics.adjusted_epa_build, written by scripts/compute_adjusted_epa.py
--     (Phase 3)
-- Both scripts write idempotently, per season, via DELETE+INSERT (never a
-- single giant statement -- see 022's header for why that matters on this
-- compute tier). These tables are created empty here; nothing reads them
-- until the compute scripts and marts 034-036 land in later phases.
--
-- analytics.* is contract-internal (docs/SCHEMA_CONTRACT.md) -- downstream
-- consumers must read the marts, never these tables directly.
--
-- NOTE: these CREATE TABLEs are intentionally duplicated IF-NOT-EXISTS in the
-- consuming marts (034-036), mirroring the 022<->marts/011 precedent so each
-- mart file stands alone in any provisioning order; keep the definitions
-- in sync.
--
-- Not in MIGRATION_ORDER: applied via run_migrations.py --file (deploy
-- manifest), like 019-024. Idempotent (IF NOT EXISTS throughout).

-- -----------------------------------------------------------------------------
-- House Elo: game-grain history (pregame/postgame Elo both sides, win prob,
-- expected vs actual margin, CFBD Elo copies retained for validation).
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.house_elo_game (
    game_id BIGINT NOT NULL,
    season BIGINT NOT NULL,
    week BIGINT,
    season_type VARCHAR,
    start_date TIMESTAMPTZ,
    neutral_site BOOLEAN,
    home_team VARCHAR NOT NULL,
    away_team VARCHAR NOT NULL,

    home_pregame_elo NUMERIC(8, 2),
    away_pregame_elo NUMERIC(8, 2),
    home_postgame_elo NUMERIC(8, 2),
    away_postgame_elo NUMERIC(8, 2),
    home_win_prob NUMERIC(5, 4),
    expected_home_margin NUMERIC(6, 2),
    actual_home_margin BIGINT,
    mov_multiplier NUMERIC(6, 3),

    cfbd_home_pregame_elo NUMERIC(8, 2),
    cfbd_away_pregame_elo NUMERIC(8, 2)
);

CREATE UNIQUE INDEX IF NOT EXISTS house_elo_game_key
    ON analytics.house_elo_game (game_id);

CREATE INDEX IF NOT EXISTS house_elo_game_season_idx
    ON analytics.house_elo_game (season);

CREATE INDEX IF NOT EXISTS house_elo_game_home_team_season_idx
    ON analytics.house_elo_game (home_team, season);

CREATE INDEX IF NOT EXISTS house_elo_game_away_team_season_idx
    ON analytics.house_elo_game (away_team, season);

-- -----------------------------------------------------------------------------
-- House Elo: live per-team rating snapshot (one row per team, most recent
-- rating carried forward across seasons per the plan's carryover rule).
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.house_elo_current (
    team VARCHAR NOT NULL,
    season BIGINT,
    rating NUMERIC(8, 2) NOT NULL,
    games_played BIGINT,
    last_game_id BIGINT,
    last_game_date TIMESTAMPTZ,
    low_confidence BOOLEAN,
    updated_at TIMESTAMPTZ
);

CREATE UNIQUE INDEX IF NOT EXISTS house_elo_current_key
    ON analytics.house_elo_current (team);

-- -----------------------------------------------------------------------------
-- Ridge-adjusted EPA: one row per (team, season) with the fitted
-- offense/defense coefficients from scripts/compute_adjusted_epa.py.
--
-- Sign convention: off_coef higher = better offense (more EPA/play above
-- average); def_coef LOWER / more negative = better defense (EPA *allowed*
-- above average -- a stingier defense pulls this further negative). lambda
-- is the ridge penalty used for that row's fit, recorded per row (not just
-- documented in code) so historical fits stay auditable even if the tunable
-- ledger value changes later.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.adjusted_epa_build (
    team VARCHAR NOT NULL,
    season BIGINT NOT NULL,
    off_coef NUMERIC(8, 5),
    def_coef NUMERIC(8, 5),
    hfa_coef NUMERIC(8, 5),
    mu NUMERIC(8, 5),
    plays BIGINT,
    lambda NUMERIC(8, 1),
    n_teams BIGINT
);

CREATE UNIQUE INDEX IF NOT EXISTS adjusted_epa_build_key
    ON analytics.adjusted_epa_build (team, season);

COMMENT ON TABLE analytics.adjusted_epa_build IS
    'Ridge-regressed opponent-adjusted EPA per (team, season). Sign convention: off_coef higher = better offense; def_coef LOWER/more negative = better defense (EPA allowed above average). lambda is the ridge penalty recorded per row for auditability across tunable-ledger changes.';
