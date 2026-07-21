-- Tier 3 analytics: walk-forward (as-of-week) ridge-adjusted EPA staging
-- =============================================================================
-- Tier 3 analytics (docs/plans/2026-07-21-tier3-analytics-plan.md), Pillar A,
-- Phase 1.
--
-- analytics.adjusted_epa_week_build mirrors analytics.adjusted_epa_build
-- (see 025) with one addition: a `week` column. Coefficient semantics:
--
--   Row (team, season, week) = the ridge coefficients ENTERING week W --
--   i.e. fit on that season's plays with week < W only. This is a
--   walk-forward, as-of rating: the value stored for week W reflects
--   nothing that happened in week W or later, so it is safe to join onto
--   a week-W game without leaking that game's own result into its own
--   pregame rating. (Team identity/column layout is fixed from the full
--   season's team list per the plan -- that's not leakage, only the fitted
--   coefficients are as-of.)
--
-- Written by scripts/compute_adjusted_epa_week.py (RidgeAccumulator is
-- additive, so the script streams each season's plays ordered by week and
-- solves once per week boundary). Idempotent per-season DELETE+INSERT, same
-- as analytics.adjusted_epa_build (see 022's header for why that matters on
-- this compute tier). Created empty here; nothing reads this table until
-- the compute script and Pillar C's features.team_week / Pillar A's
-- --as-of-week backfill land in later phases.
--
-- analytics.* is contract-internal (docs/SCHEMA_CONTRACT.md) -- downstream
-- consumers must read the marts, never these tables directly.
--
-- Not in MIGRATION_ORDER: applied via run_migrations.py --file (deploy
-- manifest), like 019-025. Idempotent (IF NOT EXISTS throughout).

-- -----------------------------------------------------------------------------
-- Ridge-adjusted EPA, as-of-week grain: one row per (team, season, week)
-- with the fitted offense/defense coefficients entering that week.
--
-- Sign convention (matches analytics.adjusted_epa_build): off_coef higher =
-- better offense (more EPA/play above average); def_coef LOWER / more
-- negative = better defense (EPA *allowed* above average -- a stingier
-- defense pulls this further negative). lambda is the ridge penalty used
-- for that row's fit, recorded per row (not just documented in code) so
-- historical fits stay auditable even if the tunable ledger value changes
-- later.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.adjusted_epa_week_build (
    team VARCHAR NOT NULL,
    season BIGINT NOT NULL,
    week INTEGER NOT NULL,
    off_coef NUMERIC(8, 5),
    def_coef NUMERIC(8, 5),
    hfa_coef NUMERIC(8, 5),
    mu NUMERIC(8, 5),
    plays BIGINT,
    lambda NUMERIC(8, 1),
    n_teams BIGINT
);

CREATE UNIQUE INDEX IF NOT EXISTS adjusted_epa_week_build_key
    ON analytics.adjusted_epa_week_build (team, season, week);

CREATE INDEX IF NOT EXISTS adjusted_epa_week_build_season_week_idx
    ON analytics.adjusted_epa_week_build (season, week);

COMMENT ON TABLE analytics.adjusted_epa_week_build IS
    'Walk-forward ridge-regressed opponent-adjusted EPA per (team, season, week): coefficients ENTERING week W, fit on plays with week < W only (as-of rating, no leakage of week W or later). Sign convention: off_coef higher = better offense; def_coef LOWER/more negative = better defense (EPA allowed above average). lambda is the ridge penalty recorded per row for auditability across tunable-ledger changes.';

-- Grant USAGE + read-only SELECT per the repo's read-access pattern
-- (see grant_read_access_for_security_invoker.sql).
GRANT USAGE ON SCHEMA analytics TO anon, authenticated;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO anon, authenticated;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA analytics FROM anon, authenticated;
