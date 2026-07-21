-- Tier 3 analytics: walk-forward (as-of-week) ridge-adjusted EPA staging
-- =============================================================================
-- Tier 3 analytics (docs/plans/2026-07-21-tier3-analytics-plan.md), Pillar A,
-- Phase 1.
--
-- analytics.adjusted_epa_week_build mirrors analytics.adjusted_epa_build
-- (see 025) with one addition: a `week_index` column. Coefficient semantics:
--
--   Row (team, season, week_index) = the ridge coefficients ENTERING that
--   week -- i.e. fit on that season's plays with week_index < WI only. This
--   is a walk-forward, as-of rating: the value stored for week WI reflects
--   nothing that happened in that week or later, so it is safe to join onto
--   that week's game without leaking the game's own result into its own
--   pregame rating. (Team identity/column layout is fixed from the full
--   season's team list per the plan -- that's not leakage, only the fitted
--   coefficients are as-of.)
--
-- week_index, not raw CFBD week: CFBD restarts week numbering at 1 for
-- season_type='postseason' (bowls are week 1), so raw week cannot order a
-- season monotonically. Convention (shared with features.team_week, see
-- docs/brainstorms/2026-07-21-team-week-feature-design.md):
--
--   week_index = week            for season_type = 'regular'
--   week_index = 100 + week      for season_type = 'postseason'
--
-- The build emits one row per regular-week boundary plus one ENTERING-
-- POSTSEASON boundary (week_index 100 + first postseason week), whose state
-- is the full regular season -- so bowl-game lookups ("greatest week_index
-- <= WI") see every regular-season play, including the final week's.
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
-- Ridge-adjusted EPA, as-of-week grain: one row per (team, season,
-- week_index) with the fitted offense/defense coefficients entering that
-- week (week_index convention documented above).
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
    week_index INTEGER NOT NULL,
    off_coef NUMERIC(8, 5),
    def_coef NUMERIC(8, 5),
    hfa_coef NUMERIC(8, 5),
    mu NUMERIC(8, 5),
    plays BIGINT,
    lambda NUMERIC(8, 1),
    n_teams BIGINT
);

CREATE UNIQUE INDEX IF NOT EXISTS adjusted_epa_week_build_key
    ON analytics.adjusted_epa_week_build (team, season, week_index);

CREATE INDEX IF NOT EXISTS adjusted_epa_week_build_season_week_idx
    ON analytics.adjusted_epa_week_build (season, week_index);

COMMENT ON TABLE analytics.adjusted_epa_week_build IS
    'Walk-forward ridge-regressed opponent-adjusted EPA per (team, season, week_index): coefficients ENTERING that week, fit on plays with week_index < WI only (week_index = week for regular season, 100 + week for postseason) (as-of rating, no leakage of week W or later). Sign convention: off_coef higher = better offense; def_coef LOWER/more negative = better defense (EPA allowed above average). lambda is the ridge penalty recorded per row for auditability across tunable-ledger changes.';

-- Grant USAGE + read-only SELECT per the repo's read-access pattern
-- (see grant_read_access_for_security_invoker.sql).
GRANT USAGE ON SCHEMA analytics TO anon, authenticated;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO anon, authenticated;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA analytics FROM anon, authenticated;
