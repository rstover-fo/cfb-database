-- marts.adjusted_epa_week
-- =============================================================================
-- Tier 3 analytics (docs/plans/2026-07-21-tier3-analytics-plan.md), Pillar A
-- (walk-forward honesty) / Pillar C exposure, Phase 3.
--
-- Walk-forward TRANSPARENCY surface: SELECT * FROM
-- analytics.adjusted_epa_week_build, the as-of ridge-adjusted-EPA
-- coefficients underlying features.team_week's adj_epa_* columns (migration
-- 026). Exposed as its own mart/api pair -- separate from
-- marts.team_week_features / api.team_week_features -- so the raw per-week
-- fit is independently queryable and auditable, not just team_week's
-- resolved-with-fallback values.
--
-- Grain: (team, season, week_index). Row (team, season, week_index = WI)
-- holds the ridge coefficients ENTERING week WI -- i.e. fit on that season's
-- plays with week_index < WI only (see migration 027's header for the full
-- walk-forward derivation). This is week_index, not raw CFBD week, because
-- CFBD restarts week numbering at 1 for season_type = 'postseason' (bowls
-- are week 1):
--   week_index = week            for season_type = 'regular'
--   week_index = 100 + week      for season_type = 'postseason'
-- (the postseason boundary's state is the full regular season, so bowl-game
-- lookups against "greatest week_index <= WI" see every regular-season
-- play, including the final week's).
--
-- SIGN CONVENTION (matches marts.team_adjusted_epa / migration 027): off_coef
-- HIGHER = better offense (more EPA/play above average); def_coef LOWER /
-- more negative = better defense (EPA *allowed* above average -- a stingier
-- defense pulls this further negative). lambda is the ridge penalty recorded
-- per row (not just documented in code) so historical fits stay auditable
-- even if the tunable ledger value changes later.
--
-- Written by scripts/compute_adjusted_epa_week.py, backfilled 2004+.

DROP MATERIALIZED VIEW IF EXISTS marts.adjusted_epa_week CASCADE;

CREATE MATERIALIZED VIEW marts.adjusted_epa_week AS
SELECT * FROM analytics.adjusted_epa_week_build;

-- Required for REFRESH CONCURRENTLY; also the natural grain key (matches
-- analytics.adjusted_epa_week_build's own UNIQUE (team, season, week_index)).
CREATE UNIQUE INDEX ON marts.adjusted_epa_week (team, season, week_index);

-- Query index
CREATE INDEX ON marts.adjusted_epa_week (season, week_index);

-- Empty-guard: analytics.adjusted_epa_week_build is backed by
-- scripts/compute_adjusted_epa_week.py's 2004+ backfill. If that has not run
-- yet (or ever refreshes to zero rows), fail loudly at deploy time instead of
-- silently serving an empty mart downstream -- same convention as
-- marts/036_team_adjusted_epa.sql.
DO $$
BEGIN
    IF (SELECT count(*) FROM marts.adjusted_epa_week) = 0 THEN
        RAISE EXCEPTION 'marts.adjusted_epa_week is empty: analytics.adjusted_epa_week_build has no rows. Run scripts/compute_adjusted_epa_week.py --from 2004 to build the historical as-of-week fit, then refresh this mart before use.';
    END IF;
END $$;
