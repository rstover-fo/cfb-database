-- marts.team_adjusted_epa
-- Ridge-regressed opponent-adjusted EPA per team-season (Tier 2 analytics,
-- docs/plans/2026-07-21-tier2-analytics-plan.md).
-- Grain: (team, season) -- one row per team per season
-- Source: analytics.adjusted_epa_build, written by scripts/compute_adjusted_epa.py
--
-- SIGN CONVENTION (carried over from the staging table -- see
-- migrations/025_tier2_analytics_staging.sql): off_adj_epa (off_coef) HIGHER
-- = better offense (more EPA/play above average). def_adj_epa (def_coef)
-- LOWER / more negative = better defense (EPA *allowed* above average -- a
-- stingier defense pulls this further negative). net_adj_epa is therefore
-- computed as (off_coef - def_coef), NOT (off_coef + def_coef): subtracting
-- a lower (better) def_coef makes net_adj_epa larger, so net_adj_epa HIGHER
-- = better team overall, matching off_adj_epa's own "higher is better"
-- direction. def_rank is ordered ASC (most negative def_coef = best defense
-- = rank 1) to match that same convention.
--
-- wepa_total / wepa_allowed_total are CFBD's own opponent-adjusted EPA
-- (marts.team_wepa_season, itself a passthrough of metrics.wepa_team_season)
-- joined in purely as a side-by-side sanity comparison against this house
-- ridge fit -- they are not used in any computation here.

-- Prerequisite: the staging table this matview reads. DDL is intentionally
-- duplicated from migrations/025_tier2_analytics_staging.sql (which is
-- POPULATED by scripts/compute_adjusted_epa.py, not this file) so this mart
-- stands alone in any provisioning order -- mirrors the 022<->marts/011
-- precedent. Keep the two definitions in sync.
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

DROP MATERIALIZED VIEW IF EXISTS marts.team_adjusted_epa CASCADE;

CREATE MATERIALIZED VIEW marts.team_adjusted_epa AS
SELECT
    a.team,
    a.season,
    a.off_coef AS off_adj_epa,
    a.def_coef AS def_adj_epa,
    (a.off_coef - a.def_coef) AS net_adj_epa,
    a.hfa_coef,
    a.plays,
    a.lambda,

    RANK() OVER (PARTITION BY a.season ORDER BY a.off_coef DESC) AS off_rank,
    RANK() OVER (PARTITION BY a.season ORDER BY a.def_coef ASC) AS def_rank,
    RANK() OVER (PARTITION BY a.season ORDER BY (a.off_coef - a.def_coef) DESC) AS net_rank,

    -- CFBD WEPA sanity comparison (see header)
    w.epa_total AS wepa_total,
    w.epa_allowed_total AS wepa_allowed_total
FROM analytics.adjusted_epa_build a
LEFT JOIN marts.team_wepa_season w
    ON w.team = a.team AND w.season = a.season;

-- Required for REFRESH CONCURRENTLY
CREATE UNIQUE INDEX ON marts.team_adjusted_epa (team, season);

-- Query indexes
CREATE INDEX ON marts.team_adjusted_epa (season, net_rank);

-- Empty-guard: analytics.adjusted_epa_build backs this mart via
-- scripts/compute_adjusted_epa.py. If the historical fit has not run yet (or
-- the staging table ever refreshes to zero rows), fail loudly at deploy time
-- instead of silently serving an empty mart downstream.
DO $$
BEGIN
    IF (SELECT count(*) FROM marts.team_adjusted_epa) = 0 THEN
        RAISE EXCEPTION 'marts.team_adjusted_epa is empty: analytics.adjusted_epa_build has no rows. Run scripts/compute_adjusted_epa.py --from 2004 to build the historical ridge fit, then refresh this mart before use.';
    END IF;
END $$;
