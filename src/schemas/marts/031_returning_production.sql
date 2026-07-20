-- marts.returning_production
-- Returning production by team-season: how much of last season's PPA production
-- (overall/passing/receiving/rushing) is returning, plus returning usage shares.
-- Passthrough of stats.player_returning with friendlier percent_* naming.
-- Grain: (season, team) -- one row per team per season
-- Source: stats.player_returning (CFBD GET /player/returning)
--
-- LIVE-VERIFIED SOURCE COLUMNS (2026-07-20 presence check against production
-- information_schema; supersedes the prior "ASSUMED" column list). The loader
-- (src/pipelines/sources/stats.py::player_returning_resource, ~L219-251)
-- yields the raw API dict unmodified, so dlt's default naming convention 1:1
-- snake_cases each camelCase field below. The endpoint's response has NO
-- nested objects, so no double-underscore columns are expected here (contrast
-- with stats.player_usage's nested "usage" object) -- EXCEPT for
-- total_receiving_ppa, where dlt type-inferred a bigint for some rows; a
-- double-typed row's value instead lands in the VARIANT twin
-- total_receiving_ppa__v_double -- ALWAYS COALESCE the pair.
--   season                (bigint) -- primary key component
--   team                  (text)   -- primary key component
--   conference            (text)
--   total_ppa             (float)   <- totalPPA
--   total_passing_ppa     (float)   <- totalPassingPPA
--   total_receiving_ppa   (bigint)  <- totalReceivingPPA -- COALESCE with
--                                       total_receiving_ppa__v_double (float)
--   total_rushing_ppa     (float)   <- totalRushingPPA
--   percent_ppa           (float)   <- percentPPA
--   percent_passing_ppa   (float)   <- percentPassingPPA
--   percent_receiving_ppa (float)   <- percentReceivingPPA
--   percent_rushing_ppa   (float)   <- percentRushingPPA
--   usage                 (float)   <- usage (scalar, NOT nested -- unlike player_usage)
--   passing_usage         (float)   <- passingUsage
--   receiving_usage       (float)   <- receivingUsage
--   rushing_usage         (float)   <- rushingUsage
-- If a future deploy fails with "column ... does not exist", re-run the
-- presence check against information_schema.columns for stats.player_returning.

DROP MATERIALIZED VIEW IF EXISTS marts.returning_production CASCADE;

CREATE MATERIALIZED VIEW marts.returning_production AS
SELECT
    p.season,
    p.team,
    p.conference,

    -- Total PPA returning (absolute). total_receiving_ppa COALESCEs the
    -- bigint column with its dlt __v_double VARIANT twin so variant-typed
    -- rows aren't silently NULLed out.
    ROUND(p.total_ppa::numeric, 2) AS total_ppa,
    ROUND(p.total_passing_ppa::numeric, 2) AS total_passing_ppa,
    ROUND(COALESCE(p.total_receiving_ppa::double precision, p.total_receiving_ppa__v_double)::numeric, 2) AS total_receiving_ppa,
    ROUND(p.total_rushing_ppa::numeric, 2) AS total_rushing_ppa,

    -- Percent of last season's PPA returning (renamed from percent_* for clarity)
    ROUND(p.percent_ppa::numeric, 4) AS returning_ppa_pct,
    ROUND(p.percent_passing_ppa::numeric, 4) AS returning_passing_ppa_pct,
    ROUND(p.percent_receiving_ppa::numeric, 4) AS returning_receiving_ppa_pct,
    ROUND(p.percent_rushing_ppa::numeric, 4) AS returning_rushing_ppa_pct,

    -- Returning usage shares
    ROUND(p."usage"::numeric, 4) AS usage,
    ROUND(p.passing_usage::numeric, 4) AS passing_usage,
    ROUND(p.receiving_usage::numeric, 4) AS receiving_usage,
    ROUND(p.rushing_usage::numeric, 4) AS rushing_usage,

    -- Computed ranking within season
    RANK() OVER (PARTITION BY p.season ORDER BY p.percent_ppa DESC NULLS LAST) AS returning_rank

FROM stats.player_returning p;

-- Required for REFRESH CONCURRENTLY
CREATE UNIQUE INDEX ON marts.returning_production (team, season);

-- Query indexes
CREATE INDEX ON marts.returning_production (season);
CREATE INDEX ON marts.returning_production (season, returning_rank);

-- Empty-guard: stats.player_returning backs this mart. It is one of the Phase 0
-- gate tables (docs/db-snapshot-current.json predates it) -- if it refreshes to
-- zero rows, fail loudly at deploy time instead of silently serving an empty
-- mart downstream.
DO $$
BEGIN
    IF (SELECT count(*) FROM marts.returning_production) = 0 THEN
        RAISE EXCEPTION 'marts.returning_production is empty: stats.player_returning has no rows. Run the stats backfill (deploy/tier1-backfill, action=backfill, sources=stats) and refresh this mart before use.';
    END IF;
END $$;
