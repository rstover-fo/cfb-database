-- marts.player_usage
-- Player usage rates by season: overall/pass/rush/down-split shares of a team's
-- plays a given athlete was on the field for. Passthrough of stats.player_usage
-- with the nested "usage" object flattened.
-- Grain: (season, athlete_id) -- one row per player per season
-- Source: stats.player_usage (CFBD GET /player/usage)
--
-- LIVE-VERIFIED SOURCE COLUMNS (2026-07-20 presence check against production
-- information_schema; supersedes the prior "ASSUMED" column list). The loader
-- (src/pipelines/sources/stats.py::player_usage_resource, ~L184-216) yields
-- the raw API dict unmodified. The primary key is ["season", "id"]
-- (src/pipelines/config/endpoints.py), confirming a top-level "id" column
-- survives dlt normalization untouched (it is already snake_case). The
-- response's "usage" field is a NESTED OBJECT (unlike stats.player_returning's
-- scalar "usage"), so dlt's default naming convention flattens it with a
-- double-underscore separator:
--   season          (bigint)  -- primary key component
--   id              (text)    -- primary key component; athlete id, exposed as athlete_id
--   name            (text)    -- exposed as player_name
--   position        (text)
--   team            (text)
--   conference      (text)
--   usage__overall         (float)   <- usage.overall
--   usage__pass            (bigint)  <- usage.pass -- dlt type-inferred bigint;
--                                        a double-typed row's value instead
--                                        lands in the VARIANT twin below --
--                                        ALWAYS COALESCE the pair
--   usage__pass__v_double  (float)   -- dlt VARIANT twin of usage__pass
--   usage__rush            (float)   <- usage.rush
--   usage__first_down      (float)   <- usage.firstDown
--   usage__second_down     (float)   <- usage.secondDown
--   usage__third_down      (bigint)  <- usage.thirdDown -- same bigint/VARIANT
--                                        split as usage__pass; COALESCE it
--   usage__third_down__v_double (float)  -- dlt VARIANT twin of usage__third_down
--   usage__standard_downs  (float)   <- usage.standardDowns
--   usage__passing_downs   (float)   <- usage.passingDowns
-- If a future deploy fails with "column ... does not exist", re-run the
-- presence check against information_schema.columns for stats.player_usage --
-- dlt's variant-typing behavior can add new __v_* columns as data shape shifts.

DROP MATERIALIZED VIEW IF EXISTS marts.player_usage CASCADE;

CREATE MATERIALIZED VIEW marts.player_usage AS
SELECT
    u.season,
    u.id AS athlete_id,
    u.name AS player_name,
    u.position,
    u.team,
    u.conference,

    -- Usage shares, flattened from the nested "usage" object. usage__pass and
    -- usage__third_down COALESCE the bigint column with its dlt __v_double
    -- VARIANT twin so variant-typed rows aren't silently NULLed out.
    ROUND(u."usage__overall"::numeric, 4) AS usage_overall,
    ROUND(COALESCE(u."usage__pass"::double precision, u."usage__pass__v_double")::numeric, 4) AS usage_pass,
    ROUND(u."usage__rush"::numeric, 4) AS usage_rush,
    ROUND(u."usage__first_down"::numeric, 4) AS usage_first_down,
    ROUND(u."usage__second_down"::numeric, 4) AS usage_second_down,
    ROUND(COALESCE(u."usage__third_down"::double precision, u."usage__third_down__v_double")::numeric, 4) AS usage_third_down,
    ROUND(u."usage__standard_downs"::numeric, 4) AS usage_standard_downs,
    ROUND(u."usage__passing_downs"::numeric, 4) AS usage_passing_downs

FROM stats.player_usage u;

-- Required for REFRESH CONCURRENTLY
CREATE UNIQUE INDEX ON marts.player_usage (season, athlete_id);

-- Query indexes
CREATE INDEX ON marts.player_usage (season, team);

-- Empty-guard: stats.player_usage backs this mart. It is one of the Phase 0
-- gate tables (docs/db-snapshot-current.json predates it) -- if it refreshes to
-- zero rows, fail loudly at deploy time instead of silently serving an empty
-- mart downstream.
DO $$
BEGIN
    IF (SELECT count(*) FROM marts.player_usage) = 0 THEN
        RAISE EXCEPTION 'marts.player_usage is empty: stats.player_usage has no rows. Run the stats backfill (deploy/tier1-backfill, action=backfill, sources=stats) and refresh this mart before use.';
    END IF;
END $$;
