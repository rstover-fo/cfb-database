-- marts.passing_charting_player_season
-- =============================================================================
-- Passing charting (spec v5.25.0, 2026-08-30 expansion_views unit, task 2).
-- Thin per-row mart over stats.passing_player_season (PK season, player_id,
-- team) -- charting is player-season-grain already; nothing to aggregate.
--
-- Column names LIVE-VERIFIED 2026-08-30 via information_schema.columns
-- against stats.passing_player_season (supersedes the prior
-- migration-057/fixture-only check), including a check for dlt VARIANT
-- (__v_double) twins on every bigint-typed column -- same pattern as
-- src/schemas/009_variant_columns.sql / marts/032_player_usage.sql. Result:
--   - total_air_yards, total_yards_after_catch: bigint, but integer-BY-NATURE
--     (sums of whole charted yards) -- no twin is possible for these, and
--     none exists live.
--   - average_depth_of_target, completion_rate: natively double precision --
--     no twin, read directly.
--   - average_yards_after_catch: bigint base column WITH a __v_double twin
--     -- live, 254/365 real (non-null) values are stranded in
--     average_yards_after_catch__v_double alone. COALESCEd below; this
--     mart's own NULL-means-"not charted" semantics (next paragraph) made
--     the miss actively misreport charted plays as uncharted.
--
-- NULL/denominator semantics (mirrors migration 057's phrasing verbatim):
-- NULL on total_air_yards/average_depth_of_target/total_yards_after_catch/
-- average_yards_after_catch means the play(s) behind that value were not
-- charted; 0 is a real observed value. air_yards_attempts_available and
-- yards_after_catch_attempts_available are the two charting-coverage
-- denominators -- they are DIFFERENT counts (air-yards and YAC charting can
-- cover different play sets for the same player-season) and must never be
-- merged or aliased into one "attempts_available" column: a leaderboard
-- built on the metrics alone, without both denominators alongside, ranks on
-- charting coverage rather than skill (2025: 407/820 player-seasons
-- charted). Data starts 2025 (PASSING_DATA_START in
-- src/pipelines/sources/passing.py).
--
-- Sibling marts unaffected (reviewer-confirmed 2026-08-30):
-- marts.passing_charting_target_season (046) computes its air-yards/YAC
-- aggregates fresh via SUM()/AVG() over stats.passing_plays' plain bigint
-- columns, which have no twin to miss; marts.passing_charting_team_season
-- (047) flattens stats.passing_team_season's offense__/defense__ metric
-- family, which are natively double precision on that table, same as this
-- table's average_depth_of_target/completion_rate above.
--
-- `position` is joined from core.roster on (id, team, year) -- ALL THREE of
-- core.roster's own primary-key columns (src/pipelines/sources/rosters.py),
-- matched against passing_player_season's player_id/team/season -- so this
-- is a join to roster's exact PK, not a name-string join, and cannot fan
-- out: at most one roster row can match. `conference` needs no join at all
-- -- it is already a column on stats.passing_player_season itself (per the
-- probe fixture).
--
-- DEPLOY-ORDER CAVEAT (same shape as migration 057's guarded COMMENT
-- block, kept for a future fresh/re-provisioned database, not a live
-- concern today): dlt omits a column entirely from a table's schema when
-- every value loaded for it so far was NULL. total_air_yards/
-- average_depth_of_target/total_yards_after_catch/average_yards_after_catch
-- (plus average_yards_after_catch__v_double) are the ones that would be at
-- risk of this (the two *_attempts_available denominators are plain
-- integers that are 0, not NULL, when nothing is charted, so they always
-- materialize). The 2026-08-30 live check above confirms every column this
-- file reads already exists today; a fresh database must still apply this
-- file after the passing source's first load, not before.
DROP MATERIALIZED VIEW IF EXISTS marts.passing_charting_player_season CASCADE;

CREATE MATERIALIZED VIEW marts.passing_charting_player_season AS
SELECT
    pps.season,
    pps.player_id,
    pps.player,
    pps.team,
    pps.conference,
    r.position,
    pps.attempts,
    pps.completions,
    pps.interceptions,
    pps.completion_rate,
    pps.total_air_yards,
    pps.average_depth_of_target,
    pps.air_yards_attempts_available,
    pps.total_yards_after_catch,
    -- bigint base column WITH a dlt __v_double VARIANT twin on this table
    -- (live-verified 2026-08-30: 254/365 real values stranded in the twin
    -- alone) -- COALESCE per src/schemas/009_variant_columns.sql /
    -- marts/032_player_usage.sql:56's codified pattern. See file header.
    COALESCE(pps.average_yards_after_catch::double precision, pps.average_yards_after_catch__v_double)
        AS average_yards_after_catch,
    pps.yards_after_catch_attempts_available
FROM stats.passing_player_season pps
LEFT JOIN core.roster r
    ON r.id::text = pps.player_id::text
    AND r.team = pps.team
    AND r.year = pps.season;

-- Required for REFRESH CONCURRENTLY
CREATE UNIQUE INDEX ON marts.passing_charting_player_season (season, player_id, team);

-- Query indexes
CREATE INDEX ON marts.passing_charting_player_season (season, team);
CREATE INDEX ON marts.passing_charting_player_season (player_id);
