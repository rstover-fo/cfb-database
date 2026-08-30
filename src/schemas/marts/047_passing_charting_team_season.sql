-- marts.passing_charting_team_season
-- =============================================================================
-- Passing charting (spec v5.25.0, 2026-08-30 expansion_views unit, task 4).
-- Thin per-row mart over stats.passing_team_season (PK season, team).
-- Flattens the raw dlt dunder shape (offense__*/defense__*) to
-- offense_*/defense_* -- per this repo's Contract Rule 4, raw dlt column
-- shapes must never reach the api layer.
--
-- Only the SIX charting-family columns per side are flattened here (4
-- metrics + 2 coverage denominators -- the same six as task 2's player-
-- season mart), not the full attempts/completions/interceptions/
-- completion_rate/totalYards family: stats.passing_team_season nests those
-- under offense__/defense__ too, but they are plain box-score counts, not
-- charting-derived, so they carry no NULL-vs-not-charted ambiguity and were
-- not called out in the work order for this unit. conference is included
-- (already a plain column on stats.passing_team_season, no join, no fanout
-- risk) for parity with task 2's player-season mart.
--
-- Column names verified against
-- src/schemas/migrations/057_passing_grants_indexes.sql (applied, column
-- comments already live) and the 2026-08-30 probe fixture
-- tests/fixtures/cfbd_2026/passing_teams_season.json -- NOT a fresh
-- pg_attribute read from this session (no DB access here). Re-verify at
-- deploy time.
--
-- CRITICAL semantic note (migration 057's phrasing, verbatim): defense_* is
-- THIS TEAM'S PASSING DEFENSE -- what opposing offenses did against them --
-- NOT the opponent's own offensive row. NULL on a metric means the play(s)
-- behind it were not charted; 0 is a real observed value.
-- offense_air_yards_attempts_available / offense_yards_after_catch_attempts_available
-- (and the defense_ equivalents) are the charting-coverage denominators for
-- their respective metric pairs and must never be merged/aliased together.
-- Data starts 2025.
--
-- DEPLOY-ORDER CAVEAT (same shape as migration 057's guarded COMMENT
-- block): dlt omits a column entirely from a table's schema when every
-- value loaded for it so far was NULL. The four *_total_air_yards/
-- *_average_depth_of_target/*_total_yards_after_catch/
-- *_average_yards_after_catch columns per side are the ones actually at
-- risk of this (the two *_attempts_available denominators are plain
-- integers that are 0, not NULL, when nothing is charted, so they always
-- materialize). A plain CREATE MATERIALIZED VIEW AS SELECT cannot guard a
-- possibly-absent column the way migration 057's DO block guards a COMMENT,
-- so this file must be applied AFTER the passing source's first successful
-- load, same precondition as 057 itself -- per that migration's header,
-- deploy run 188 (post-backfill) already had all 36 charting-family columns
-- live, so this is a which-order-was-this-applied-in check, not a design
-- gap. If a fresh/unbackfilled database ever applies this file first, it
-- will fail with "column does not exist" on whichever metric column dlt
-- has not yet created -- reapply after the next passing load completes.
DROP MATERIALIZED VIEW IF EXISTS marts.passing_charting_team_season CASCADE;

CREATE MATERIALIZED VIEW marts.passing_charting_team_season AS
SELECT
    pts.season,
    pts.team,
    pts.conference,
    pts.offense__total_air_yards AS offense_total_air_yards,
    pts.offense__average_depth_of_target AS offense_average_depth_of_target,
    pts.offense__air_yards_attempts_available AS offense_air_yards_attempts_available,
    pts.offense__total_yards_after_catch AS offense_total_yards_after_catch,
    pts.offense__average_yards_after_catch AS offense_average_yards_after_catch,
    pts.offense__yards_after_catch_attempts_available AS offense_yards_after_catch_attempts_available,
    pts.defense__total_air_yards AS defense_total_air_yards,
    pts.defense__average_depth_of_target AS defense_average_depth_of_target,
    pts.defense__air_yards_attempts_available AS defense_air_yards_attempts_available,
    pts.defense__total_yards_after_catch AS defense_total_yards_after_catch,
    pts.defense__average_yards_after_catch AS defense_average_yards_after_catch,
    pts.defense__yards_after_catch_attempts_available AS defense_yards_after_catch_attempts_available
FROM stats.passing_team_season pts;

-- Required for REFRESH CONCURRENTLY
CREATE UNIQUE INDEX ON marts.passing_charting_team_season (season, team);
