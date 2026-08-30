-- marts.passing_charting_player_season
-- =============================================================================
-- Passing charting (spec v5.25.0, 2026-08-30 expansion_views unit, task 2).
-- Thin per-row mart over stats.passing_player_season (PK season, player_id,
-- team) -- charting is player-season-grain already; nothing to aggregate.
--
-- Column names verified against src/schemas/migrations/057_passing_grants_indexes.sql
-- (applied, column comments already live) and the 2026-08-30 probe fixture
-- tests/fixtures/cfbd_2026/passing_players_season.json -- NOT a fresh
-- pg_attribute read from this session (no DB access here). Re-verify at
-- deploy time per this repo's column-contract convention.
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
-- `position` is joined from core.roster on (id, team, year) -- ALL THREE of
-- core.roster's own primary-key columns (src/pipelines/sources/rosters.py),
-- matched against passing_player_season's player_id/team/season -- so this
-- is a join to roster's exact PK, not a name-string join, and cannot fan
-- out: at most one roster row can match. `conference` needs no join at all
-- -- it is already a column on stats.passing_player_season itself (per the
-- probe fixture).
--
-- DEPLOY-ORDER CAVEAT (same shape as migration 057's guarded COMMENT
-- block): dlt omits a column entirely from a table's schema when every
-- value loaded for it so far was NULL. total_air_yards/
-- average_depth_of_target/total_yards_after_catch/average_yards_after_catch
-- are the ones actually at risk of this (the two *_attempts_available
-- denominators are plain integers that are 0, not NULL, when nothing is
-- charted, so they always materialize). Per 057's header, deploy run 188
-- (post-backfill) already had all of these columns live, so apply this file
-- after that same passing-source load, not before.
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
    pps.average_yards_after_catch,
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
