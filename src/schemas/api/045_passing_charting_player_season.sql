-- api.passing_charting_player_season
-- Passing charting by player-season (2025+): air yards, aDOT, YAC. Thin
-- passthrough of marts.passing_charting_player_season.
--
-- NULL means the play(s) behind that value were not charted; 0 is a real
-- observed value. air_yards_attempts_available and
-- yards_after_catch_attempts_available are the two charting-coverage
-- denominators for the air-yards and YAC metric pairs respectively -- they
-- are DIFFERENT counts and must never be merged/aliased into one: a
-- leaderboard on the metrics alone, without both denominators alongside,
-- ranks on charting coverage rather than skill (2025: 407/820
-- player-seasons charted). Data starts 2025.
--
-- position is joined from core.roster on its own primary key (id, team,
-- year) -- never a name-string join -- so it can be legitimately NULL for a
-- player-season with no matching roster row; conference comes straight off
-- stats.passing_player_season, no join.
--
-- PostgREST usage:
--   GET /api/passing_charting_player_season?season=eq.2025&order=total_air_yards.desc

DROP VIEW IF EXISTS api.passing_charting_player_season;

CREATE VIEW api.passing_charting_player_season AS
SELECT *
FROM marts.passing_charting_player_season;

COMMENT ON VIEW api.passing_charting_player_season IS 'Passing charting by player-season (2025+): season, player_id, player, team, conference, position, attempts, completions, interceptions, completion_rate, total_air_yards, average_depth_of_target, air_yards_attempts_available, total_yards_after_catch, average_yards_after_catch, yards_after_catch_attempts_available. NULL on a charting metric means those plays were not charted (0 is a real value); air_yards_attempts_available/yards_after_catch_attempts_available are the two charting-coverage denominators and are never interchangeable. CONTRACT: yards_after_catch_attempts_available <= completions (YAC exists only on completions; live-verified 840/840 rows 2026-08-31) -- YAC coverage at this grain reads yards_after_catch_attempts_available / completions, never / attempts. Backed by marts.passing_charting_player_season.';

GRANT SELECT ON api.passing_charting_player_season TO anon, authenticated;
