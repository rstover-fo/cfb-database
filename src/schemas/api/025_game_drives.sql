-- api.game_drives
-- Drive-by-drive summary for a game, one row per possession.
--
-- Source: core.drives (dlt-loaded from CFBD /drives). No join to core.games is
-- needed: core.drives.season is populated directly by the ingestion pipeline
-- (src/pipelines/sources/games.py sets drive["season"] = year for every row,
-- same pattern as core.plays), and core.drives.is_home_offense already exists
-- as a native column -- it does not need to be derived from games.home_team.
--
-- PostgREST usage:
--   GET /api/game_drives?game_id=eq.401628455&order=drive_number

CREATE OR REPLACE VIEW api.game_drives AS
SELECT
    d.game_id,
    d.season,
    d.drive_number,
    d.offense,
    d.defense,
    d.start_period,
    d.start_yards_to_goal,
    d.end_yards_to_goal,
    d.plays,
    d.yards,
    d.drive_result,
    d.scoring,
    d.start_offense_score,
    d.end_offense_score,
    d.start_defense_score,
    d.end_defense_score,
    d.start_time__minutes AS start_time_minutes,
    d.start_time__seconds AS start_time_seconds,
    d.elapsed__minutes AS elapsed_minutes,
    d.elapsed__seconds AS elapsed_seconds,
    d.is_home_offense
FROM core.drives d;

GRANT SELECT ON api.game_drives TO anon, authenticated;

COMMENT ON VIEW api.game_drives IS 'Drive-by-drive summary for a game. One row per possession. Columns: game_id, season, drive_number, offense, defense, start_period, start_yards_to_goal, end_yards_to_goal, plays, yards, drive_result, scoring, start_offense_score, end_offense_score, start_defense_score, end_defense_score, start_time_minutes, start_time_seconds, elapsed_minutes, elapsed_seconds, is_home_offense.';
