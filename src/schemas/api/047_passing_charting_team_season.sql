-- api.passing_charting_team_season
-- Team-season passing charting, offense and defense sides (2025+). Thin
-- passthrough of marts.passing_charting_team_season, which flattens the raw
-- dlt offense__*/defense__* dunder shape to offense_*/defense_* -- per this
-- repo's Contract Rule 4, raw dlt column shapes must never reach the api
-- layer.
--
-- CRITICAL: defense_* is THIS TEAM'S PASSING DEFENSE -- what opposing
-- offenses did against them -- NOT the opponent's own offensive row.
-- NULL on a metric means those plays were not charted; 0 is a real observed
-- value. offense_air_yards_attempts_available/
-- offense_yards_after_catch_attempts_available (and the defense_
-- equivalents) are the charting-coverage denominators for their respective
-- metric pairs and must never be merged/aliased together. Data starts 2025.
--
-- PostgREST usage:
--   GET /api/passing_charting_team_season?season=eq.2025&order=offense_total_air_yards.desc

DROP VIEW IF EXISTS api.passing_charting_team_season;

CREATE VIEW api.passing_charting_team_season AS
SELECT *
FROM marts.passing_charting_team_season;

COMMENT ON VIEW api.passing_charting_team_season IS 'Team-season passing charting, offense and defense sides (2025+): season, team, conference, offense_total_air_yards, offense_average_depth_of_target, offense_air_yards_attempts_available, offense_total_yards_after_catch, offense_average_yards_after_catch, offense_yards_after_catch_attempts_available, and the defense_ equivalents. defense_* is this team''s passing DEFENSE (what opposing offenses did against them), not the opponent''s offensive row. NULL on a metric means those plays were not charted, 0 is a real observed value; the two *_attempts_available columns per side are the charting-coverage denominators. Backed by marts.passing_charting_team_season.';

GRANT SELECT ON api.passing_charting_team_season TO anon, authenticated;
