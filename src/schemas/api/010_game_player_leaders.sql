-- api.game_player_leaders
-- Per-game player stats flattened from dlt hierarchy.
-- Replaces core_staging dependency for cfb-app.
--
-- PostgREST usage:
--   GET /api/game_player_leaders?game_id=eq.401628455&category=eq.passing&order=stat.desc

CREATE OR REPLACE VIEW api.game_player_leaders AS
SELECT
    gps.id AS game_id,
    g.season,
    teams.team,
    teams.conference,
    teams.home_away,
    cats.name AS category,
    types.name AS stat_type,
    athletes.id AS player_id,
    athletes.name AS player_name,
    athletes.stat
FROM core.game_player_stats gps
JOIN core.games g ON g.id = gps.id
JOIN core.game_player_stats__teams teams ON teams._dlt_parent_id = gps._dlt_id
JOIN core.game_player_stats__teams__categories cats ON cats._dlt_parent_id = teams._dlt_id
JOIN core.game_player_stats__teams__categories__types types ON types._dlt_parent_id = cats._dlt_id
JOIN core.game_player_stats__teams__categories__types__athletes athletes ON athletes._dlt_parent_id = types._dlt_id;

COMMENT ON VIEW api.game_player_leaders IS 'Per-game player stats flattened from dlt hierarchy. Replaces core_staging dependency for cfb-app.';
