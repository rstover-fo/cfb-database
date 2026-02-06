-- api.game_box_score
-- Per-game team stats in EAV format. One row per stat per team per game.
--
-- PostgREST usage:
--   GET /api/game_box_score?game_id=eq.401628455&order=team,category

CREATE OR REPLACE VIEW api.game_box_score AS
SELECT
    gts.id AS game_id,
    g.season,
    teams.team,
    teams.home_away,
    stats.category,
    stats.stat AS stat_value
FROM core.game_team_stats gts
JOIN core.games g ON g.id = gts.id
JOIN core.game_team_stats__teams teams ON teams._dlt_parent_id = gts._dlt_id
JOIN core.game_team_stats__teams__stats stats ON stats._dlt_parent_id = teams._dlt_id;

COMMENT ON VIEW api.game_box_score IS 'Per-game team stats in EAV format. One row per stat per team per game.';
