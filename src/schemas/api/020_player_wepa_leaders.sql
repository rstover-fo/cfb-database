-- Player WEPA leaders API view
-- Thin passthrough of marts.player_wepa_season (player WEPA passing/rushing, kicker PAAR)
-- Query with filters: ?season=eq.2024&category=eq.passing&order=season_rank.asc
-- Exposed via PostgREST as /api/player_wepa_leaders

DROP VIEW IF EXISTS api.player_wepa_leaders;

CREATE VIEW api.player_wepa_leaders AS
SELECT *
FROM marts.player_wepa_season;

COMMENT ON VIEW api.player_wepa_leaders IS 'Player WEPA leaders: passing/rushing WEPA and kicker PAAR, tall grain (season, athlete_id, category), ranked within season+category via season_rank. Backed by marts.player_wepa_season.';
