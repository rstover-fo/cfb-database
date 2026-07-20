-- Team WEPA season API view
-- Thin passthrough of marts.team_wepa_season (opponent-adjusted EPA)
-- Query with filters: ?season=eq.2024&order=epa_rank.asc
-- Exposed via PostgREST as /api/team_wepa_season

DROP VIEW IF EXISTS api.team_wepa_season;

CREATE VIEW api.team_wepa_season AS
SELECT *
FROM marts.team_wepa_season;

COMMENT ON VIEW api.team_wepa_season IS 'Opponent-adjusted EPA (WEPA) by team-season: EPA/success-rate/explosiveness for and against, rushing yardage splits, and epa_rank/defense_rank. Backed by marts.team_wepa_season.';
