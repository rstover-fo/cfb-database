-- Team returning production API view
-- Thin passthrough of marts.returning_production (PPA/usage returning from last season)
-- Query with filters: ?season=eq.2024&order=returning_rank.asc
-- Exposed via PostgREST as /api/team_returning_production

DROP VIEW IF EXISTS api.team_returning_production;

CREATE VIEW api.team_returning_production AS
SELECT *
FROM marts.returning_production;

COMMENT ON VIEW api.team_returning_production IS 'Returning production by team-season: total and percent of last season''s PPA (overall/passing/receiving/rushing) and usage returning, with returning_rank within season. Backed by marts.returning_production.';
