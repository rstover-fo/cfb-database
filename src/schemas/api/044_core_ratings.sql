-- CORE ratings API view
-- Thin passthrough of marts.core_ratings (CFBD CORE team ratings, 2016+).
-- offense higher-better; defense LOWER-better (defense_rank is ASC);
-- overall = offense - defense. through_week/through_season_type are as-of
-- markers: in-season rows are snapshots through that week, not final ratings.
-- Query with filters: ?season=eq.2025&order=overall_rank.asc
-- Exposed via PostgREST as /api/core_ratings

DROP VIEW IF EXISTS api.core_ratings;

CREATE VIEW api.core_ratings AS
SELECT *
FROM marts.core_ratings;

COMMENT ON VIEW api.core_ratings IS 'CFBD CORE ratings by team-season (2016+): overall/offense/defense with in-season overall_rank/offense_rank/defense_rank (defense ranked ascending -- lower is better) and through_week as-of markers. Backed by marts.core_ratings.';

-- Grants are part of the definition: an apply that DROPs/recreates the
-- view would otherwise leave the PostgREST roles without read access
-- (no ALTER DEFAULT PRIVILEGES for them in this database; analyst_ro is
-- covered by the api-schema default privileges in public/012).
GRANT SELECT ON api.core_ratings TO anon, authenticated;
