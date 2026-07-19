-- Player usage leaders API view
-- Thin passthrough of marts.player_usage (overall/pass/rush/down-split usage shares)
-- Query with filters: ?season=eq.2024&order=usage_overall.desc
-- Exposed via PostgREST as /api/player_usage_leaders

DROP VIEW IF EXISTS api.player_usage_leaders;

CREATE VIEW api.player_usage_leaders AS
SELECT *
FROM marts.player_usage;

COMMENT ON VIEW api.player_usage_leaders IS 'Player usage rates by season: share of team plays (overall, pass, rush, and down-split: first/second/third down, standard/passing downs) per athlete. Backed by marts.player_usage.';
