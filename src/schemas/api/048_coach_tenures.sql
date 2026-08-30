-- api.coach_tenures
-- CFBD's own continuous coach-tenure record (2026-08-30 expansion_views
-- unit, task 5b). Grain: one row per (coach_id, team_id, tenure_start).
-- Thin passthrough of marts.coach_tenures.
--
-- Distinct from api.coaching_history (marts.coaching_tenure, gap-detected
-- from ref.coaches__seasons): this view is CFBD's own pre-computed tenure
-- record and is the one that carries is_interim and classification.
-- tenure_end IS NULL for an active tenure. May be empty until the
-- coach_tenures backfill (`--source coach_tenures`) has run for a given
-- environment -- see marts.coach_tenures' header.
--
-- PostgREST usage:
--   GET /api/coach_tenures?team_id=eq.333&order=tenure_start.desc
--   GET /api/coach_tenures?is_interim=eq.false&order=record_win_percentage.desc

DROP VIEW IF EXISTS api.coach_tenures;

CREATE VIEW api.coach_tenures AS
SELECT *
FROM marts.coach_tenures;

COMMENT ON VIEW api.coach_tenures IS 'CFBD''s own continuous coach-tenure record. One row per (coach_id, team_id, tenure_start). Columns: coach_id, coach_name, team_id, team, tenure_start, tenure_end (NULL = active), hire_date, is_interim, record_games, record_wins, record_losses, record_ties, record_win_percentage, classification. is_interim and classification retire cfb-app heuristics that previously stood in for them (a min-games floor and a hardcoded FBS team-name list respectively). May be empty until the coach_tenures backfill has run for this environment. Backed by marts.coach_tenures.';

GRANT SELECT ON api.coach_tenures TO anon, authenticated;
