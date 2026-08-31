-- api.coaching_history
-- Coaching history with tenure performance, recruiting impact, and postseason record.
-- Filter by: team, coach_name, last_name, is_active
-- Example: /api/coaching_history?team=eq.Alabama&order=tenure_start.desc

CREATE OR REPLACE VIEW api.coaching_history AS
SELECT
    coach_id,
    coach_name,
    first_name,
    last_name,
    team,
    tenure_start,
    tenure_end,
    seasons_count,
    total_games,
    total_wins,
    total_losses,
    total_ties,
    win_pct,
    conf_wins,
    conf_losses,
    conf_win_pct,
    best_season_wins,
    worst_season_wins,
    avg_sp_rating,
    peak_sp_rating,
    best_preseason_rank,
    best_postseason_rank,
    avg_recruiting_rank,
    best_recruiting_rank,
    inherited_talent_rank,
    year3_talent_rank,
    talent_improvement,
    bowl_games,
    bowl_wins,
    is_active
FROM marts.coaching_tenure;

COMMENT ON VIEW api.coaching_history IS
'Coaching history with tenure performance summaries. One row per coach-team-tenure. '
'Filter by team, coach_name, last_name, is_active. '
'talent_improvement = inherited_rank - year3_rank (positive = improved recruiting). '
'coach_id (added 2026-08-30) is ref.coach_seasons'' coach__id, matched by (first_name, '
'last_name, team, year) -- the single id every MATCHED season of the tenure agrees on. '
'NULL when a season''s match is ambiguous (more than one distinct coach__id), when matched '
'seasons disagree, or when zero seasons matched; seasons outside ref.coach_seasons'' '
'coverage (it has no pre-2014 depth) do NOT invalidate the match. Use it instead of '
'first_name+last_name to join to api.coach_records or api.coach_tenures.';

-- Grants are part of the definition: an apply that DROPs/recreates the
-- view would otherwise leave the PostgREST roles without read access
-- (no ALTER DEFAULT PRIVILEGES for them in this database).
GRANT SELECT ON api.coaching_history TO anon, authenticated;
