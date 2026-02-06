-- api.coaching_history
-- Coaching history with tenure performance, recruiting impact, and postseason record.
-- Filter by: team, coach_name, last_name, is_active
-- Example: /api/coaching_history?team=eq.Alabama&order=tenure_start.desc

CREATE OR REPLACE VIEW api.coaching_history AS
SELECT
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
'talent_improvement = inherited_rank - year3_rank (positive = improved recruiting).';
