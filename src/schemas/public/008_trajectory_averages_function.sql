-- Trajectory averages function
-- Returns conference and FBS average metrics per season for comparison charts.
-- Created ad-hoc in Supabase; now tracked in version control.

CREATE OR REPLACE FUNCTION public.get_trajectory_averages(
    p_conference TEXT,
    p_season_start INT DEFAULT 2020,
    p_season_end INT DEFAULT 2025
)
RETURNS TABLE(
    season BIGINT,
    conf_wins NUMERIC,
    conf_win_pct NUMERIC,
    conf_epa_per_play NUMERIC,
    conf_success_rate NUMERIC,
    conf_off_epa_rank NUMERIC,
    conf_def_epa_rank NUMERIC,
    conf_recruiting_rank NUMERIC,
    fbs_wins NUMERIC,
    fbs_win_pct NUMERIC,
    fbs_epa_per_play NUMERIC,
    fbs_success_rate NUMERIC,
    fbs_off_epa_rank NUMERIC,
    fbs_def_epa_rank NUMERIC,
    fbs_recruiting_rank NUMERIC
)
LANGUAGE plpgsql
SET search_path = ''
AS $function$
BEGIN
    RETURN QUERY
    SELECT
        t.season,
        AVG(t.wins) FILTER (WHERE tm.conference = p_conference)::NUMERIC AS conf_wins,
        AVG(t.win_pct) FILTER (WHERE tm.conference = p_conference)::NUMERIC AS conf_win_pct,
        AVG(t.epa_per_play) FILTER (WHERE tm.conference = p_conference)::NUMERIC AS conf_epa_per_play,
        AVG(t.success_rate) FILTER (WHERE tm.conference = p_conference)::NUMERIC AS conf_success_rate,
        AVG(t.off_epa_rank) FILTER (WHERE tm.conference = p_conference)::NUMERIC AS conf_off_epa_rank,
        AVG(t.def_epa_rank) FILTER (WHERE tm.conference = p_conference)::NUMERIC AS conf_def_epa_rank,
        AVG(t.recruiting_rank) FILTER (WHERE tm.conference = p_conference)::NUMERIC AS conf_recruiting_rank,
        AVG(t.wins)::NUMERIC AS fbs_wins,
        AVG(t.win_pct)::NUMERIC AS fbs_win_pct,
        AVG(t.epa_per_play)::NUMERIC AS fbs_epa_per_play,
        AVG(t.success_rate)::NUMERIC AS fbs_success_rate,
        AVG(t.off_epa_rank)::NUMERIC AS fbs_off_epa_rank,
        AVG(t.def_epa_rank)::NUMERIC AS fbs_def_epa_rank,
        AVG(t.recruiting_rank)::NUMERIC AS fbs_recruiting_rank
    FROM public.team_season_trajectory t
    JOIN public.teams tm ON t.team = tm.school
    WHERE tm.classification = 'fbs'
      AND t.season BETWEEN p_season_start AND p_season_end
    GROUP BY t.season
    ORDER BY t.season;
END;
$function$;
