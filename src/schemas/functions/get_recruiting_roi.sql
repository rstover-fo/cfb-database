-- Get recruiting ROI metrics for a team and season
-- Returns blue chip ratio, efficiency, and percentile rankings

CREATE OR REPLACE FUNCTION public.get_recruiting_roi(p_team text, p_season integer)
RETURNS TABLE(
    season integer,
    avg_class_rank_4yr numeric,
    avg_class_points_4yr numeric,
    total_blue_chips_4yr integer,
    blue_chip_ratio numeric,
    wins integer,
    losses integer,
    win_pct numeric,
    sp_rating double precision,
    sp_rank integer,
    epa_per_play numeric,
    wins_over_expected numeric,
    epa_over_expected numeric,
    recruiting_efficiency numeric,
    win_pct_pctl double precision,
    epa_pctl double precision,
    recruiting_efficiency_pctl double precision
)
LANGUAGE sql
STABLE
SET search_path = ''
AS $function$
  SELECT
    r.season::int,
    r.avg_class_rank_4yr,
    r.avg_class_points_4yr::numeric,
    r.total_blue_chips_4yr::int,
    r.blue_chip_ratio,
    r.wins,
    r.losses,
    r.win_pct,
    r.sp_rating,
    r.sp_rank::int,
    r.epa_per_play,
    r.wins_over_expected,
    r.epa_over_expected,
    r.recruiting_efficiency,
    r.win_pct_pctl,
    r.epa_pctl,
    r.recruiting_efficiency_pctl
  FROM api.recruiting_roi r
  WHERE r.team = p_team AND r.season = p_season;
$function$;
