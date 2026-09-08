-- Scoped consumer restoration for the trajectory mart release.
CREATE OR REPLACE VIEW public.team_season_trajectory
WITH (security_invoker = true) AS
SELECT team, season, epa_per_play, success_rate, off_epa_rank, def_epa_rank,
       win_pct, wins, games, recruiting_rank, era_code, era_name, prev_epa, epa_delta
FROM marts.team_season_trajectory;
GRANT SELECT ON public.team_season_trajectory TO anon, authenticated;
