-- Ratings-derived views in public schema
-- Joins SP+ and FPI ratings for special teams and SOS data.
-- Created ad-hoc in Supabase; now tracked in version control.

CREATE OR REPLACE VIEW public.team_special_teams_sos AS
SELECT
    COALESCE(sp.year, fpi.year) AS season,
    COALESCE(sp.team, fpi.team) AS team,
    sp.special_teams__rating AS sp_st_rating,
    fpi.efficiencies__special_teams AS fpi_st_efficiency,
    fpi.resume_ranks__strength_of_schedule AS sos_rank
FROM ratings.sp_ratings sp
FULL JOIN ratings.fpi_ratings fpi
    ON sp.year = fpi.year AND sp.team = fpi.team
WHERE COALESCE(sp.year, fpi.year) >= 2015;
