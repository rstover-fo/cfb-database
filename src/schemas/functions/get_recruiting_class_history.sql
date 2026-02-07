-- Get recruiting class history for a team
-- Returns year-by-year recruiting class with star breakdowns

CREATE OR REPLACE FUNCTION public.get_recruiting_class_history(p_team text)
RETURNS TABLE(
    year integer,
    rank integer,
    points numeric,
    five_stars integer,
    four_stars integer,
    three_stars integer,
    two_stars integer,
    total_commits integer
)
LANGUAGE sql
STABLE
SET search_path = ''
AS $function$
  SELECT
    tr.year::int,
    tr.rank::int,
    tr.points::numeric,
    COALESCE(SUM(CASE WHEN rl.stars = 5 THEN 1 ELSE 0 END), 0)::int AS five_stars,
    COALESCE(SUM(CASE WHEN rl.stars = 4 THEN 1 ELSE 0 END), 0)::int AS four_stars,
    COALESCE(SUM(CASE WHEN rl.stars = 3 THEN 1 ELSE 0 END), 0)::int AS three_stars,
    COALESCE(SUM(CASE WHEN rl.stars = 2 THEN 1 ELSE 0 END), 0)::int AS two_stars,
    COUNT(rl.id)::int AS total_commits
  FROM recruiting.team_recruiting tr
  LEFT JOIN api.recruit_lookup rl
    ON rl.committed_to = tr.team AND rl.year = tr.year
  WHERE tr.team = p_team
  GROUP BY tr.year, tr.rank, tr.points
  ORDER BY tr.year;
$function$;
