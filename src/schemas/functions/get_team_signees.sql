-- Get team signees for a recruiting class year
-- Returns individual recruit details ordered by ranking

CREATE OR REPLACE FUNCTION public.get_team_signees(p_team text, p_year integer)
RETURNS TABLE(
    ranking integer,
    name text,
    "position" text,
    stars integer,
    rating numeric,
    city text,
    state_province text
)
LANGUAGE sql
STABLE
SET search_path = ''
AS $function$
  SELECT
    rl.ranking::int,
    rl.name::text,
    rl.position::text,
    rl.stars::int,
    rl.rating::numeric,
    rl.city::text,
    rl.state_province::text
  FROM api.recruit_lookup rl
  WHERE rl.committed_to = p_team AND rl.year = p_year
  ORDER BY rl.ranking NULLS LAST;
$function$;
