-- Player search RPC function
-- Fuzzy name search using pg_trgm with optional position/team/season filters
-- Called via supabase.rpc('get_player_search', {...})
-- Extracted from deployed Supabase database on 2026-02-06

CREATE OR REPLACE FUNCTION public.get_player_search(p_query text, p_position text DEFAULT NULL::text, p_team text DEFAULT NULL::text, p_season integer DEFAULT NULL::integer, p_limit integer DEFAULT 25)
 RETURNS TABLE(player_id text, name text, team text, "position" text, season bigint, height bigint, weight bigint, jersey bigint, stars bigint, recruit_rating double precision, similarity_score real)
 LANGUAGE plpgsql
 STABLE
SET search_path = ''
AS $function$
BEGIN
    RETURN QUERY
    SELECT
        r.id::text AS player_id,
        (r.first_name || ' ' || r.last_name)::text AS "name",
        r.team::text,
        r.position::text AS "position",
        r.year AS season,
        r.height,
        r.weight,
        r.jersey,
        rec.stars,
        rec.rating AS recruit_rating,
        public.similarity(lower(r.first_name || ' ' || r.last_name), lower(p_query)) AS similarity_score
    FROM core.roster r
    LEFT JOIN LATERAL (
        SELECT rr.stars, rr.rating
        FROM recruiting.recruits rr
        WHERE rr.athlete_id = r.id
        ORDER BY rr.rating DESC NULLS LAST
        LIMIT 1
    ) rec ON true
    WHERE lower(r.first_name || ' ' || r.last_name) OPERATOR(public.%) lower(p_query)
      AND (p_position IS NULL OR r.position = p_position)
      AND (p_team IS NULL OR r.team = p_team)
      AND (p_season IS NULL OR r.year = p_season)
    ORDER BY similarity_score DESC
    LIMIT p_limit;
END;
$function$;
