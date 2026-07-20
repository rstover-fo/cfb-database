-- Get detailed player information for a single player
-- Pulls from marts.player_comparison for pre-computed stats, plus WEPA/PAAR
-- (marts.player_wepa_season) for opponent-adjusted passing/rushing EPA and kicker PAAR.
--
-- Changing the RETURNS TABLE column list is a return-type change: CREATE OR REPLACE
-- fails against a live function with a different signature, so the function must be
-- dropped first.
DROP FUNCTION IF EXISTS public.get_player_detail(text, integer);

CREATE OR REPLACE FUNCTION public.get_player_detail(p_player_id text, p_season integer DEFAULT NULL::integer)
RETURNS TABLE(
    player_id character varying,
    name text,
    team character varying,
    "position" character varying,
    jersey bigint,
    height bigint,
    weight bigint,
    year bigint,
    home_city character varying,
    home_state character varying,
    season bigint,
    stars bigint,
    recruit_rating double precision,
    national_ranking bigint,
    recruit_class bigint,
    pass_att numeric,
    pass_cmp numeric,
    pass_yds numeric,
    pass_td numeric,
    pass_int numeric,
    pass_pct numeric,
    rush_car numeric,
    rush_yds numeric,
    rush_td numeric,
    rush_ypc numeric,
    rec numeric,
    rec_yds numeric,
    rec_td numeric,
    rec_ypr numeric,
    tackles numeric,
    solo numeric,
    sacks numeric,
    tfl numeric,
    pass_def numeric,
    def_int numeric,
    fg_made numeric,
    fg_att numeric,
    xp_made numeric,
    xp_att numeric,
    punt_yds numeric,
    wepa_passing double precision,
    wepa_rushing double precision,
    paar double precision
)
LANGUAGE sql
STABLE
SET search_path = ''
AS $function$
  SELECT
    pc.player_id::varchar,
    pc.name::text,
    pc.team::varchar,
    pc.position::varchar,
    pc.jersey::bigint,
    pc.height::bigint,
    pc.weight::bigint,
    pc.season::bigint AS year,
    pc.home_city::varchar,
    pc.home_state::varchar,
    pc.season::bigint,
    pc.stars::bigint,
    pc.recruit_rating::double precision,
    pc.national_ranking::bigint,
    pc.recruit_class::bigint,
    pc.pass_att::numeric,
    pc.pass_cmp::numeric,
    pc.pass_yds::numeric,
    pc.pass_td::numeric,
    pc.pass_int::numeric,
    pc.pass_pct::numeric,
    pc.rush_car::numeric,
    pc.rush_yds::numeric,
    pc.rush_td::numeric,
    pc.rush_ypc::numeric,
    pc.rec::numeric,
    pc.rec_yds::numeric,
    pc.rec_td::numeric,
    pc.rec_ypr::numeric,
    pc.tackles::numeric,
    NULL::numeric AS solo,
    pc.sacks::numeric,
    pc.tfl::numeric,
    pc.pass_def::numeric,
    NULL::numeric AS def_int,
    NULL::numeric AS fg_made,
    NULL::numeric AS fg_att,
    NULL::numeric AS xp_made,
    NULL::numeric AS xp_att,
    NULL::numeric AS punt_yds,
    wp_pass.wepa::double precision AS wepa_passing,
    wp_rush.wepa::double precision AS wepa_rushing,
    wp_kick.paar::double precision AS paar
  FROM marts.player_comparison pc
  LEFT JOIN marts.player_wepa_season wp_pass
    ON wp_pass.athlete_id::text = pc.player_id::text
    AND wp_pass.season = pc.season
    AND wp_pass.category = 'passing'
  LEFT JOIN marts.player_wepa_season wp_rush
    ON wp_rush.athlete_id::text = pc.player_id::text
    AND wp_rush.season = pc.season
    AND wp_rush.category = 'rushing'
  LEFT JOIN marts.player_wepa_season wp_kick
    ON wp_kick.athlete_id::text = pc.player_id::text
    AND wp_kick.season = pc.season
    AND wp_kick.category = 'kicking'
  WHERE pc.player_id = p_player_id
    AND (p_season IS NULL OR pc.season = p_season)
  ORDER BY pc.season DESC
  LIMIT 1;
$function$;
