-- Get player stats with percentile rankings
-- Pulls from marts.player_comparison which has pre-computed PERCENT_RANK values

CREATE OR REPLACE FUNCTION public.get_player_percentiles(p_player_id text, p_season integer)
RETURNS TABLE(
    player_id character varying,
    name character varying,
    team character varying,
    "position" character varying,
    position_group text,
    season integer,
    pass_yds numeric,
    pass_td numeric,
    pass_pct numeric,
    rush_yds numeric,
    rush_td numeric,
    rush_ypc numeric,
    rec_yds numeric,
    rec_td numeric,
    tackles numeric,
    sacks numeric,
    tfl numeric,
    ppa_avg double precision,
    pass_yds_pctl double precision,
    pass_td_pctl double precision,
    pass_pct_pctl double precision,
    rush_yds_pctl double precision,
    rush_td_pctl double precision,
    rush_ypc_pctl double precision,
    rec_yds_pctl double precision,
    rec_td_pctl double precision,
    tackles_pctl double precision,
    sacks_pctl double precision,
    tfl_pctl double precision,
    ppa_avg_pctl double precision
)
LANGUAGE sql
STABLE
SET search_path = ''
AS $function$
  SELECT
    pc.player_id::varchar,
    pc.name::varchar,
    pc.team::varchar,
    pc.position::varchar,
    pc.position_group::text,
    pc.season::integer,
    pc.pass_yds::numeric,
    pc.pass_td::numeric,
    pc.pass_pct::numeric,
    pc.rush_yds::numeric,
    pc.rush_td::numeric,
    pc.rush_ypc::numeric,
    pc.rec_yds::numeric,
    pc.rec_td::numeric,
    pc.tackles::numeric,
    pc.sacks::numeric,
    pc.tfl::numeric,
    pc.ppa_avg::float,
    pc.pass_yds_pctl::float,
    pc.pass_td_pctl::float,
    pc.pass_pct_pctl::float,
    pc.rush_yds_pctl::float,
    pc.rush_td_pctl::float,
    pc.rush_ypc_pctl::float,
    pc.rec_yds_pctl::float,
    pc.rec_td_pctl::float,
    pc.tackles_pctl::float,
    pc.sacks_pctl::float,
    pc.tfl_pctl::float,
    pc.ppa_avg_pctl::float
  FROM marts.player_comparison pc
  WHERE pc.player_id = p_player_id
    AND pc.season = p_season
  LIMIT 1;
$function$;
