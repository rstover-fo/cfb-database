-- Get season stat leaders by category (passing, rushing, receiving, defense)
-- Pulls from marts.player_comparison with optional conference filter

CREATE OR REPLACE FUNCTION public.get_player_season_leaders(
    p_season integer,
    p_category text DEFAULT 'passing'::text,
    p_conference text DEFAULT NULL::text,
    p_limit integer DEFAULT 50
)
RETURNS TABLE(
    player_id character varying,
    player_name character varying,
    team character varying,
    conference character varying,
    "position" character varying,
    yards numeric,
    touchdowns numeric,
    interceptions numeric,
    pct numeric,
    attempts numeric,
    completions numeric,
    carries numeric,
    yards_per_carry numeric,
    receptions numeric,
    yards_per_reception numeric,
    longest numeric,
    total_tackles numeric,
    solo_tackles numeric,
    sacks numeric,
    tackles_for_loss numeric,
    passes_defended numeric,
    yards_rank bigint
)
LANGUAGE sql
STABLE
SET search_path = ''
AS $function$
  SELECT
    pc.player_id::varchar,
    pc.name::varchar AS player_name,
    pc.team::varchar,
    t.conference::varchar,
    pc.position::varchar,
    -- yards: depends on category
    CASE p_category
      WHEN 'passing'   THEN pc.pass_yds::numeric
      WHEN 'rushing'   THEN pc.rush_yds::numeric
      WHEN 'receiving' THEN pc.rec_yds::numeric
      WHEN 'defense'   THEN NULL
    END AS yards,
    -- touchdowns
    CASE p_category
      WHEN 'passing'   THEN pc.pass_td::numeric
      WHEN 'rushing'   THEN pc.rush_td::numeric
      WHEN 'receiving' THEN pc.rec_td::numeric
      WHEN 'defense'   THEN NULL
    END AS touchdowns,
    -- interceptions (passing INT or defensive INT)
    CASE p_category
      WHEN 'passing'  THEN pc.pass_int::numeric
      WHEN 'defense'  THEN NULL -- no def_int in matview, use pass_def
      ELSE NULL
    END AS interceptions,
    -- pct (passing completion %)
    CASE WHEN p_category = 'passing' THEN pc.pass_pct::numeric ELSE NULL END AS pct,
    -- attempts
    CASE WHEN p_category = 'passing' THEN pc.pass_att::numeric ELSE NULL END AS attempts,
    -- completions
    CASE WHEN p_category = 'passing' THEN pc.pass_cmp::numeric ELSE NULL END AS completions,
    -- carries
    CASE WHEN p_category = 'rushing' THEN pc.rush_car::numeric ELSE NULL END AS carries,
    -- yards_per_carry
    CASE WHEN p_category = 'rushing' THEN pc.rush_ypc::numeric ELSE NULL END AS yards_per_carry,
    -- receptions
    CASE WHEN p_category = 'receiving' THEN pc.rec::numeric ELSE NULL END AS receptions,
    -- yards_per_reception
    CASE WHEN p_category = 'receiving' THEN pc.rec_ypr::numeric ELSE NULL END AS yards_per_reception,
    -- longest (not in matview)
    NULL::numeric AS longest,
    -- defense columns
    CASE WHEN p_category = 'defense' THEN pc.tackles::numeric ELSE NULL END AS total_tackles,
    NULL::numeric AS solo_tackles,
    CASE WHEN p_category = 'defense' THEN pc.sacks::numeric ELSE NULL END AS sacks,
    CASE WHEN p_category = 'defense' THEN pc.tfl::numeric ELSE NULL END AS tackles_for_loss,
    CASE WHEN p_category = 'defense' THEN pc.pass_def::numeric ELSE NULL END AS passes_defended,
    -- rank by primary stat
    RANK() OVER (
      ORDER BY
        CASE p_category
          WHEN 'passing'   THEN pc.pass_yds::numeric
          WHEN 'rushing'   THEN pc.rush_yds::numeric
          WHEN 'receiving' THEN pc.rec_yds::numeric
          WHEN 'defense'   THEN pc.tackles::numeric
        END DESC NULLS LAST
    ) AS yards_rank
  FROM marts.player_comparison pc
  LEFT JOIN public.teams_with_logos t ON t.school = pc.team
  WHERE pc.season = p_season
    AND (p_conference IS NULL OR t.conference = p_conference)
    -- Filter to relevant positions per category
    AND CASE p_category
      WHEN 'passing'   THEN pc.pass_att::numeric > 0
      WHEN 'rushing'   THEN pc.rush_car::numeric > 0
      WHEN 'receiving' THEN pc.rec::numeric > 0
      WHEN 'defense'   THEN pc.tackles::numeric > 0
      ELSE true
    END
  ORDER BY
    CASE p_category
      WHEN 'passing'   THEN pc.pass_yds::numeric
      WHEN 'rushing'   THEN pc.rush_yds::numeric
      WHEN 'receiving' THEN pc.rec_yds::numeric
      WHEN 'defense'   THEN pc.tackles::numeric
    END DESC NULLS LAST
  LIMIT p_limit;
$function$;
