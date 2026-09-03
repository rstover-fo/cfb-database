-- Get detailed player information for a single player
-- Pulls from marts.player_comparison for pre-computed stats, plus WEPA/PAAR
-- (marts.player_wepa_season) for opponent-adjusted passing/rushing EPA and kicker PAAR.
--
-- rushing_charting (2026-09-03 rushing-charting unit, U7, R16/KTD6): additive
-- jsonb block for the requested (player, season, team) built from
-- marts.rushing_charting_player_season, joined team-aware on
-- (player_id, season, team) -- the mart's own grain (KTD3) -- so a
-- mid-season transfer's two team stints each get their own rushing block,
-- never a duplicated or cross-team one. NULL whenever the player-season has
-- no rushing charting row (AE5); every pre-existing column is unchanged.
-- Once non-NULL, the object always carries all four direction keys (left,
-- middle, right, unknown) because marts.rushing_charting_direction_season
-- guarantees a fixed four-row melt per (entity, side).
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
    paar double precision,
    rushing_charting jsonb
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
    wp_kick.paar::double precision AS paar,
    CASE WHEN rcps.player_id IS NULL THEN NULL ELSE
      jsonb_build_object(
        'attempts', rcps.attempts,
        'individual_attempts', rcps.individual_attempts,
        'unattributed_attempts', rcps.unattributed_attempts,
        'sacks', rcps.sacks,
        'kneels', rcps.kneels,
        'team_rushes', rcps.team_rushes,
        'multi_carrier_attempts', rcps.multi_carrier_attempts,
        'total_rushing_yards', rcps.total_rushing_yards,
        'yards_per_carry', rcps.yards_per_carry,
        'success_rate', rcps.success_rate,
        'ppa', rcps.ppa,
        'total_ppa', rcps.total_ppa,
        'line_yards', rcps.line_yards,
        'line_yards_total', rcps.line_yards_total,
        'second_level_yards', rcps.second_level_yards,
        'second_level_yards_total', rcps.second_level_yards_total,
        'open_field_yards', rcps.open_field_yards,
        'open_field_yards_total', rcps.open_field_yards_total,
        'stuff_rate', rcps.stuff_rate,
        'power_success', rcps.power_success,
        'explosiveness', rcps.explosiveness,
        'rushing_yards_available', rcps.rushing_yards_available,
        'direction_eligible_attempts', rcps.direction_eligible_attempts,
        'direction_available_attempts', rcps.direction_available_attempts,
        'directions', (
          SELECT jsonb_object_agg(d.direction, jsonb_build_object(
            'carries', d.carries,
            'yards', d.yards,
            'yards_per_carry', d.yards_per_carry,
            'success_rate', d.success_rate,
            'ppa', d.ppa,
            'total_ppa', d.total_ppa,
            'line_yards', d.line_yards,
            'line_yards_total', d.line_yards_total,
            'second_level_yards', d.second_level_yards,
            'second_level_yards_total', d.second_level_yards_total,
            'open_field_yards', d.open_field_yards,
            'open_field_yards_total', d.open_field_yards_total,
            'stuff_rate', d.stuff_rate,
            'power_success', d.power_success,
            'explosiveness', d.explosiveness
          ))
          FROM marts.rushing_charting_direction_season d
          WHERE d.entity_type = 'player'
            AND d.entity_id = rcps.player_id
            AND d.season = rcps.season
            AND d.team = rcps.team
            AND d.side = 'offense'
        )
      )
    END AS rushing_charting
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
  LEFT JOIN marts.rushing_charting_player_season rcps
    ON rcps.player_id = pc.player_id::text
    AND rcps.season = pc.season
    AND rcps.team = pc.team
  WHERE pc.player_id = p_player_id
    AND (p_season IS NULL OR pc.season = p_season)
  ORDER BY pc.season DESC
  LIMIT 1;
$function$;

-- PostgREST schema reload: this file DROPs and recreates the function with a
-- changed return type, and Supabase's DDL watcher may not always pick that
-- up on its own (harmless no-op if it already did).
NOTIFY pgrst, 'reload schema';
