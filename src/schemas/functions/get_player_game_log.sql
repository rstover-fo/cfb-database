-- Get player game log with EPA stats per game
-- Joins player_game_epa mart with roster and games for context

CREATE OR REPLACE FUNCTION public.get_player_game_log(p_player_id text, p_season integer)
RETURNS TABLE(
    game_id bigint,
    season integer,
    team text,
    player_name text,
    play_category text,
    plays bigint,
    total_epa numeric,
    epa_per_play numeric,
    success_rate numeric,
    explosive_plays bigint,
    total_yards numeric,
    week integer,
    opponent text,
    home_away text,
    result text
)
LANGUAGE sql
STABLE
SET search_path = ''
AS $function$
  WITH player_info AS (
    SELECT
      r.first_name || ' ' || r.last_name AS full_name,
      r.team
    FROM core.roster r
    WHERE r.id = p_player_id
      AND r.year = p_season
    LIMIT 1
  )
  SELECT
    e.game_id::bigint,
    e.season::integer,
    e.team::text,
    e.player_name::text,
    e.play_category::text,
    e.plays::bigint,
    e.total_epa::numeric,
    e.epa_per_play::numeric,
    e.success_rate::numeric,
    e.explosive_plays::bigint,
    e.total_yards::numeric,
    g.week::integer,
    CASE WHEN g.home_team = pi.team THEN g.away_team ELSE g.home_team END::text AS opponent,
    CASE WHEN g.home_team = pi.team THEN 'home' ELSE 'away' END::text AS home_away,
    CASE
      WHEN g.home_team = pi.team AND g.home_points > g.away_points THEN 'W'
      WHEN g.home_team = pi.team AND g.home_points < g.away_points THEN 'L'
      WHEN g.away_team = pi.team AND g.away_points > g.home_points THEN 'W'
      WHEN g.away_team = pi.team AND g.away_points < g.home_points THEN 'L'
      WHEN g.home_points = g.away_points THEN 'T'
      ELSE NULL
    END::text AS result
  FROM marts.player_game_epa e
  CROSS JOIN player_info pi
  JOIN core.games g ON g.id = e.game_id
  WHERE e.player_name = pi.full_name
    AND e.team = pi.team
    AND e.season = p_season
  ORDER BY g.week;
$function$;
