-- Get player game log with EPA stats per game
-- Joins player_game_epa mart with games for context
--
-- Rekeyed on athlete_id (Phase 3, Tier 1 analytics-unlock plan): p_player_id is
-- matched primarily against marts.player_game_epa.athlete_id (the CFBD athlete
-- id, same key space as core.roster.id). The previous roster-name path is
-- retained as a FALLBACK for any rows that predate athlete_id attribution
-- (athlete_id IS NULL); the current mart always populates athlete_id, so the
-- fallback is a safety net rather than the common path.
--
-- Home/away/opponent/result are computed from the mart's own team column
-- (e.team, i.e. the play's offense) rather than the roster team, so the
-- athlete_id path needs no roster row at all -- player_info is LEFT JOINed and
-- only consulted for the name fallback.

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
    CASE WHEN g.home_team = e.team THEN g.away_team ELSE g.home_team END::text AS opponent,
    CASE WHEN g.home_team = e.team THEN 'home' ELSE 'away' END::text AS home_away,
    CASE
      WHEN g.home_team = e.team AND g.home_points > g.away_points THEN 'W'
      WHEN g.home_team = e.team AND g.home_points < g.away_points THEN 'L'
      WHEN g.away_team = e.team AND g.away_points > g.home_points THEN 'W'
      WHEN g.away_team = e.team AND g.away_points < g.home_points THEN 'L'
      WHEN g.home_points = g.away_points THEN 'T'
      ELSE NULL
    END::text AS result
  FROM marts.player_game_epa e
  JOIN core.games g ON g.id = e.game_id
  LEFT JOIN player_info pi ON true
  WHERE e.season = p_season
    AND (
      e.athlete_id::text = p_player_id
      OR (e.athlete_id IS NULL AND e.player_name = pi.full_name AND e.team = pi.team)
    )
  ORDER BY g.week;
$function$;
