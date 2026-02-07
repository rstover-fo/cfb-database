-- Team split analysis functions
-- RPC functions called by the app for team detail pages.
-- Created ad-hoc in Supabase; now tracked in version control.

CREATE OR REPLACE FUNCTION public.get_home_away_splits(p_team TEXT, p_season INT)
RETURNS TABLE(
    location TEXT,
    games BIGINT,
    wins BIGINT,
    win_pct NUMERIC,
    points_per_game NUMERIC,
    points_allowed_per_game NUMERIC,
    epa_per_play NUMERIC,
    success_rate NUMERIC,
    yards_per_play NUMERIC
)
LANGUAGE plpgsql
SET search_path = ''
AS $function$
BEGIN
    RETURN QUERY
    WITH game_results AS (
        SELECT
            CASE WHEN g.home_team = p_team THEN 'home' ELSE 'away' END AS location,
            g.id AS game_id,
            CASE WHEN g.home_team = p_team THEN g.home_points ELSE g.away_points END AS points_for,
            CASE WHEN g.home_team = p_team THEN g.away_points ELSE g.home_points END AS points_against,
            CASE
                WHEN (g.home_team = p_team AND g.home_points > g.away_points) OR
                     (g.away_team = p_team AND g.away_points > g.home_points)
                THEN 1 ELSE 0
            END AS won
        FROM core.games g
        WHERE g.season = p_season
          AND (g.home_team = p_team OR g.away_team = p_team)
          AND g.home_points IS NOT NULL
    ),
    play_stats AS (
        SELECT
            CASE WHEN g.home_team = p_team THEN 'home' ELSE 'away' END AS location,
            p.ppa,
            CASE WHEN p.ppa > 0 THEN 1 ELSE 0 END AS successful,
            p.yards_gained
        FROM core.plays p
        JOIN core.games g ON p.game_id = g.id
        WHERE g.season = p_season
          AND p.offense = p_team
    )
    SELECT
        gr.location::TEXT,
        COUNT(DISTINCT gr.game_id)::BIGINT AS games,
        SUM(gr.won)::BIGINT AS wins,
        ROUND(SUM(gr.won)::NUMERIC / NULLIF(COUNT(DISTINCT gr.game_id), 0), 3) AS win_pct,
        ROUND(AVG(gr.points_for)::NUMERIC, 1) AS points_per_game,
        ROUND(AVG(gr.points_against)::NUMERIC, 1) AS points_allowed_per_game,
        ROUND((SELECT AVG(ps.ppa) FROM play_stats ps WHERE ps.location = gr.location)::NUMERIC, 3) AS epa_per_play,
        ROUND((SELECT AVG(ps.successful) FROM play_stats ps WHERE ps.location = gr.location)::NUMERIC, 3) AS success_rate,
        ROUND((SELECT AVG(ps.yards_gained) FROM play_stats ps WHERE ps.location = gr.location)::NUMERIC, 1) AS yards_per_play
    FROM game_results gr
    GROUP BY gr.location
    ORDER BY gr.location DESC;
END;
$function$;

CREATE OR REPLACE FUNCTION public.get_conference_splits(p_team TEXT, p_season INT)
RETURNS TABLE(
    opponent_type TEXT,
    games BIGINT,
    wins BIGINT,
    win_pct NUMERIC,
    points_per_game NUMERIC,
    points_allowed_per_game NUMERIC,
    epa_per_play NUMERIC,
    success_rate NUMERIC,
    margin_per_game NUMERIC
)
LANGUAGE plpgsql
SET search_path = ''
AS $function$
DECLARE
    v_team_conference TEXT;
BEGIN
    SELECT conference INTO v_team_conference
    FROM public.teams
    WHERE school = p_team;

    RETURN QUERY
    WITH game_results AS (
        SELECT
            CASE
                WHEN opp.conference = v_team_conference THEN 'conference'
                ELSE 'non_conference'
            END AS opponent_type,
            g.id AS game_id,
            CASE WHEN g.home_team = p_team THEN g.home_points ELSE g.away_points END AS points_for,
            CASE WHEN g.home_team = p_team THEN g.away_points ELSE g.home_points END AS points_against,
            CASE
                WHEN (g.home_team = p_team AND g.home_points > g.away_points) OR
                     (g.away_team = p_team AND g.away_points > g.home_points)
                THEN 1 ELSE 0
            END AS won
        FROM core.games g
        JOIN public.teams opp ON opp.school = CASE WHEN g.home_team = p_team THEN g.away_team ELSE g.home_team END
        WHERE g.season = p_season
          AND (g.home_team = p_team OR g.away_team = p_team)
          AND g.home_points IS NOT NULL
    ),
    play_stats AS (
        SELECT
            CASE
                WHEN opp.conference = v_team_conference THEN 'conference'
                ELSE 'non_conference'
            END AS opponent_type,
            p.ppa,
            CASE WHEN p.ppa > 0 THEN 1 ELSE 0 END AS successful
        FROM core.plays p
        JOIN core.games g ON p.game_id = g.id
        JOIN public.teams opp ON opp.school = CASE WHEN g.home_team = p_team THEN g.away_team ELSE g.home_team END
        WHERE g.season = p_season
          AND p.offense = p_team
    )
    SELECT
        gr.opponent_type::TEXT,
        COUNT(DISTINCT gr.game_id)::BIGINT AS games,
        SUM(gr.won)::BIGINT AS wins,
        ROUND(SUM(gr.won)::NUMERIC / NULLIF(COUNT(DISTINCT gr.game_id), 0), 3) AS win_pct,
        ROUND(AVG(gr.points_for)::NUMERIC, 1) AS points_per_game,
        ROUND(AVG(gr.points_against)::NUMERIC, 1) AS points_allowed_per_game,
        ROUND((SELECT AVG(ps.ppa) FROM play_stats ps WHERE ps.opponent_type = gr.opponent_type)::NUMERIC, 3) AS epa_per_play,
        ROUND((SELECT AVG(ps.successful) FROM play_stats ps WHERE ps.opponent_type = gr.opponent_type)::NUMERIC, 3) AS success_rate,
        ROUND(AVG(gr.points_for - gr.points_against)::NUMERIC, 1) AS margin_per_game
    FROM game_results gr
    GROUP BY gr.opponent_type
    ORDER BY gr.opponent_type;
END;
$function$;
