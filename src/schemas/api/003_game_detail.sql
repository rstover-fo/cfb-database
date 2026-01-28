-- Game detail API view
-- Single game page: teams, scores, betting, EPA, venue
-- Exposed via PostgREST as /api/game_detail

DROP VIEW IF EXISTS api.game_detail;

CREATE VIEW api.game_detail AS
WITH consensus_lines AS (
    -- Pick 'consensus' provider first; fall back to first available
    SELECT DISTINCT ON (game_id)
        game_id,
        spread,
        over_under,
        provider
    FROM betting.lines
    ORDER BY game_id, CASE WHEN provider = 'consensus' THEN 0 ELSE 1 END, provider
)
SELECT
    g.id AS game_id,
    g.season,
    g.week,
    g.season_type,
    g.start_date,
    g.start_time_tbd,
    g.completed,
    g.neutral_site,
    g.conference_game,

    -- Home team
    g.home_team,
    g.home_conference,
    g.home_points,
    g.home_pregame_elo,
    home_epa.epa_per_play AS home_epa,
    home_epa.success_rate AS home_success_rate,

    -- Away team
    g.away_team,
    g.away_conference,
    g.away_points,
    g.away_pregame_elo,
    away_epa.epa_per_play AS away_epa,
    away_epa.success_rate AS away_success_rate,

    -- Result
    CASE
        WHEN g.home_points > g.away_points THEN g.home_team
        WHEN g.away_points > g.home_points THEN g.away_team
        ELSE NULL
    END AS winner,
    ABS(g.home_points - g.away_points) AS point_diff,

    -- Betting
    cl.spread AS home_spread,
    cl.over_under,
    cl.provider AS line_provider,

    -- Spread result (negative spread = home favored)
    CASE
        WHEN cl.spread IS NOT NULL AND g.completed THEN
            CASE
                WHEN (g.home_points - g.away_points) > (-1 * cl.spread) THEN 'home_covered'
                WHEN (g.home_points - g.away_points) < (-1 * cl.spread) THEN 'away_covered'
                ELSE 'push'
            END
        ELSE NULL
    END AS spread_result,

    -- Over/under result
    CASE
        WHEN cl.over_under IS NOT NULL AND g.completed THEN
            CASE
                WHEN (g.home_points + g.away_points) > cl.over_under THEN 'over'
                WHEN (g.home_points + g.away_points) < cl.over_under THEN 'under'
                ELSE 'push'
            END
        ELSE NULL
    END AS ou_result,

    -- Win probability
    wp.home_win_probability AS pregame_home_win_prob,

    -- Venue
    g.venue,
    g.venue_id,
    g.attendance,

    -- Excitement
    g.excitement_index

FROM core.games g
LEFT JOIN consensus_lines cl ON cl.game_id = g.id
LEFT JOIN metrics.pregame_win_probability wp ON wp.game_id = g.id
LEFT JOIN marts._game_epa_calc home_epa
    ON home_epa.game_id = g.id AND home_epa.team = g.home_team
LEFT JOIN marts._game_epa_calc away_epa
    ON away_epa.game_id = g.id AND away_epa.team = g.away_team;

COMMENT ON VIEW api.game_detail IS 'Single game detail with teams, betting lines, EPA, and results';
