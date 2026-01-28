-- Matchup API view
-- Head-to-head comparison between two teams
-- Query with: SELECT * FROM api.matchup WHERE team1 = 'Alabama' AND team2 = 'Georgia'
-- Exposed via PostgREST as /api/matchup

DROP VIEW IF EXISTS api.matchup;

CREATE VIEW api.matchup AS
WITH h2h_games AS (
    -- All games between any two teams
    SELECT
        LEAST(home_team, away_team) AS team1,
        GREATEST(home_team, away_team) AS team2,
        season,
        week,
        start_date,
        home_team,
        away_team,
        home_points,
        away_points,
        venue,
        CASE
            WHEN home_points > away_points THEN home_team
            WHEN away_points > home_points THEN away_team
            ELSE NULL
        END AS winner
    FROM core.games
    WHERE completed = true
),
h2h_summary AS (
    SELECT
        team1,
        team2,
        COUNT(*) AS total_games,
        SUM(CASE WHEN winner = team1 THEN 1 ELSE 0 END) AS team1_wins,
        SUM(CASE WHEN winner = team2 THEN 1 ELSE 0 END) AS team2_wins,
        SUM(CASE WHEN winner IS NULL THEN 1 ELSE 0 END) AS ties,
        MIN(season) AS first_meeting,
        MAX(season) AS last_meeting
    FROM h2h_games
    GROUP BY team1, team2
),
recent_games AS (
    SELECT
        team1,
        team2,
        ARRAY_AGG(
            jsonb_build_object(
                'season', season,
                'winner', winner,
                'home_team', home_team,
                'home_points', home_points,
                'away_points', away_points
            ) ORDER BY season DESC
        ) FILTER (WHERE season >= 2014) AS recent_results
    FROM h2h_games
    GROUP BY team1, team2
)
SELECT
    h.team1,
    h.team2,

    -- Head-to-head record
    h.total_games,
    h.team1_wins,
    h.team2_wins,
    h.ties,
    h.first_meeting,
    h.last_meeting,

    -- Recent results (last 10 years)
    r.recent_results,

    -- Team 1 current season
    t1.season AS team1_season,
    t1.wins AS team1_wins_season,
    t1.losses AS team1_losses_season,
    t1.sp_rank AS team1_sp_rank,
    e1.epa_per_play AS team1_epa,
    e1.epa_tier AS team1_epa_tier,

    -- Team 2 current season
    t2.season AS team2_season,
    t2.wins AS team2_wins_season,
    t2.losses AS team2_losses_season,
    t2.sp_rank AS team2_sp_rank,
    e2.epa_per_play AS team2_epa,
    e2.epa_tier AS team2_epa_tier

FROM h2h_summary h
LEFT JOIN recent_games r ON r.team1 = h.team1 AND r.team2 = h.team2
LEFT JOIN LATERAL (
    SELECT * FROM marts.team_season_summary
    WHERE team = h.team1
    ORDER BY season DESC LIMIT 1
) t1 ON true
LEFT JOIN LATERAL (
    SELECT * FROM marts.team_season_summary
    WHERE team = h.team2
    ORDER BY season DESC LIMIT 1
) t2 ON true
LEFT JOIN LATERAL (
    SELECT * FROM marts.team_epa_season
    WHERE team = h.team1
    ORDER BY season DESC LIMIT 1
) e1 ON true
LEFT JOIN LATERAL (
    SELECT * FROM marts.team_epa_season
    WHERE team = h.team2
    ORDER BY season DESC LIMIT 1
) e2 ON true;

COMMENT ON VIEW api.matchup IS 'Head-to-head matchup history and current season comparison';
