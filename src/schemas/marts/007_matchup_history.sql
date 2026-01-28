-- Matchup history: head-to-head records between teams
-- Grain: Team1 × Team2 (alphabetically ordered for uniqueness)
-- Includes: all-time record, recent form, avg margin

DROP MATERIALIZED VIEW IF EXISTS marts.matchup_history CASCADE;

CREATE MATERIALIZED VIEW marts.matchup_history AS
WITH game_matchups AS (
    SELECT
        LEAST(home_team, away_team) AS team1,
        GREATEST(home_team, away_team) AS team2,
        season,
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
        END AS winner,
        ABS(home_points - away_points) AS margin
    FROM core.games
    WHERE completed = true
      AND home_points IS NOT NULL
      AND away_points IS NOT NULL
)
SELECT
    team1,
    team2,

    -- All-time record
    COUNT(*) AS total_games,
    SUM(CASE WHEN winner = team1 THEN 1 ELSE 0 END)::int AS team1_wins,
    SUM(CASE WHEN winner = team2 THEN 1 ELSE 0 END)::int AS team2_wins,
    SUM(CASE WHEN winner IS NULL THEN 1 ELSE 0 END)::int AS ties,

    -- Series dates
    MIN(season) AS first_meeting_year,
    MAX(season) AS last_meeting_year,
    MIN(start_date) AS first_meeting_date,
    MAX(start_date) AS last_meeting_date,

    -- Margins
    ROUND(AVG(margin)::numeric, 1) AS avg_margin,
    MAX(margin)::int AS largest_margin,

    -- Team1 perspective stats
    ROUND(AVG(
        CASE
            WHEN home_team = team1 THEN home_points - away_points
            ELSE away_points - home_points
        END
    )::numeric, 1) AS team1_avg_margin,

    SUM(
        CASE
            WHEN home_team = team1 THEN home_points
            ELSE away_points
        END
    )::int AS team1_total_points,

    SUM(
        CASE
            WHEN home_team = team2 THEN home_points
            ELSE away_points
        END
    )::int AS team2_total_points,

    -- Recent form (last 10 years)
    SUM(CASE WHEN winner = team1 AND season >= 2015 THEN 1 ELSE 0 END)::int AS team1_wins_last_10yr,
    SUM(CASE WHEN winner = team2 AND season >= 2015 THEN 1 ELSE 0 END)::int AS team2_wins_last_10yr,
    COUNT(*) FILTER (WHERE season >= 2015)::int AS games_last_10yr,

    -- Current streak (simplified: who won last game)
    (
        SELECT winner
        FROM game_matchups gm2
        WHERE gm2.team1 = game_matchups.team1
          AND gm2.team2 = game_matchups.team2
        ORDER BY start_date DESC
        LIMIT 1
    ) AS last_winner

FROM game_matchups
GROUP BY team1, team2;

-- Required for REFRESH CONCURRENTLY
CREATE UNIQUE INDEX ON marts.matchup_history (team1, team2);

-- Query indexes
CREATE INDEX ON marts.matchup_history (team1);
CREATE INDEX ON marts.matchup_history (team2);
CREATE INDEX ON marts.matchup_history (total_games DESC);
