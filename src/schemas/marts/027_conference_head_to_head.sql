-- Conference head-to-head: conference vs conference records by season
-- Grain: Conference1 × Conference2 × Season (alphabetical order to avoid duplicates)
-- Enables "SEC is 47-12 against the Big 12 this year" analysis

DROP MATERIALIZED VIEW IF EXISTS marts.conference_head_to_head CASCADE;

CREATE MATERIALIZED VIEW marts.conference_head_to_head AS
WITH conf_games AS (
    SELECT
        g.season,
        -- Always alphabetical order for consistent pairing
        LEAST(g.home_conference, g.away_conference) AS conference_1,
        GREATEST(g.home_conference, g.away_conference) AS conference_2,
        g.home_conference,
        g.away_conference,
        g.home_points,
        g.away_points,
        g.home_points - g.away_points AS point_diff
    FROM core.games g
    WHERE g.completed = true
      AND g.home_conference IS NOT NULL
      AND g.away_conference IS NOT NULL
      AND g.home_conference != g.away_conference
      AND g.home_points IS NOT NULL
      AND g.away_points IS NOT NULL
)
SELECT
    conference_1,
    conference_2,
    season,
    COUNT(*)::int AS total_games,

    -- Wins for conference_1 (the alphabetically first conference)
    COUNT(*) FILTER (
        WHERE (home_conference = conference_1 AND home_points > away_points)
           OR (away_conference = conference_1 AND away_points > home_points)
    )::int AS conf1_wins,

    -- Wins for conference_2
    COUNT(*) FILTER (
        WHERE (home_conference = conference_2 AND home_points > away_points)
           OR (away_conference = conference_2 AND away_points > home_points)
    )::int AS conf2_wins,

    -- Ties
    COUNT(*) FILTER (WHERE home_points = away_points)::int AS ties,

    -- Win pct for conference_1
    ROUND(
        COUNT(*) FILTER (
            WHERE (home_conference = conference_1 AND home_points > away_points)
               OR (away_conference = conference_1 AND away_points > home_points)
        )::numeric / NULLIF(COUNT(*), 0),
        4
    ) AS conf1_win_pct,

    -- Average point diff (positive = conference_1 advantage)
    ROUND(
        AVG(
            CASE
                WHEN home_conference = conference_1 THEN home_points - away_points
                ELSE away_points - home_points
            END
        )::numeric,
        2
    ) AS avg_point_diff

FROM conf_games
GROUP BY conference_1, conference_2, season;

-- Required for REFRESH CONCURRENTLY
CREATE UNIQUE INDEX ON marts.conference_head_to_head (conference_1, conference_2, season);

-- Query indexes
CREATE INDEX ON marts.conference_head_to_head (season);
CREATE INDEX ON marts.conference_head_to_head (conference_1, season);
CREATE INDEX ON marts.conference_head_to_head (conference_2, season);
