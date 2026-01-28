-- Team season summary: record, scoring, ratings, recruiting
-- Enhanced version with ratings and recruiting data joined
-- Uses team name as primary join key (dlt schema pattern)

CREATE SCHEMA IF NOT EXISTS marts;

DROP MATERIALIZED VIEW IF EXISTS marts.team_season_summary CASCADE;

CREATE MATERIALIZED VIEW marts.team_season_summary AS
WITH home_games AS (
    SELECT
        season,
        home_team AS team,
        home_conference AS conference,
        home_points AS points_for,
        away_points AS points_against,
        CASE WHEN home_points > away_points THEN 1 ELSE 0 END AS win,
        CASE WHEN home_points < away_points THEN 1 ELSE 0 END AS loss,
        CASE
            WHEN home_conference IS NOT NULL
                AND home_conference = away_conference THEN 1
            ELSE 0
        END AS is_conference_game,
        CASE
            WHEN home_conference IS NOT NULL
                AND home_conference = away_conference
                AND home_points > away_points THEN 1
            ELSE 0
        END AS conf_win,
        CASE
            WHEN home_conference IS NOT NULL
                AND home_conference = away_conference
                AND home_points < away_points THEN 1
            ELSE 0
        END AS conf_loss
    FROM core.games
    WHERE completed = true
),
away_games AS (
    SELECT
        season,
        away_team AS team,
        away_conference AS conference,
        away_points AS points_for,
        home_points AS points_against,
        CASE WHEN away_points > home_points THEN 1 ELSE 0 END AS win,
        CASE WHEN away_points < home_points THEN 1 ELSE 0 END AS loss,
        CASE
            WHEN away_conference IS NOT NULL
                AND home_conference = away_conference THEN 1
            ELSE 0
        END AS is_conference_game,
        CASE
            WHEN away_conference IS NOT NULL
                AND home_conference = away_conference
                AND away_points > home_points THEN 1
            ELSE 0
        END AS conf_win,
        CASE
            WHEN away_conference IS NOT NULL
                AND home_conference = away_conference
                AND away_points < home_points THEN 1
            ELSE 0
        END AS conf_loss
    FROM core.games
    WHERE completed = true
),
all_games AS (
    SELECT * FROM home_games
    UNION ALL
    SELECT * FROM away_games
),
team_records AS (
    SELECT
        season,
        team,
        MAX(conference) AS conference,
        COUNT(*) AS games,
        SUM(win) AS wins,
        SUM(loss) AS losses,
        SUM(conf_win) AS conf_wins,
        SUM(conf_loss) AS conf_losses,
        ROUND(AVG(points_for)::numeric, 1) AS ppg,
        ROUND(AVG(points_against)::numeric, 1) AS opp_ppg,
        ROUND(AVG(points_for - points_against)::numeric, 1) AS avg_margin
    FROM all_games
    GROUP BY season, team
)
SELECT
    tr.team,
    tr.conference,
    tr.season,

    -- Record
    tr.games::int,
    tr.wins::int,
    tr.losses::int,
    tr.conf_wins::int,
    tr.conf_losses::int,

    -- Scoring
    tr.ppg,
    tr.opp_ppg,
    tr.avg_margin,

    -- Ratings (joined from ratings schema)
    sp.rating AS sp_rating,
    sp.ranking AS sp_rank,
    sp."offense__rating" AS sp_offense,
    sp."defense__rating" AS sp_defense,
    elo.elo,
    fpi.fpi,

    -- Recruiting
    rec.rank AS recruiting_rank,
    rec.points AS recruiting_points

FROM team_records tr
LEFT JOIN ratings.sp_ratings sp ON tr.team = sp.team AND tr.season = sp.year
LEFT JOIN ratings.elo_ratings elo ON tr.team = elo.team AND tr.season = elo.year
LEFT JOIN ratings.fpi_ratings fpi ON tr.team = fpi.team AND tr.season = fpi.year
LEFT JOIN recruiting.team_recruiting rec ON tr.team = rec.team AND tr.season = rec.year;

-- Required for REFRESH CONCURRENTLY
CREATE UNIQUE INDEX ON marts.team_season_summary (team, season);

-- Query indexes
CREATE INDEX ON marts.team_season_summary (season);
CREATE INDEX ON marts.team_season_summary (conference);
CREATE INDEX ON marts.team_season_summary (sp_rank);
CREATE INDEX ON marts.team_season_summary (wins DESC);
