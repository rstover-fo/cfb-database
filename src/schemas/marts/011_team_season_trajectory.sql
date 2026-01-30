-- Team historical trajectory for year-over-year comparison
-- Combines EPA, win %, recruiting rank, and era classification
-- Depends on: marts.team_epa_season, core.team_records, recruiting.team_recruiting

DROP MATERIALIZED VIEW IF EXISTS marts.team_season_trajectory CASCADE;

CREATE MATERIALIZED VIEW marts.team_season_trajectory AS
WITH era_definitions AS (
    SELECT
        season,
        CASE
            WHEN season BETWEEN 2004 AND 2013 THEN 'BCS'
            WHEN season BETWEEN 2014 AND 2023 THEN 'CFP_4'
            WHEN season >= 2024 THEN 'CFP_12'
        END AS era_code,
        CASE
            WHEN season BETWEEN 2004 AND 2013 THEN 'BCS Era'
            WHEN season BETWEEN 2014 AND 2023 THEN '4-Team Playoff'
            WHEN season >= 2024 THEN '12-Team Playoff'
        END AS era_name
    FROM generate_series(2004, 2026) AS season
),
team_records AS (
    SELECT
        team,
        year AS season,
        total__wins AS wins,
        total__games AS games,
        ROUND(total__wins::numeric / NULLIF(total__games, 0), 3) AS win_pct
    FROM core.records
),
recruiting_ranks AS (
    SELECT
        team,
        year AS season,
        rank AS recruiting_rank
    FROM recruiting.team_recruiting
)
SELECT
    epa.team,
    epa.season,
    -- EPA metrics
    epa.epa_per_play,
    epa.success_rate,
    -- Rankings (would need window function for actual rank, using tier for now)
    RANK() OVER (PARTITION BY epa.season ORDER BY epa.epa_per_play DESC) AS off_epa_rank,
    RANK() OVER (PARTITION BY epa.season ORDER BY epa.epa_per_play ASC) AS def_epa_rank,
    -- Win/loss record
    tr.win_pct,
    tr.wins,
    tr.games,
    -- Recruiting
    rr.recruiting_rank,
    -- Era
    e.era_code,
    e.era_name,
    -- Year-over-year delta
    LAG(epa.epa_per_play) OVER (PARTITION BY epa.team ORDER BY epa.season) AS prev_epa,
    ROUND(epa.epa_per_play - LAG(epa.epa_per_play) OVER (PARTITION BY epa.team ORDER BY epa.season), 4) AS epa_delta
FROM marts.team_epa_season epa
LEFT JOIN team_records tr ON epa.team = tr.team AND epa.season = tr.season
LEFT JOIN recruiting_ranks rr ON epa.team = rr.team AND epa.season = rr.season
JOIN era_definitions e ON epa.season = e.season;

-- Required for REFRESH CONCURRENTLY
CREATE UNIQUE INDEX ON marts.team_season_trajectory (team, season);

-- Query indexes
CREATE INDEX ON marts.team_season_trajectory (season);
CREATE INDEX ON marts.team_season_trajectory (team);
CREATE INDEX ON marts.team_season_trajectory (era_code);
