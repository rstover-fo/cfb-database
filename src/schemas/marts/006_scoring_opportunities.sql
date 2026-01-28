-- Scoring opportunities: drive efficiency and red zone conversion
-- Grain: Team × Season
-- Uses drives table for possession-level analysis

DROP MATERIALIZED VIEW IF EXISTS marts.scoring_opportunities CASCADE;

CREATE MATERIALIZED VIEW marts.scoring_opportunities AS
SELECT
    d.offense AS team,
    d.season,

    -- Drive counts
    COUNT(*) AS total_drives,

    -- Scoring drives
    COUNT(*) FILTER (WHERE d.drive_result IN ('TD', 'FG', 'Touchdown', 'Field Goal')) AS scoring_drives,
    ROUND(
        COUNT(*) FILTER (WHERE d.drive_result IN ('TD', 'FG', 'Touchdown', 'Field Goal'))::numeric /
        NULLIF(COUNT(*), 0),
        4
    ) AS scoring_rate,

    -- Touchdown drives
    COUNT(*) FILTER (WHERE d.drive_result IN ('TD', 'Touchdown')) AS td_drives,
    ROUND(
        COUNT(*) FILTER (WHERE d.drive_result IN ('TD', 'Touchdown'))::numeric /
        NULLIF(COUNT(*), 0),
        4
    ) AS td_rate,

    -- Points per drive (estimated: TD=7, FG=3)
    ROUND(
        (COUNT(*) FILTER (WHERE d.drive_result IN ('TD', 'Touchdown')) * 7.0 +
         COUNT(*) FILTER (WHERE d.drive_result IN ('FG', 'Field Goal')) * 3.0) /
        NULLIF(COUNT(*), 0),
        3
    ) AS points_per_drive,

    -- Turnover drives
    COUNT(*) FILTER (WHERE d.drive_result IN ('INT', 'FUMBLE', 'Interception', 'Fumble', 'Fumble Lost', 'Interception Return')) AS turnover_drives,
    ROUND(
        COUNT(*) FILTER (WHERE d.drive_result IN ('INT', 'FUMBLE', 'Interception', 'Fumble', 'Fumble Lost', 'Interception Return'))::numeric /
        NULLIF(COUNT(*), 0),
        4
    ) AS turnover_rate,

    -- Punt drives
    COUNT(*) FILTER (WHERE d.drive_result IN ('PUNT', 'Punt')) AS punt_drives,
    ROUND(
        COUNT(*) FILTER (WHERE d.drive_result IN ('PUNT', 'Punt'))::numeric /
        NULLIF(COUNT(*), 0),
        4
    ) AS punt_rate,

    -- Average drive stats
    ROUND(AVG(d.plays)::numeric, 1) AS avg_plays_per_drive,
    ROUND(AVG(d.yards)::numeric, 1) AS avg_yards_per_drive,
    ROUND(AVG(d.elapsed__minutes)::numeric, 2) AS avg_time_per_drive,

    -- Red zone drives (started inside opponent 20)
    COUNT(*) FILTER (WHERE d.start_yards_to_goal <= 20) AS red_zone_drives,
    COUNT(*) FILTER (WHERE d.start_yards_to_goal <= 20 AND d.drive_result IN ('TD', 'FG', 'Touchdown', 'Field Goal')) AS red_zone_scores,
    ROUND(
        COUNT(*) FILTER (WHERE d.start_yards_to_goal <= 20 AND d.drive_result IN ('TD', 'FG', 'Touchdown', 'Field Goal'))::numeric /
        NULLIF(COUNT(*) FILTER (WHERE d.start_yards_to_goal <= 20), 0),
        4
    ) AS red_zone_scoring_rate,

    -- Red zone TD rate
    ROUND(
        COUNT(*) FILTER (WHERE d.start_yards_to_goal <= 20 AND d.drive_result IN ('TD', 'Touchdown'))::numeric /
        NULLIF(COUNT(*) FILTER (WHERE d.start_yards_to_goal <= 20), 0),
        4
    ) AS red_zone_td_rate

FROM core.drives d
WHERE d.offense IS NOT NULL
GROUP BY d.offense, d.season;

-- Required for REFRESH CONCURRENTLY
CREATE UNIQUE INDEX ON marts.scoring_opportunities (team, season);

-- Query indexes
CREATE INDEX ON marts.scoring_opportunities (season);
CREATE INDEX ON marts.scoring_opportunities (points_per_drive DESC);
CREATE INDEX ON marts.scoring_opportunities (scoring_rate DESC);
