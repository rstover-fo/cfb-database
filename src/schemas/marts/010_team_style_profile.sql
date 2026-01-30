-- Team offensive/defensive identity profile
-- Provides run/pass tendencies, EPA by play type, and tempo classification
-- Depends on: core.plays, core.games

DROP MATERIALIZED VIEW IF EXISTS marts.team_style_profile CASCADE;

CREATE MATERIALIZED VIEW marts.team_style_profile AS
WITH play_aggregates AS (
    SELECT
        p.offense AS team,
        g.season,
        -- Play counts by type
        COUNT(*) FILTER (WHERE p.play_type IN ('Rush', 'Rushing Touchdown')) AS rush_plays,
        COUNT(*) FILTER (WHERE p.play_type IN ('Pass Reception', 'Pass Incompletion', 'Passing Touchdown', 'Pass Interception', 'Sack')) AS pass_plays,
        COUNT(*) AS total_plays,
        -- EPA by play type
        AVG(p.ppa) FILTER (WHERE p.play_type IN ('Rush', 'Rushing Touchdown')) AS epa_rushing,
        AVG(p.ppa) FILTER (WHERE p.play_type IN ('Pass Reception', 'Pass Incompletion', 'Passing Touchdown', 'Pass Interception', 'Sack')) AS epa_passing,
        -- Game count for tempo
        COUNT(DISTINCT p.game_id) AS games
    FROM core.plays p
    JOIN core.games g ON p.game_id = g.id
    WHERE p.ppa IS NOT NULL
    GROUP BY p.offense, g.season
),
defensive_aggregates AS (
    SELECT
        p.defense AS team,
        g.season,
        -- Defensive EPA allowed by play type
        AVG(p.ppa) FILTER (WHERE p.play_type IN ('Rush', 'Rushing Touchdown')) AS def_epa_vs_run,
        AVG(p.ppa) FILTER (WHERE p.play_type IN ('Pass Reception', 'Pass Incompletion', 'Passing Touchdown', 'Pass Interception', 'Sack')) AS def_epa_vs_pass
    FROM core.plays p
    JOIN core.games g ON p.game_id = g.id
    WHERE p.ppa IS NOT NULL
    GROUP BY p.defense, g.season
)
SELECT
    o.team,
    o.season,
    -- Run/pass rates
    ROUND((o.rush_plays::numeric / NULLIF(o.total_plays, 0)), 3) AS run_rate,
    ROUND((o.pass_plays::numeric / NULLIF(o.total_plays, 0)), 3) AS pass_rate,
    -- EPA by play type
    ROUND(o.epa_rushing::numeric, 4) AS epa_rushing,
    ROUND(o.epa_passing::numeric, 4) AS epa_passing,
    -- Tempo (plays per game)
    ROUND((o.total_plays::numeric / NULLIF(o.games, 0)), 1) AS plays_per_game,
    -- Tempo category
    CASE
        WHEN (o.total_plays::numeric / NULLIF(o.games, 0)) >= 75 THEN 'up_tempo'
        WHEN (o.total_plays::numeric / NULLIF(o.games, 0)) >= 65 THEN 'balanced'
        ELSE 'slow'
    END AS tempo_category,
    -- Offensive identity
    CASE
        WHEN (o.rush_plays::numeric / NULLIF(o.total_plays, 0)) >= 0.55 THEN 'run_heavy'
        WHEN (o.pass_plays::numeric / NULLIF(o.total_plays, 0)) >= 0.55 THEN 'pass_heavy'
        ELSE 'balanced'
    END AS offensive_identity,
    -- Defensive EPA (positive = bad for defense, negative = good)
    ROUND(d.def_epa_vs_run::numeric, 4) AS def_epa_vs_run,
    ROUND(d.def_epa_vs_pass::numeric, 4) AS def_epa_vs_pass
FROM play_aggregates o
LEFT JOIN defensive_aggregates d ON o.team = d.team AND o.season = d.season;

-- Required for REFRESH CONCURRENTLY
CREATE UNIQUE INDEX ON marts.team_style_profile (team, season);

-- Query indexes
CREATE INDEX ON marts.team_style_profile (season);
CREATE INDEX ON marts.team_style_profile (offensive_identity);
CREATE INDEX ON marts.team_style_profile (tempo_category);
