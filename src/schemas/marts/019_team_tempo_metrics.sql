-- Team Tempo Metrics
-- Grain: Team × Season
-- Purpose: Tempo (plays per game) for scatter plot visualization

DROP MATERIALIZED VIEW IF EXISTS marts.team_tempo_metrics CASCADE;

CREATE MATERIALIZED VIEW marts.team_tempo_metrics AS
WITH game_plays AS (
    SELECT
        p.season,
        p.offense AS team,
        g.id AS game_id,
        COUNT(*) AS plays
    FROM core.plays p
    JOIN core.games g ON p.game_id = g.id
    WHERE g.season >= 2014
      AND p.play_type NOT IN ('Timeout', 'End Period', 'End of Half', 'End of Game', 'Kickoff')
      -- Exclude garbage time
      AND NOT (
          (p.period = 4 AND ABS(COALESCE(p.score_diff, 0)) > 28) OR
          (p.period >= 3 AND ABS(COALESCE(p.score_diff, 0)) > 35)
      )
    GROUP BY p.season, p.offense, g.id
),
team_tempo AS (
    SELECT
        season,
        team,
        COUNT(DISTINCT game_id) AS games,
        SUM(plays) AS total_plays,
        ROUND(AVG(plays)::numeric, 1) AS plays_per_game,
        CASE
            WHEN AVG(plays) >= 75 THEN 'up_tempo'
            WHEN AVG(plays) >= 65 THEN 'balanced'
            ELSE 'slow'
        END AS tempo_tier
    FROM game_plays
    GROUP BY season, team
)
SELECT
    t.season,
    t.team,
    t.games,
    t.total_plays,
    t.plays_per_game,
    t.tempo_tier,
    e.epa_per_play,
    e.success_rate,
    e.explosiveness
FROM team_tempo t
LEFT JOIN marts.team_epa_season e
    ON t.season = e.season AND t.team = e.team
WHERE t.games >= 5;

-- Required for REFRESH CONCURRENTLY
CREATE UNIQUE INDEX ON marts.team_tempo_metrics (season, team);

-- Query optimization
CREATE INDEX ON marts.team_tempo_metrics (season);
CREATE INDEX ON marts.team_tempo_metrics (tempo_tier);

COMMENT ON MATERIALIZED VIEW marts.team_tempo_metrics IS
'Team tempo metrics joining plays per game with EPA. Grain: team × season.';
